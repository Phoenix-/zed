//! Current activity of listed agent sessions, as the session-state extension reports.
//!
//! The sidebar can show that an imported session is working, waiting, or idle even
//! before its conversation is loaded. The states are snapshots, not history: each
//! listing reports them afresh, and live `_gorynych/session_state` updates replace
//! them only when their revision is newer. Losing the connection forgets them, so a
//! stale running state never outlives the agent that reported it.

use agent_client_protocol::schema::v2 as acp;
use collections::{HashMap, HashSet};
use gpui::{App, AppContext as _, Context, Entity, Global};
use ui::AgentThreadStatus;

use acp_thread::{ReportedSessionState, SessionStateReconciler, SessionStateSnapshot};

use crate::session_auto_import::SessionSource;

pub fn init(cx: &mut App) {
    SessionActivityStore::init_global(cx);
}

struct GlobalSessionActivityStore(Entity<SessionActivityStore>);

impl Global for GlobalSessionActivityStore {}

#[derive(Default)]
struct ActivityEntry {
    reconciler: SessionStateReconciler,
    /// `None` means the extension was advertised but no valid snapshot was reported,
    /// which is unconfirmed activity rather than idle.
    snapshot: Option<SessionStateSnapshot>,
}

#[derive(Default)]
pub struct SessionActivityStore {
    sessions: HashMap<(SessionSource, acp::SessionId), ActivityEntry>,
}

impl SessionActivityStore {
    pub fn init_global(cx: &mut App) {
        let store = cx.new(|_| Self::default());
        cx.set_global(GlobalSessionActivityStore(store));
    }

    pub fn try_global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<GlobalSessionActivityStore>()
            .map(|store| store.0.clone())
    }

    /// The sidebar status a session's current snapshot implies, or `None` when the
    /// store has no information about it.
    pub fn status(
        &self,
        source: &SessionSource,
        session_id: &acp::SessionId,
    ) -> Option<AgentThreadStatus> {
        let entry = self.sessions.get(&(source.clone(), session_id.clone()))?;
        Some(match &entry.snapshot {
            Some(snapshot) => match snapshot.state {
                ReportedSessionState::Running => AgentThreadStatus::Running,
                ReportedSessionState::Idle
                    if snapshot
                        .background
                        .as_ref()
                        .or(snapshot.background_known.as_ref())
                        .is_some_and(|tasks| !tasks.is_empty()) =>
                {
                    AgentThreadStatus::Background
                }
                ReportedSessionState::Idle => AgentThreadStatus::Completed,
                ReportedSessionState::RequiresAction => AgentThreadStatus::WaitingForConfirmation,
            },
            None => AgentThreadStatus::Unconfirmed,
        })
    }

    /// Records what a listing reported. A supported source lists every session with
    /// its current snapshot, and a session without one has unknown activity. When the
    /// listing is complete, sessions missing from it are forgotten.
    pub fn apply_listing(
        &mut self,
        source: &SessionSource,
        sessions: impl IntoIterator<Item = (acp::SessionId, Option<SessionStateSnapshot>)>,
        complete: bool,
        supported: bool,
        cx: &mut Context<Self>,
    ) {
        if !supported {
            let previous = self.sessions.len();
            self.sessions
                .retain(|(entry_source, _), _| entry_source != source);
            if self.sessions.len() != previous {
                cx.notify();
            }
            return;
        }

        let mut changed = false;
        let mut listed = HashSet::default();
        for (session_id, snapshot) in sessions {
            if !listed.insert(session_id.clone()) {
                continue;
            }
            let entry = self
                .sessions
                .entry((source.clone(), session_id))
                .or_default();
            changed |= entry.set_snapshot(snapshot);
        }
        if complete {
            let previous = self.sessions.len();
            self.sessions.retain(|(entry_source, session_id), _| {
                entry_source != source || listed.contains(session_id)
            });
            changed |= self.sessions.len() != previous;
        }
        if changed {
            cx.notify();
        }
    }

    /// Records a live notification. A new epoch replaces the previous one; late
    /// snapshots from retired epochs are ignored.
    pub fn set_live_snapshot(
        &mut self,
        source: &SessionSource,
        session_id: acp::SessionId,
        snapshot: Option<SessionStateSnapshot>,
        cx: &mut Context<Self>,
    ) {
        let entry = self
            .sessions
            .entry((source.clone(), session_id))
            .or_default();
        if entry.set_live(snapshot) {
            cx.notify();
        }
    }

    /// Keeps a source's known sessions as unconfirmed tombstones when its
    /// connection goes. Removing them would make the sidebar fall back to its
    /// default completed status; a fresh supported listing replaces the tombstones.
    pub fn forget_source(&mut self, source: &SessionSource, cx: &mut Context<Self>) {
        let mut changed = false;
        for ((entry_source, _), entry) in &mut self.sessions {
            if entry_source == source {
                changed |= entry.forget();
            }
        }
        if changed {
            cx.notify();
        }
    }

    /// Forgets one session the agent no longer has.
    pub fn forget_session(
        &mut self,
        source: &SessionSource,
        session_id: &acp::SessionId,
        cx: &mut Context<Self>,
    ) {
        if self
            .sessions
            .remove(&(source.clone(), session_id.clone()))
            .is_some()
        {
            cx.notify();
        }
    }
}

impl ActivityEntry {
    fn set_live(&mut self, snapshot: Option<SessionStateSnapshot>) -> bool {
        self.set(snapshot, true)
    }

    fn set_snapshot(&mut self, snapshot: Option<SessionStateSnapshot>) -> bool {
        self.set(snapshot, false)
    }

    /// Drops confirmation and epoch history, but keeps the session known so the
    /// sidebar shows unconfirmed instead of its completed default.
    fn forget(&mut self) -> bool {
        let changed = self.snapshot.is_some();
        self.snapshot = None;
        self.reconciler.reset();
        changed
    }

    fn set(&mut self, snapshot: Option<SessionStateSnapshot>, live: bool) -> bool {
        match snapshot {
            Some(snapshot) => {
                let accepted = if live {
                    self.reconciler.accept_live(&snapshot)
                } else {
                    self.reconciler.accept_snapshot(&snapshot)
                };
                if !accepted {
                    return false;
                }
                let changed = self.snapshot.as_ref() != Some(&snapshot);
                self.snapshot = Some(snapshot);
                changed
            }
            None => {
                let changed = self.snapshot.is_some();
                self.snapshot = None;
                changed
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;
    use project::AgentId;

    fn local(agent: &str) -> SessionSource {
        SessionSource::local(AgentId::new(agent))
    }

    fn snapshot(state: ReportedSessionState, revision: u64) -> SessionStateSnapshot {
        snapshot_with_epoch(state, "conductor", revision)
    }

    fn snapshot_with_epoch(
        state: ReportedSessionState,
        epoch: &str,
        revision: u64,
    ) -> SessionStateSnapshot {
        SessionStateSnapshot {
            state,
            epoch: epoch.into(),
            revision,
            background: None,
            background_known: None,
        }
    }

    fn status(
        store: &SessionActivityStore,
        source: &SessionSource,
        session: &str,
    ) -> Option<AgentThreadStatus> {
        store.status(source, &acp::SessionId::new(session))
    }

    #[gpui::test]
    fn listed_sessions_without_a_conversation_have_current_status(cx: &mut TestAppContext) {
        let store = cx.new(|_| SessionActivityStore::default());
        let source = local("gorynych");
        store.update(cx, |store, cx| {
            store.apply_listing(
                &source,
                [
                    (
                        acp::SessionId::new("working"),
                        Some(snapshot(ReportedSessionState::Running, 4)),
                    ),
                    (
                        acp::SessionId::new("waiting"),
                        Some(snapshot(ReportedSessionState::RequiresAction, 8)),
                    ),
                    (
                        acp::SessionId::new("resting"),
                        Some(snapshot(ReportedSessionState::Idle, 2)),
                    ),
                    (acp::SessionId::new("unreported"), None),
                ],
                true,
                true,
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "working"),
                Some(AgentThreadStatus::Running)
            );
            assert_eq!(
                status(store, &source, "waiting"),
                Some(AgentThreadStatus::WaitingForConfirmation)
            );
            assert_eq!(
                status(store, &source, "resting"),
                Some(AgentThreadStatus::Completed)
            );
            assert_eq!(
                status(store, &source, "unreported"),
                Some(AgentThreadStatus::Unconfirmed)
            );
        });
    }

    #[gpui::test]
    fn idle_background_activity_is_visible_for_unopened_sessions(cx: &mut TestAppContext) {
        let store = cx.new(|_| SessionActivityStore::default());
        let source = local("gorynych");
        let session_id = acp::SessionId::new("background");
        let mut busy = snapshot(ReportedSessionState::Idle, 1);
        busy.background = Some(vec![serde_json::json!({"id": "task"})]);
        store.update(cx, |store, cx| {
            store.set_live_snapshot(&source, session_id.clone(), Some(busy), cx)
        });
        assert_eq!(
            store.read_with(cx, |store, _| store.status(&source, &session_id)),
            Some(AgentThreadStatus::Background)
        );
        store.update(cx, |store, cx| {
            store.set_live_snapshot(
                &source,
                session_id.clone(),
                Some(snapshot(ReportedSessionState::Idle, 2)),
                cx,
            )
        });
        assert_eq!(
            store.read_with(cx, |store, _| store.status(&source, &session_id)),
            Some(AgentThreadStatus::Completed)
        );
    }

    #[gpui::test]
    fn live_updates_ignore_stale_revisions_and_malformed_payloads(cx: &mut TestAppContext) {
        let store = cx.new(|_| SessionActivityStore::default());
        let source = local("gorynych");
        let session_id = acp::SessionId::new("session");
        store.update(cx, |store, cx| {
            store.set_live_snapshot(
                &source,
                session_id.clone(),
                Some(snapshot(ReportedSessionState::Running, 5)),
                cx,
            );
            store.set_live_snapshot(
                &source,
                session_id.clone(),
                Some(snapshot(ReportedSessionState::Idle, 4)),
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Running)
            );
        });

        store.update(cx, |store, cx| {
            store.set_live_snapshot(&source, session_id.clone(), None, cx);
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Unconfirmed)
            );
        });

        store.update(cx, |store, cx| {
            store.set_live_snapshot(
                &source,
                session_id,
                Some(snapshot(ReportedSessionState::Idle, 6)),
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Completed)
            );
        });
    }

    #[gpui::test]
    fn complete_listings_and_disconnect_forget_state(cx: &mut TestAppContext) {
        let store = cx.new(|_| SessionActivityStore::default());
        let source = local("gorynych");
        store.update(cx, |store, cx| {
            store.apply_listing(
                &source,
                [
                    (
                        acp::SessionId::new("one"),
                        Some(snapshot(ReportedSessionState::Running, 1)),
                    ),
                    (
                        acp::SessionId::new("two"),
                        Some(snapshot(ReportedSessionState::Idle, 1)),
                    ),
                ],
                true,
                true,
                cx,
            );
        });
        store.update(cx, |store, cx| {
            store.apply_listing(
                &source,
                [(
                    acp::SessionId::new("one"),
                    Some(snapshot(ReportedSessionState::Running, 2)),
                )],
                true,
                true,
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert!(status(store, &source, "one").is_some());
            assert!(status(store, &source, "two").is_none());
        });

        store.update(cx, |store, cx| store.forget_source(&source, cx));
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "one"),
                Some(AgentThreadStatus::Unconfirmed),
                "disconnect keeps a known session as an unconfirmed tombstone"
            );
            assert!(status(store, &source, "two").is_none());
        });

        store.update(cx, |store, cx| {
            store.apply_listing(
                &source,
                [(
                    acp::SessionId::new("one"),
                    Some(snapshot_with_epoch(
                        ReportedSessionState::Idle,
                        "new-conductor",
                        1,
                    )),
                )],
                true,
                true,
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "one"),
                Some(AgentThreadStatus::Completed),
                "a fresh supported listing replaces the tombstone and binds a new epoch"
            );
        });

        store.update(cx, |store, cx| {
            store.apply_listing(&source, [], true, false, cx);
        });
        store.read_with(cx, |store, _| {
            assert!(
                status(store, &source, "one").is_none(),
                "an unsupported source still removes extension state"
            );
        });
    }

    #[gpui::test]
    fn live_epoch_replaces_old_and_stale_listings_cannot_restore_it(cx: &mut TestAppContext) {
        let store = cx.new(|_| SessionActivityStore::default());
        let source = local("gorynych");
        let session_id = acp::SessionId::new("session");
        store.update(cx, |store, cx| {
            store.apply_listing(
                &source,
                [(
                    session_id.clone(),
                    Some(SessionStateSnapshot {
                        state: ReportedSessionState::Running,
                        epoch: "one".into(),
                        revision: 7,
                        background: None,
                        background_known: None,
                    }),
                )],
                true,
                true,
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Running)
            );
        });

        store.update(cx, |store, cx| {
            store.set_live_snapshot(
                &source,
                session_id.clone(),
                Some(snapshot_with_epoch(ReportedSessionState::Idle, "two", 1)),
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Completed)
            );
        });

        store.update(cx, |store, cx| {
            store.apply_listing(
                &source,
                [(
                    session_id.clone(),
                    Some(SessionStateSnapshot {
                        state: ReportedSessionState::Running,
                        epoch: "one".into(),
                        revision: 8,
                        background: None,
                        background_known: None,
                    }),
                )],
                true,
                true,
                cx,
            );
            store.set_live_snapshot(
                &source,
                session_id.clone(),
                Some(snapshot_with_epoch(ReportedSessionState::Running, "one", 9)),
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Completed),
                "a stale list response or late live snapshot must not restore a retired epoch"
            );
        });
    }

    #[gpui::test]
    fn ordinary_listing_cannot_replace_a_bound_epoch(cx: &mut TestAppContext) {
        let store = cx.new(|_| SessionActivityStore::default());
        let source = local("gorynych");
        let session_id = acp::SessionId::new("session");
        store.update(cx, |store, cx| {
            store.apply_listing(
                &source,
                [(
                    session_id.clone(),
                    Some(snapshot_with_epoch(ReportedSessionState::Running, "one", 1)),
                )],
                true,
                true,
                cx,
            );
            store.apply_listing(
                &source,
                [(
                    session_id.clone(),
                    Some(snapshot_with_epoch(ReportedSessionState::Idle, "two", 9)),
                )],
                true,
                true,
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Running),
                "only a live observation may replace the bound epoch"
            );
        });
    }

    #[gpui::test]
    fn matching_duplicate_restores_confirmation(cx: &mut TestAppContext) {
        let store = cx.new(|_| SessionActivityStore::default());
        let source = local("gorynych");
        let session_id = acp::SessionId::new("session");
        store.update(cx, |store, cx| {
            store.set_live_snapshot(
                &source,
                session_id.clone(),
                Some(snapshot(ReportedSessionState::Running, 5)),
                cx,
            );
            store.set_live_snapshot(&source, session_id.clone(), None, cx);
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Unconfirmed)
            );
        });

        store.update(cx, |store, cx| {
            store.set_live_snapshot(
                &source,
                session_id.clone(),
                Some(snapshot(ReportedSessionState::Running, 5)),
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Running)
            );
        });

        store.update(cx, |store, cx| {
            store.set_live_snapshot(&source, session_id.clone(), None, cx);
            store.set_live_snapshot(
                &source,
                session_id.clone(),
                Some(snapshot(ReportedSessionState::Idle, 5)),
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Unconfirmed),
                "a conflicting same-revision snapshot must not restore confirmation"
            );
        });

        store.update(cx, |store, cx| {
            store.set_live_snapshot(
                &source,
                session_id,
                Some(snapshot(ReportedSessionState::Idle, 6)),
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert_eq!(
                status(store, &source, "session"),
                Some(AgentThreadStatus::Completed)
            );
        });
    }

    #[gpui::test]
    fn a_source_without_the_extension_has_no_imported_activity(cx: &mut TestAppContext) {
        let store = cx.new(|_| SessionActivityStore::default());
        let source = local("plain");
        store.update(cx, |store, cx| {
            store.apply_listing(
                &source,
                [(
                    acp::SessionId::new("session"),
                    Some(snapshot(ReportedSessionState::Running, 1)),
                )],
                true,
                true,
                cx,
            );
            store.apply_listing(
                &source,
                [(
                    acp::SessionId::new("session"),
                    Some(snapshot(ReportedSessionState::Running, 2)),
                )],
                true,
                false,
                cx,
            );
        });
        store.read_with(cx, |store, _| {
            assert!(status(store, &source, "session").is_none());
        });
    }
}
