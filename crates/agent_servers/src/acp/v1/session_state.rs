//! The Gorynych ACP v1 session-state extension.
//!
//! Agents advertise `agentCapabilities._meta["gorynych/sessionState"] = 1`. Sessions
//! then carry a current `{state, epoch, revision}` snapshot in their `_meta`, and the
//! agent pushes `_gorynych/session_state` notifications as activity changes. The
//! snapshots describe aggregate current activity, not a prompt turn.

use acp_thread::{
    SESSION_STATE_META_KEY, SESSION_STATE_NOTIFICATION_METHOD, SessionStateReconciler,
    SessionStateSnapshot,
};
use agent_client_protocol::{
    UntypedMessage,
    schema::{v1 as acp, v2 as acp_v2},
};
use collections::HashMap;
use gpui::AsyncApp;
use serde_json::Value;
use util::ResultExt as _;

use crate::acp::ClientContext;

/// Session IDs remembered while their creation response is still in flight. The
/// response's snapshot is current, but a later live update can win by revision.
const MAX_PENDING_SESSION_STATES: usize = 64;

/// Reads the numeric capability flag from the agent capabilities `_meta`.
pub(super) fn advertises(capabilities: &acp::AgentCapabilities) -> bool {
    capabilities
        .meta
        .as_ref()
        .and_then(|meta| meta.get(SESSION_STATE_META_KEY))
        .and_then(Value::as_u64)
        == Some(1)
}

#[derive(Debug)]
pub(super) struct SessionStateNotification {
    pub session_id: acp_v2::SessionId,
    /// `None` when the payload was malformed, which leaves activity unconfirmed.
    pub snapshot: Option<SessionStateSnapshot>,
}

/// Parses the custom notification tolerantly: a bad state or revision still names a
/// session and marks it unconfirmed, while a missing session ID cannot be routed.
pub(super) fn parse_notification(message: &UntypedMessage) -> Option<SessionStateNotification> {
    if message.method() != SESSION_STATE_NOTIFICATION_METHOD {
        return None;
    }
    let params = message.params();
    let session_id = params.get("sessionId")?.as_str()?;
    if session_id.is_empty() {
        return None;
    }
    Some(SessionStateNotification {
        session_id: acp_v2::SessionId::new(session_id),
        snapshot: SessionStateSnapshot::from_value(params),
    })
}

#[derive(Default)]
struct PendingSessionState {
    reconciler: SessionStateReconciler,
    snapshot: Option<SessionStateSnapshot>,
}

/// Connection-scoped extension state, shared by the inbound handler and the
/// connection that creates sessions.
#[derive(Default)]
pub(crate) struct SessionStateExtension {
    enabled: bool,
    pending: HashMap<acp_v2::SessionId, PendingSessionState>,
}

impl SessionStateExtension {
    pub(crate) fn from_enabled(enabled: bool) -> Self {
        Self {
            enabled,
            pending: HashMap::default(),
        }
    }

    pub(super) fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        if !enabled {
            self.pending.clear();
        }
    }

    pub(super) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(super) fn record_pending(
        &mut self,
        session_id: acp_v2::SessionId,
        snapshot: Option<SessionStateSnapshot>,
    ) {
        if self.pending.len() >= MAX_PENDING_SESSION_STATES
            && !self.pending.contains_key(&session_id)
        {
            if let Some(evicted) = self.pending.keys().next().cloned() {
                self.pending.remove(&evicted);
            }
            log::warn!("Evicted a pending ACP session-state snapshot");
        }
        match snapshot {
            Some(snapshot) => {
                let pending = self.pending.entry(session_id).or_default();
                if pending.reconciler.accept_live(&snapshot) {
                    pending.snapshot = Some(snapshot);
                }
            }
            None => {
                self.pending.entry(session_id).or_default().snapshot = None;
            }
        }
    }

    pub(super) fn take_pending(
        &mut self,
        session_id: &acp_v2::SessionId,
    ) -> Option<Option<SessionStateSnapshot>> {
        self.pending
            .remove(session_id)
            .map(|pending| pending.snapshot)
    }

    pub(crate) fn clear(&mut self) {
        self.pending.clear();
    }
}

pub(super) fn handle_session_state_notification(
    notification: SessionStateNotification,
    cx: &mut AsyncApp,
    context: &ClientContext,
) {
    if !context.session_state.borrow().enabled() {
        return;
    }
    let session_id = notification.session_id;
    if let Some(session_list) = context.session_list.borrow().clone() {
        session_list.send_state_update(session_id.clone(), notification.snapshot.clone());
    }

    let thread = context
        .sessions
        .borrow()
        .get(&session_id)
        .map(|session| session.thread.clone());
    if let Some(thread) = thread.and_then(|thread| thread.upgrade()) {
        thread.update(cx, |thread, cx| match notification.snapshot {
            Some(snapshot) => {
                thread.apply_live_session_state(snapshot, cx).log_err();
            }
            None => thread.mark_session_state_unknown(cx),
        });
    } else {
        context
            .session_state
            .borrow_mut()
            .record_pending(session_id, notification.snapshot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acp_thread::ReportedSessionState;
    use serde_json::json;

    fn capabilities(value: Value) -> acp::AgentCapabilities {
        acp::AgentCapabilities::new().meta(acp::Meta::from_iter([(
            SESSION_STATE_META_KEY.into(),
            value,
        )]))
    }

    #[test]
    fn only_numeric_one_advertises_the_extension() {
        assert!(advertises(&capabilities(json!(1))));
        for value in [json!(0), json!(true), json!("1"), json!(null), json!(2)] {
            assert!(!advertises(&capabilities(value)));
        }
        assert!(!advertises(&acp::AgentCapabilities::new()));
    }

    #[test]
    fn malformed_notifications_keep_their_session() {
        let message = UntypedMessage::new(
            SESSION_STATE_NOTIFICATION_METHOD,
            json!({
                "sessionId": "s",
                "state": "sleeping",
                "epoch": "one",
                "revision": 1,
            }),
        )
        .expect("untyped notification");
        let notification = parse_notification(&message).expect("routable notification");
        assert_eq!(notification.session_id, acp_v2::SessionId::new("s"));
        assert_eq!(notification.snapshot, None);

        let no_session =
            UntypedMessage::new(SESSION_STATE_NOTIFICATION_METHOD, json!({"state": "idle"}))
                .expect("untyped notification");
        assert!(parse_notification(&no_session).is_none());
    }

    #[test]
    fn pending_snapshots_keep_the_newest_revision_and_malformed_wins_unknown() {
        let mut extension = SessionStateExtension::from_enabled(true);
        let session_id = acp_v2::SessionId::new("s");
        extension.record_pending(
            session_id.clone(),
            Some(SessionStateSnapshot {
                state: ReportedSessionState::Running,
                epoch: "one".into(),
                revision: 2,
            }),
        );
        extension.record_pending(
            session_id.clone(),
            Some(SessionStateSnapshot {
                state: ReportedSessionState::Idle,
                epoch: "one".into(),
                revision: 1,
            }),
        );
        assert_eq!(
            extension
                .take_pending(&session_id)
                .flatten()
                .map(|snapshot| snapshot.state),
            Some(ReportedSessionState::Running)
        );

        extension.record_pending(session_id.clone(), None);
        assert_eq!(extension.take_pending(&session_id), Some(None));

        extension.record_pending(
            session_id.clone(),
            Some(SessionStateSnapshot {
                state: ReportedSessionState::Running,
                epoch: "one".into(),
                revision: 5,
            }),
        );
        extension.record_pending(
            session_id.clone(),
            Some(SessionStateSnapshot {
                state: ReportedSessionState::Idle,
                epoch: "two".into(),
                revision: 1,
            }),
        );
        extension.record_pending(
            session_id.clone(),
            Some(SessionStateSnapshot {
                state: ReportedSessionState::Running,
                epoch: "one".into(),
                revision: 6,
            }),
        );
        assert_eq!(
            extension
                .take_pending(&session_id)
                .flatten()
                .map(|snapshot| (snapshot.state, snapshot.epoch)),
            Some((ReportedSessionState::Idle, "two".to_string())),
            "a live epoch replaces the previous one, and late updates from it are ignored"
        );
    }
}
