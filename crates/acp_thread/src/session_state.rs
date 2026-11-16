//! Client model for the Gorynych ACP v1 session-state extension.
//!
//! The extension reports a session's aggregate foreground activity as a snapshot in
//! `_meta` and as live `_gorynych/session_state` notifications. Revisions increase
//! within an epoch; the epoch identifies the conductor process that reported them.

use agent_client_protocol::schema::v2 as acp_v2;
use serde_json::Value;
use std::collections::{HashSet, VecDeque};

/// Key in the `_meta` of agent capabilities, listed sessions, and session-open
/// responses that advertises or carries this extension.
pub const SESSION_STATE_META_KEY: &str = "gorynych/sessionState";

/// Custom live notification method the extension adds.
pub const SESSION_STATE_NOTIFICATION_METHOD: &str = "_gorynych/session_state";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReportedSessionState {
    Running,
    Idle,
    RequiresAction,
}

impl ReportedSessionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Idle => "idle",
            Self::RequiresAction => "requires_action",
        }
    }

    pub fn from_str(state: &str) -> Option<Self> {
        match state {
            "running" => Some(Self::Running),
            "idle" => Some(Self::Idle),
            "requires_action" => Some(Self::RequiresAction),
            _ => None,
        }
    }

    pub fn activity(self) -> acp_v2::StateUpdate {
        match self {
            Self::Running => acp_v2::StateUpdate::Running(acp_v2::RunningStateUpdate::new()),
            Self::Idle => acp_v2::StateUpdate::Idle(acp_v2::IdleStateUpdate::new()),
            Self::RequiresAction => {
                acp_v2::StateUpdate::RequiresAction(acp_v2::RequiresActionStateUpdate::new())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionStateSnapshot {
    pub state: ReportedSessionState,
    pub epoch: String,
    pub revision: u64,
    pub background: Option<Vec<Value>>,
    pub background_known: Option<Vec<Value>>,
    pub background_malformed: bool,
    pub background_known_malformed: bool,
}

impl SessionStateSnapshot {
    pub fn from_meta(meta: Option<&acp_v2::Meta>) -> Option<Self> {
        Self::from_value(meta?.get(SESSION_STATE_META_KEY)?)
    }

    fn parse_background(value: Option<&Value>) -> Option<Vec<Value>> {
        let mut seen = HashSet::new();
        match value {
            Some(Value::Array(tasks))
                if tasks.iter().all(|task| {
                    let Some(object) = task.as_object() else {
                        return false;
                    };
                    let Some(kind) = object
                        .get("type")
                        .and_then(Value::as_str)
                        .filter(|kind| !kind.trim().is_empty())
                    else {
                        return false;
                    };
                    if object
                        .get("_meta")
                        .is_some_and(|meta| !meta.is_null() && !meta.is_object())
                    {
                        return false;
                    }
                    let field = match kind {
                        "terminal" => Some("terminalId"),
                        "tool_call" => Some("toolCallId"),
                        "subagent" => Some("sessionId"),
                        _ => None,
                    };
                    field.is_none_or(|field| {
                        object
                            .get(field)
                            .and_then(Value::as_str)
                            .is_some_and(|id| !id.trim().is_empty() && seen.insert((kind, id)))
                    })
                }) =>
            {
                Some(tasks.clone())
            }
            _ => None,
        }
    }

    /// Parses a snapshot. A malformed foreground envelope is unconfirmed; malformed
    /// background fields are independently treated as unreported.
    pub fn from_value(value: &Value) -> Option<Self> {
        let object = value.as_object()?;
        let state = object
            .get("state")?
            .as_str()
            .and_then(ReportedSessionState::from_str)?;
        let epoch = object.get("epoch")?.as_str()?;
        if epoch.is_empty() {
            return None;
        }
        let revision = object.get("revision")?.as_u64()?;
        let background = Self::parse_background(object.get("background"));
        let background_known = Self::parse_background(object.get("backgroundKnown"));
        let background_malformed = object
            .get("background")
            .is_some_and(|value| !value.is_null())
            && background.is_none();
        let background_known_malformed = object
            .get("backgroundKnown")
            .is_some_and(|value| !value.is_null())
            && background_known.is_none();
        Some(Self {
            background,
            background_malformed,
            background_known_malformed,
            background_known,
            state,
            epoch: epoch.to_owned(),
            revision,
        })
    }
}

/// Keeps the newest snapshot of one session accepted from the current connection.
///
/// The first snapshot binds the epoch. Ordinary list/open snapshots then only update
/// that epoch. A live notification from another epoch retires the current one, as a
/// bridge can reconnect its service without reconnecting this client's stdio.
#[derive(Debug, Default)]
pub struct SessionStateReconciler {
    epoch: Option<String>,
    revision: u64,
    last_snapshot: Option<SessionStateSnapshot>,
    retired_epochs: VecDeque<String>,
}

/// Enough to reject late responses from the recent conductor processes without
/// retaining unbounded epoch history.
const MAX_RETIRED_EPOCHS: usize = 8;

impl SessionStateReconciler {
    /// Live observations may replace the bound epoch. Notifications from a retired
    /// epoch are late and must not roll it back.
    pub fn accept_live(&mut self, snapshot: &SessionStateSnapshot) -> bool {
        if self.is_retired(&snapshot.epoch) {
            return false;
        }
        match self.epoch.clone() {
            Some(epoch) if epoch == snapshot.epoch => self.accept_current(snapshot),
            Some(epoch) => {
                self.retire(epoch);
                self.bind(snapshot);
                true
            }
            None => {
                self.bind(snapshot);
                true
            }
        }
    }

    /// Snapshots from a list or open response can establish the first epoch but
    /// cannot replace a bound one or a retired one.
    pub fn accept_snapshot(&mut self, snapshot: &SessionStateSnapshot) -> bool {
        if self.is_retired(&snapshot.epoch) {
            return false;
        }
        match self.epoch.as_ref() {
            Some(epoch) if epoch == &snapshot.epoch => self.accept_current(snapshot),
            Some(_) => false,
            None => {
                self.bind(snapshot);
                true
            }
        }
    }

    fn accept_current(&mut self, snapshot: &SessionStateSnapshot) -> bool {
        if snapshot.revision > self.revision {
            self.revision = snapshot.revision;
            self.last_snapshot = Some(snapshot.clone());
            return true;
        }
        if snapshot.revision != self.revision {
            return false;
        }
        let Some(last) = self.last_snapshot.as_mut() else {
            return false;
        };
        if snapshot.state != last.state {
            return false;
        }
        if !(snapshot.background_malformed
            || last.background_malformed
            || snapshot.background == last.background)
            || !(snapshot.background_known_malformed
                || last.background_known_malformed
                || snapshot.background_known == last.background_known)
        {
            return false;
        }
        // Malformed fields invalidate only their own presentation. Keep the last
        // confirmed value for same-revision conflict detection and recovery.
        if last.background_malformed && !snapshot.background_malformed {
            last.background = snapshot.background.clone();
            last.background_malformed = false;
        }
        if last.background_known_malformed && !snapshot.background_known_malformed {
            last.background_known = snapshot.background_known.clone();
            last.background_known_malformed = false;
        }
        true
    }

    fn bind(&mut self, snapshot: &SessionStateSnapshot) {
        self.epoch = Some(snapshot.epoch.clone());
        self.revision = snapshot.revision;
        self.last_snapshot = Some(snapshot.clone());
    }

    fn retire(&mut self, epoch: String) {
        if self.retired_epochs.contains(&epoch) {
            return;
        }
        self.retired_epochs.push_back(epoch);
        while self.retired_epochs.len() > MAX_RETIRED_EPOCHS {
            self.retired_epochs.pop_front();
        }
    }

    fn is_retired(&self, epoch: &str) -> bool {
        self.retired_epochs.iter().any(|retired| retired == epoch)
    }

    /// A new connection may report the same session with another epoch.
    pub fn reset(&mut self) {
        self.epoch = None;
        self.revision = 0;
        self.last_snapshot = None;
        self.retired_epochs.clear();
    }

    pub fn epoch(&self) -> Option<&str> {
        self.epoch.as_deref()
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snapshot(state: ReportedSessionState, epoch: &str, revision: u64) -> SessionStateSnapshot {
        SessionStateSnapshot {
            state,
            epoch: epoch.into(),
            revision,
            background: None,
            background_known: None,
            background_malformed: false,
            background_known_malformed: false,
        }
    }

    #[test]
    fn parses_snapshots_and_rejects_malformed_payloads() {
        assert_eq!(
            SessionStateSnapshot::from_value(&json!({
                "state": "requires_action",
                "epoch": "conductor-1",
                "revision": 7,
            })),
            Some(snapshot(
                ReportedSessionState::RequiresAction,
                "conductor-1",
                7
            ))
        );

        for malformed in [
            json!(null),
            json!({}),
            json!({"state": "running", "epoch": "conductor-1"}),
            json!({"state": "unknown", "epoch": "conductor-1", "revision": 1}),
            json!({"state": "running", "epoch": "", "revision": 1}),
            json!({"state": "running", "epoch": "conductor-1", "revision": -1}),
            json!({"state": "running", "epoch": "conductor-1", "revision": "1"}),
        ] {
            assert_eq!(SessionStateSnapshot::from_value(&malformed), None);
        }
    }

    #[test]
    fn background_snapshots_reconcile_as_part_of_the_revision() {
        let parse = |background| {
            SessionStateSnapshot::from_value(&json!({
                "state": "idle", "epoch": "one", "revision": 2, "background": background
            }))
            .expect("valid snapshot")
        };
        let unknown = parse(json!(null));
        let empty = parse(json!([]));
        let busy = parse(json!([{ "type": "terminal", "terminalId": "task" }]));
        assert_eq!(unknown.background, None);
        assert_eq!(empty.background.as_ref().map(Vec::len), Some(0));
        assert_eq!(busy.background.as_ref().map(Vec::len), Some(1));
        let mut reconciler = SessionStateReconciler::default();
        assert!(reconciler.accept_live(&busy));
        assert!(!reconciler.accept_snapshot(&empty));
        assert!(reconciler.accept_snapshot(&busy));
        let malformed = SessionStateSnapshot::from_value(&json!({
            "state": "idle", "epoch": "one", "revision": 3, "background": [{}]
        }))
        .expect("valid foreground remains confirmed");
        assert_eq!(malformed.state, ReportedSessionState::Idle);
        assert_eq!(malformed.background, None);
    }

    #[test]
    fn malformed_background_invalidates_only_that_field_and_can_recover() {
        let parse = |background| {
            SessionStateSnapshot::from_value(&json!({
                "state": "idle", "epoch": "one", "revision": 4, "background": background,
                "backgroundKnown": [{"type":"tool_call", "toolCallId":"known"}]
            }))
            .expect("valid foreground")
        };
        let good = parse(json!([{"type":"terminal", "terminalId":"one"}]));
        let malformed = parse(json!([{}]));
        let conflicting = parse(json!([{"type":"terminal", "terminalId":"other"}]));
        let mut reconciler = SessionStateReconciler::default();
        assert!(reconciler.accept_live(&good));
        assert!(reconciler.accept_live(&malformed));
        assert_eq!(malformed.state, ReportedSessionState::Idle);
        assert!(malformed.background.is_none());
        assert!(malformed.background_known.is_some());
        assert!(!reconciler.accept_snapshot(&conflicting));
        assert!(reconciler.accept_snapshot(&good));
    }

    #[test]
    fn revisions_must_increase_within_an_epoch() {
        let mut reconciler = SessionStateReconciler::default();
        assert!(reconciler.accept_live(&snapshot(ReportedSessionState::Running, "epoch-a", 2)));
        assert!(!reconciler.accept_live(&snapshot(ReportedSessionState::Idle, "epoch-a", 2)));
        assert!(!reconciler.accept_live(&snapshot(ReportedSessionState::Idle, "epoch-a", 1)));
        assert!(reconciler.accept_live(&snapshot(ReportedSessionState::Idle, "epoch-a", 3)));
        assert_eq!(reconciler.revision(), 3);
    }

    #[test]
    fn a_duplicate_restores_confirmation_but_conflicting_same_revision_does_not() {
        let mut reconciler = SessionStateReconciler::default();
        assert!(reconciler.accept_live(&snapshot(
            ReportedSessionState::RequiresAction,
            "epoch-a",
            4
        )));
        assert!(reconciler.accept_snapshot(&snapshot(
            ReportedSessionState::RequiresAction,
            "epoch-a",
            4
        )));
        assert!(!reconciler.accept_snapshot(&snapshot(ReportedSessionState::Idle, "epoch-a", 4)));
        assert!(!reconciler.accept_snapshot(&snapshot(ReportedSessionState::Idle, "epoch-a", 3)));
        assert_eq!(reconciler.revision(), 4);
    }

    #[test]
    fn live_epoch_replacement_retires_the_previous_epoch() {
        let mut reconciler = SessionStateReconciler::default();
        assert!(reconciler.accept_snapshot(&snapshot(ReportedSessionState::Running, "epoch-a", 7)));
        assert!(!reconciler.accept_snapshot(&snapshot(ReportedSessionState::Idle, "epoch-b", 1)));

        assert!(reconciler.accept_live(&snapshot(ReportedSessionState::Idle, "epoch-b", 1)));
        assert_eq!(reconciler.epoch(), Some("epoch-b"));
        assert!(!reconciler.accept_live(&snapshot(ReportedSessionState::Running, "epoch-a", 8)));
        assert!(!reconciler.accept_snapshot(&snapshot(
            ReportedSessionState::Running,
            "epoch-a",
            8
        )));
        assert_eq!(reconciler.epoch(), Some("epoch-b"));
    }

    #[test]
    fn another_epoch_is_ignored_until_reconnect_resets() {
        let mut reconciler = SessionStateReconciler::default();
        assert!(reconciler.accept_snapshot(&snapshot(ReportedSessionState::Running, "epoch-a", 1)));
        assert!(!reconciler.accept_snapshot(&snapshot(ReportedSessionState::Idle, "epoch-b", 9)));
        assert_eq!(reconciler.epoch(), Some("epoch-a"));

        reconciler.reset();
        assert!(reconciler.accept_snapshot(&snapshot(ReportedSessionState::Idle, "epoch-b", 9)));
        assert_eq!(reconciler.epoch(), Some("epoch-b"));
    }
}
