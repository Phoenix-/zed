//! Client model for the Gorynych ACP v1 session-state extension.
//!
//! The extension reports a session's aggregate foreground activity as a snapshot in
//! `_meta` and as live `_gorynych/session_state` notifications. Revisions increase
//! within an epoch; the epoch identifies the conductor process that reported them.

use agent_client_protocol::schema::v2 as acp_v2;
use serde_json::Value;
use std::collections::VecDeque;

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
}

impl SessionStateSnapshot {
    pub fn from_meta(meta: Option<&acp_v2::Meta>) -> Option<Self> {
        Self::from_value(meta?.get(SESSION_STATE_META_KEY)?)
    }

    /// Parses a snapshot object. Anything malformed is not a snapshot, which callers
    /// treat as unconfirmed activity rather than idle.
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
        Some(Self {
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
    last_state: Option<ReportedSessionState>,
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
            self.last_state = Some(snapshot.state);
            return true;
        }
        // A malformed notification clears confirmation without advancing the
        // revision. An exactly matching snapshot may restore it; a conflicting
        // state at the same revision may not.
        snapshot.revision == self.revision && self.last_state == Some(snapshot.state)
    }

    fn bind(&mut self, snapshot: &SessionStateSnapshot) {
        self.epoch = Some(snapshot.epoch.clone());
        self.revision = snapshot.revision;
        self.last_state = Some(snapshot.state);
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
        self.last_state = None;
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
