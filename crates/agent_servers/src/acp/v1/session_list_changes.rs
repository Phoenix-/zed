//! Agent-pushed changes to the session list.
//!
//! ACP has no notification for changes to `session/list`, so clients have to poll it.
//! Gorynych extends the protocol with `_gorynych/sessions_changed` and advertises it in
//! the `_meta` of its `initialize` response.

use agent_client_protocol::{JsonRpcNotification, schema::v1 as acp, schema::v2 as acp_v2};
use gpui::AsyncApp;
use serde::{Deserialize, Serialize};

use super::{AcpSessionList, ClientContext};

/// Key in the `initialize` response `_meta` that advertises [`SessionsChangedNotification`].
pub const SESSIONS_CHANGED_META_KEY: &str = "gorynych/sessionsChanged";

#[derive(Debug, Clone, Serialize, Deserialize, JsonRpcNotification)]
#[notification(method = "_gorynych/sessions_changed")]
#[serde(rename_all = "camelCase")]
pub struct SessionsChangedNotification {
    pub session_id: String,
    pub change: SessionChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionChange {
    Added,
    Updated,
    Removed,
    #[serde(other)]
    Unknown,
}

pub(super) fn advertises_sessions_changed(meta: Option<&acp::Meta>) -> bool {
    meta.and_then(|meta| meta.get(SESSIONS_CHANGED_META_KEY))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

pub(super) fn handle_sessions_changed(
    notification: SessionsChangedNotification,
    _cx: &mut AsyncApp,
    context: &ClientContext,
) {
    let Some(session_list) = context.session_list.borrow().clone() else {
        log::debug!("Ignoring a session list change from an agent without session/list");
        return;
    };
    session_list.apply_change(notification);
}

impl AcpSessionList {
    fn apply_change(&self, notification: SessionsChangedNotification) {
        // The list holds a receiver of its own, so the channel stays open: without a
        // watcher, every change would queue for the life of the connection. A
        // watcher lists the sessions when it starts, so it misses nothing.
        if self.updates_tx.receiver_count() <= 1 {
            return;
        }
        let update = match notification.change {
            SessionChange::Removed => acp_thread::SessionListUpdate::SessionRemoved {
                session_id: acp_v2::SessionId::new(notification.session_id),
            },
            SessionChange::Added | SessionChange::Updated | SessionChange::Unknown => {
                acp_thread::SessionListUpdate::Refresh
            }
        };
        if let Err(error) = self.updates_tx.try_send(update) {
            log::warn!("Failed to forward a session list change: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acp::{ForegroundWork, drain_foreground_queue, v1::connect_client_future};
    use acp_thread::{AgentSessionList as _, SessionListUpdate};
    use agent_client_protocol::{Agent, ConnectionTo};
    use collections::HashMap;
    use futures::{StreamExt as _, channel::mpsc};
    use gpui::AppContext as _;
    use std::{cell::RefCell, rc::Rc};

    #[test]
    fn test_advertises_sessions_changed() {
        assert!(!advertises_sessions_changed(None));
        let meta = acp::Meta::from_iter([(SESSIONS_CHANGED_META_KEY.into(), true.into())]);
        assert!(advertises_sessions_changed(Some(&meta)));
        let meta = acp::Meta::from_iter([(SESSIONS_CHANGED_META_KEY.into(), "yes".into())]);
        assert!(!advertises_sessions_changed(Some(&meta)));
    }

    #[gpui::test]
    async fn test_sessions_changed_notifications_reach_the_list_watcher(
        cx: &mut gpui::TestAppContext,
    ) {
        let (client_transport, agent_transport) = agent_client_protocol::Channel::duplex();
        let (agent_connection_tx, agent_connection_rx) = futures::channel::oneshot::channel();
        let _agent_task = cx.background_spawn(Agent.builder().name("changes-agent").connect_with(
            agent_transport,
            move |connection: ConnectionTo<agent_client_protocol::Client>| async move {
                agent_connection_tx.send(connection).ok();
                futures::future::pending::<Result<(), acp::Error>>().await
            },
        ));

        let (dispatch_tx, mut dispatch_rx) = mpsc::unbounded::<ForegroundWork>();
        let (connection_tx, connection_rx) = futures::channel::oneshot::channel();
        let _client_task = cx.background_spawn(connect_client_future(
            "zed-test",
            client_transport,
            dispatch_tx.clone(),
            connection_tx,
            futures::future::pending(),
        ));
        let client_connection = connection_rx.await.expect("client connection");
        let agent_connection = agent_connection_rx.await.expect("agent connection");

        let session_list = Rc::new(AcpSessionList::new(client_connection, false));
        session_list.apply_change(SessionsChangedNotification {
            session_id: "unwatched".into(),
            change: SessionChange::Added,
        });
        assert!(
            session_list.updates_rx.is_empty(),
            "changes nobody watches are dropped"
        );
        let updates = cx
            .update(|cx| session_list.watch(cx))
            .expect("session list updates");
        let context = ClientContext {
            sessions: Rc::new(RefCell::new(HashMap::default())),
            session_list: Rc::new(RefCell::new(Some(session_list))),
            request_elicitations: cx.new(|_| acp_thread::ElicitationStore::default()),
            active_request_scopes: None,
            pending_session_updates: None,
            accepts_subagent_updates: false,
        };
        let _dispatch_task = cx.spawn(async move |mut cx| {
            while let Some(work) = dispatch_rx.next().await {
                work.run(&mut cx, &context);
            }
        });

        for (session_id, change) in [
            ("child-1", SessionChange::Added),
            ("child-1", SessionChange::Removed),
        ] {
            agent_connection
                .send_notification(SessionsChangedNotification {
                    session_id: session_id.into(),
                    change,
                })
                .expect("send sessions_changed");
        }
        agent_connection
            .send_notification(
                agent_client_protocol::UntypedMessage::new(
                    "_gorynych/sessions_changed",
                    serde_json::json!({"sessionId": "child-2", "change": "archived"}),
                )
                .expect("untyped notification"),
            )
            .expect("send future change kind");

        let mut received = Vec::new();
        for _ in 0..10 {
            cx.run_until_parked();
            drain_foreground_queue(&dispatch_tx)
                .await
                .expect("foreground queue");
            while let Ok(update) = updates.try_recv() {
                received.push(update);
            }
            if received.len() >= 3 {
                break;
            }
        }
        assert_eq!(received.len(), 3, "received {received:?}");
        assert!(matches!(received[0], SessionListUpdate::Refresh));
        assert!(matches!(
            &received[1],
            SessionListUpdate::SessionRemoved { session_id } if session_id.0.as_ref() == "child-1"
        ));
        assert!(matches!(received[2], SessionListUpdate::Refresh));
    }
}
