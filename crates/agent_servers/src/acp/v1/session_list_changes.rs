//! Gorynych extensions to the session list, each advertised in the `_meta` of the
//! agent's `initialize` response.
//!
//! ACP has no notification for changes to `session/list`, so clients have to poll it;
//! Gorynych pushes them with `_gorynych/sessions_changed`. It also keeps an archived
//! flag for its sessions, which clients set with `_gorynych/archive`.

use agent_client_protocol::{
    JsonRpcNotification, JsonRpcRequest, schema::v1 as acp, schema::v2 as acp_v2,
};
use anyhow::{Result, anyhow};
use gpui::{App, AsyncApp, Task};
use serde::{Deserialize, Serialize};

use super::{AcpSessionList, ClientContext};

/// Key in the `initialize` response `_meta` that advertises [`SessionsChangedNotification`].
pub const SESSIONS_CHANGED_META_KEY: &str = "gorynych/sessionsChanged";

/// Key in the `initialize` response `_meta` that advertises the session archive:
/// archived flags in the `_meta` of sessions, listings that include archived
/// sessions on request, and [`ArchiveSessionRequest`].
pub const ARCHIVE_META_KEY: &str = "gorynych/archive";

/// Archives or restores a session. Archiving neither closes nor unloads it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonRpcRequest)]
#[request(method = "_gorynych/archive", response = serde_json::Value)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveSessionRequest {
    pub session_id: String,
    pub archived: bool,
}

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
    advertises(meta, SESSIONS_CHANGED_META_KEY)
}

pub(super) fn advertises_archive(meta: Option<&acp::Meta>) -> bool {
    advertises(meta, ARCHIVE_META_KEY)
}

fn advertises(meta: Option<&acp::Meta>, key: &str) -> bool {
    meta.and_then(|meta| meta.get(key))
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

    pub(super) fn send_archive_request(
        &self,
        session_id: &acp_v2::SessionId,
        archived: bool,
        cx: &mut App,
    ) -> Task<Result<()>> {
        if !self.supports_archive {
            return Task::ready(Err(anyhow!("the agent has no session archive")));
        }
        let connection = self.connection.clone();
        let request = ArchiveSessionRequest {
            session_id: session_id.0.to_string(),
            archived,
        };
        cx.foreground_executor().spawn(async move {
            connection
                .send_request(request)
                .block_task()
                .await
                .map_err(crate::acp::map_acp_error)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acp::{ForegroundWork, drain_foreground_queue, v1::connect_client_future};
    use acp_thread::{AgentSessionList as _, SessionListUpdate};
    use agent_client_protocol::{Agent, Client, ConnectionTo};
    use collections::HashMap;
    use futures::{StreamExt as _, channel::mpsc};
    use gpui::AppContext as _;
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    #[test]
    fn test_advertises_sessions_changed() {
        assert!(!advertises_sessions_changed(None));
        let meta = acp::Meta::from_iter([(SESSIONS_CHANGED_META_KEY.into(), true.into())]);
        assert!(advertises_sessions_changed(Some(&meta)));
        assert!(!advertises_archive(Some(&meta)));
        let meta = acp::Meta::from_iter([(SESSIONS_CHANGED_META_KEY.into(), "yes".into())]);
        assert!(!advertises_sessions_changed(Some(&meta)));
        let meta = acp::Meta::from_iter([(ARCHIVE_META_KEY.into(), true.into())]);
        assert!(advertises_archive(Some(&meta)));
    }

    #[gpui::test]
    async fn test_archive_requests_and_listing_meta_reach_the_agent(cx: &mut gpui::TestAppContext) {
        let (client_transport, agent_transport) = agent_client_protocol::Channel::duplex();
        let archive_requests = Arc::new(Mutex::new(Vec::new()));
        let listing_metas = Arc::new(Mutex::new(Vec::new()));
        let _agent_task = cx.background_spawn(
            Agent
                .builder()
                .name("archive-agent")
                .on_receive_request(
                    {
                        let archive_requests = archive_requests.clone();
                        async move |request: ArchiveSessionRequest, responder, _cx| {
                            let known = request.session_id == "known";
                            archive_requests
                                .lock()
                                .expect("archive requests")
                                .push((request.session_id, request.archived));
                            if known {
                                responder.respond(serde_json::json!({}))
                            } else {
                                responder.respond_with_error(acp::Error::resource_not_found(None))
                            }
                        }
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    {
                        let listing_metas = listing_metas.clone();
                        async move |request: acp::ListSessionsRequest, responder, _cx| {
                            listing_metas
                                .lock()
                                .expect("listing metas")
                                .push(request.meta);
                            responder.respond(acp::ListSessionsResponse::new(Vec::new()))
                        }
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .connect_to(agent_transport),
        );
        let (connection_tx, connection_rx) = futures::channel::oneshot::channel();
        let _client_task =
            cx.background_spawn(Client.builder().name("archive-client").connect_with(
                client_transport,
                move |connection: ConnectionTo<Agent>| async move {
                    connection_tx.send(connection).ok();
                    futures::future::pending::<Result<(), acp::Error>>().await
                },
            ));
        let connection = connection_rx.await.expect("client connection");

        let mut session_list = AcpSessionList::new(connection, false);
        let known = acp_v2::SessionId::new("known");
        cx.update(|cx| session_list.set_session_archived(&known, true, cx))
            .await
            .expect_err("an agent that doesn't advertise the archive gets no requests");

        session_list.supports_archive = true;
        cx.update(|cx| session_list.set_session_archived(&known, true, cx))
            .await
            .expect("archive a known session");
        let error = cx
            .update(|cx| {
                session_list.set_session_archived(&acp_v2::SessionId::new("unknown"), false, cx)
            })
            .await
            .expect_err("restore an unknown session");
        assert_eq!(
            acp_thread::ProtocolErrorInfo::from_error(&error).map(|info| info.code),
            Some(-32002)
        );
        assert_eq!(
            *archive_requests.lock().expect("archive requests"),
            [("known".to_string(), true), ("unknown".to_string(), false)]
        );

        let meta = acp_v2::Meta::from_iter([("gorynych/includeArchived".into(), true.into())]);
        cx.update(|cx| {
            session_list.list_sessions(
                acp_thread::AgentSessionListRequest {
                    meta: Some(meta.clone()),
                    ..Default::default()
                },
                cx,
            )
        })
        .await
        .expect("list sessions");
        assert_eq!(*listing_metas.lock().expect("listing metas"), [Some(meta)]);
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
