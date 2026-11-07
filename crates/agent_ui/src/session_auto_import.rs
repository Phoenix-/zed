//! Keeps the sidebar's threads in sync with the sessions of external agents that
//! have `auto_import_sessions` enabled.
//!
//! Sessions an agent creates outside of Zed show up as threads, without going
//! through the manual import modal. The agent's session list is read when Zed
//! connects to it, whenever the agent reports a change, and on a slow timer for
//! agents that don't report changes. Sessions that an agent reports as started by
//! another session are linked to their parent, so the sidebar can nest them.

use std::{
    path::PathBuf,
    rc::{Rc, Weak},
    time::{Duration, Instant},
};

use acp_thread::{
    AgentSessionInfo, AgentSessionInfoUpdate, AgentSessionList, AgentSessionListRequest,
    SessionListUpdate,
};
use agent::ThreadStore;
use agent_client_protocol::schema::v2 as acp;
use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use collections::{HashMap, HashSet};
use db::{
    sqlez::{domain::Domain, statement::Statement, thread_safe_connection::ThreadSafeConnection},
    sqlez_macros::sql,
};
use fs::Fs;
use futures::{FutureExt as _, future::Shared};
use gpui::{
    App, AppContext as _, AsyncApp, Context, Entity, EntityId, Global, Subscription, Task,
    WeakEntity,
};
use project::{
    AgentId,
    agent_server_store::{AllAgentServersSettings, CustomAgentServerSettings},
};
use remote::{RemoteConnectionOptions, same_remote_connection_identity};
use settings::SettingsStore;
use util::{ResultExt as _, path_list::PathList};

use crate::{
    Agent, ConversationView,
    agent_connection_store::{AgentConnectionEntry, AgentConnectionStore},
    thread_metadata_store::{ThreadId, ThreadMetadata, ThreadMetadataStore, WorktreePaths},
    thread_parent_store::{ThreadParentStore, parent_session_id_from_meta},
};

/// How often to list the sessions of agents that don't report changes themselves.
const POLL_INTERVAL: Duration = Duration::from_secs(120);
/// Agents may report a burst of changes at once; they are coalesced into one listing.
const REFRESH_DEBOUNCE: Duration = Duration::from_millis(250);
/// A connection this importer starts is retried after this delay, doubled with each
/// attempt, and given up after `MAX_CONNECT_ATTEMPTS` until the agent's settings
/// change or `STABLE_CONNECTION` passes without another attempt.
const CONNECT_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_CONNECT_ATTEMPTS: u32 = 3;
const STABLE_CONNECTION: Duration = Duration::from_secs(10 * 60);

pub fn init(cx: &mut App) {
    let auto_import = cx.new(SessionAutoImport::new);
    cx.set_global(GlobalSessionAutoImport {
        _auto_import: auto_import,
    });
}

struct GlobalSessionAutoImport {
    _auto_import: Entity<SessionAutoImport>,
}

impl Global for GlobalSessionAutoImport {}

pub struct SessionAutoImport {
    watchers: Vec<SessionListWatcher>,
    /// Connections this importer requested and that haven't been established yet.
    pending_connections: Vec<(AgentId, WeakEntity<AgentConnectionStore>)>,
    connect_attempts: HashMap<AgentId, ConnectAttempts>,
    unsupported_agents: HashSet<AgentId>,
    /// Each agent's settings as last seen, so that editing one agent's entry retries
    /// only that agent.
    agent_settings: HashMap<AgentId, CustomAgentServerSettings>,
    stores: HashMap<EntityId, (WeakEntity<AgentConnectionStore>, [Subscription; 2])>,
    conversation_views: HashMap<EntityId, WeakEntity<ConversationView>>,
    known_sessions: KnownSessions,
}

struct SessionListWatcher {
    agent_id: AgentId,
    remote_connection: Option<RemoteConnectionOptions>,
    session_list: Weak<dyn AgentSessionList>,
    _task: Task<()>,
}

impl SessionListWatcher {
    fn watches(
        &self,
        agent_id: &AgentId,
        remote_connection: Option<&RemoteConnectionOptions>,
    ) -> bool {
        &self.agent_id == agent_id
            && same_remote_connection_identity(self.remote_connection.as_ref(), remote_connection)
    }
}

#[derive(Clone, Copy, Debug)]
struct ConnectAttempts {
    count: u32,
    last_attempt: Instant,
}

impl ConnectAttempts {
    fn allows_attempt(&mut self, now: Instant) -> bool {
        let since_last_attempt = now.saturating_duration_since(self.last_attempt);
        if since_last_attempt >= STABLE_CONNECTION {
            self.count = 0;
            return true;
        }
        self.count < MAX_CONNECT_ATTEMPTS
            && since_last_attempt >= CONNECT_RETRY_DELAY * 2u32.pow(self.count.saturating_sub(1))
    }
}

/// The sessions of each agent that had a thread in a listing, with whether this
/// importer created that thread.
///
/// They are persisted, so that a session whose thread was deleted in Zed is not
/// imported again, and so that only threads created here are dropped when their
/// sessions disappear from a listing.
struct KnownSessions {
    sessions: HashMap<AgentId, HashMap<acp::SessionId, bool>>,
    loaded: Shared<Task<()>>,
    /// Writes go through one queue so that they apply in the order they are made.
    pending_writes: async_channel::Sender<KnownSessionWrite>,
    _write_task: Task<()>,
}

enum KnownSessionWrite {
    Save {
        agent_id: String,
        session_id: String,
        imported: bool,
    },
    Delete {
        agent_id: String,
        session_id: String,
    },
}

impl KnownSessions {
    fn of(&self, agent_id: &AgentId) -> Option<&HashMap<acp::SessionId, bool>> {
        self.sessions.get(agent_id)
    }

    fn insert(&mut self, agent_id: &AgentId, session_id: acp::SessionId, imported: bool) {
        let previous = self
            .sessions
            .entry(agent_id.clone())
            .or_default()
            .insert(session_id.clone(), imported);
        if previous != Some(imported) {
            self.pending_writes
                .try_send(KnownSessionWrite::Save {
                    agent_id: agent_id.to_string(),
                    session_id: session_id.0.to_string(),
                    imported,
                })
                .log_err();
        }
    }

    fn remove(&mut self, agent_id: &AgentId, session_id: &acp::SessionId) {
        let removed = self
            .sessions
            .get_mut(agent_id)
            .and_then(|sessions| sessions.remove(session_id));
        if removed.is_some() {
            self.pending_writes
                .try_send(KnownSessionWrite::Delete {
                    agent_id: agent_id.to_string(),
                    session_id: session_id.0.to_string(),
                })
                .log_err();
        }
    }
}

impl SessionAutoImport {
    fn new(cx: &mut Context<Self>) -> Self {
        #[cfg(any(test, feature = "test-support"))]
        let db = SessionAutoImportDb(gpui::block_on(db::open_test_db::<SessionAutoImportDb>(
            &format!("SESSION_AUTO_IMPORT_{}", uuid::Uuid::new_v4()),
        )));
        #[cfg(not(any(test, feature = "test-support")))]
        let db = SessionAutoImportDb::global(cx);
        Self::with_db(db, cx)
    }

    fn with_db(db: SessionAutoImportDb, cx: &mut Context<Self>) -> Self {
        let this = cx.weak_entity();

        cx.observe_new::<AgentConnectionStore>({
            let this = this.clone();
            move |_store, _window, cx| {
                let store = cx.weak_entity();
                let this = this.clone();
                // The store is still being created, so it can't be read yet.
                cx.defer(move |cx| {
                    if let Some(store) = store.upgrade() {
                        this.update(cx, |this, cx| this.register_store(store, cx))
                            .ok();
                    }
                });
            }
        })
        .detach();

        cx.observe_new::<ConversationView>({
            let this = this.clone();
            move |_view, _window, cx| {
                let view = cx.weak_entity();
                let view_id = view.entity_id();
                this.update(cx, |this, _| {
                    this.conversation_views.insert(view_id, view);
                })
                .ok();
                let this = this.clone();
                cx.on_release(move |_view, cx| {
                    this.update(cx, |this, _| {
                        this.conversation_views.remove(&view_id);
                    })
                    .ok();
                })
                .detach();
            }
        })
        .detach();

        cx.observe_global::<SettingsStore>(|this, cx| {
            this.forget_attempts_of_changed_agents(cx);
            this.sync_all_stores(cx);
        })
        .detach();

        let rows = cx.background_spawn({
            let db = db.clone();
            async move { db.list() }
        });
        let loaded = cx
            .spawn(async move |this, cx| {
                let Some(rows) = rows
                    .await
                    .context("Failed to load the sessions known to auto-import")
                    .log_err()
                else {
                    return;
                };
                this.update(cx, |this, _| {
                    for (agent_id, session_id, imported) in rows {
                        // Sessions recorded while loading are newer than the stored ones.
                        this.known_sessions
                            .sessions
                            .entry(AgentId::new(agent_id))
                            .or_default()
                            .entry(acp::SessionId::new(session_id))
                            .or_insert(imported);
                    }
                })
                .ok();
            })
            .shared();
        let (pending_writes, writes) = async_channel::unbounded::<KnownSessionWrite>();
        let write_task = cx.background_spawn(async move {
            while let Ok(write) = writes.recv().await {
                db.apply(write).await.log_err();
            }
        });

        Self {
            watchers: Vec::new(),
            pending_connections: Vec::new(),
            connect_attempts: HashMap::default(),
            unsupported_agents: HashSet::default(),
            agent_settings: HashMap::default(),
            stores: HashMap::default(),
            conversation_views: HashMap::default(),
            known_sessions: KnownSessions {
                sessions: HashMap::default(),
                loaded,
                pending_writes,
                _write_task: write_task,
            },
        }
    }

    fn register_store(&mut self, store: Entity<AgentConnectionStore>, cx: &mut Context<Self>) {
        let store_id = store.entity_id();
        let subscriptions = [
            cx.observe(&store, |this, store, cx| this.sync_store(&store, cx)),
            cx.observe_release(&store, move |this, _store, cx| {
                this.stores.remove(&store_id);
                this.sync_all_stores(cx);
            }),
        ];
        self.stores
            .insert(store_id, (store.downgrade(), subscriptions));
        self.sync_store(&store, cx);
    }

    fn sync_all_stores(&mut self, cx: &mut Context<Self>) {
        let stores = self
            .stores
            .values()
            .filter_map(|(store, _)| store.upgrade())
            .collect::<Vec<_>>();
        for store in stores {
            self.sync_store(&store, cx);
        }
    }

    fn forget_attempts_of_changed_agents(&mut self, cx: &App) {
        let Some(settings_store) = cx.try_global::<SettingsStore>() else {
            return;
        };
        let agent_settings = settings_store
            .get::<AllAgentServersSettings>(None)
            .iter()
            .map(|(agent_id, settings)| (AgentId::new(agent_id.clone()), settings.clone()))
            .collect::<HashMap<_, _>>();
        for agent_id in changed_agents(&self.agent_settings, &agent_settings) {
            self.connect_attempts.remove(&agent_id);
            self.unsupported_agents.remove(&agent_id);
        }
        self.agent_settings = agent_settings;
    }

    /// Watches the session lists of the agents this store is connected to, and
    /// connects to enabled agents nobody is connected to yet.
    fn sync_store(&mut self, store: &Entity<AgentConnectionStore>, cx: &mut Context<Self>) {
        let agent_ids = auto_import_agent_ids(cx);
        self.watchers.retain(|watcher| {
            watcher.session_list.strong_count() > 0 && agent_ids.contains(&watcher.agent_id)
        });
        self.pending_connections
            .retain(|(agent_id, store)| store.upgrade().is_some() && agent_ids.contains(agent_id));

        let (remote_connection, is_local) = {
            let project = store.read(cx).project().read(cx);
            (project.remote_connection_options(cx), project.is_local())
        };

        for agent_id in agent_ids {
            let agent = Agent::from(agent_id.clone());
            let connection = store
                .read(cx)
                .entry(&agent)
                .map(|entry| match entry.read(cx) {
                    AgentConnectionEntry::Connected(state) => Some(state.connection.clone()),
                    AgentConnectionEntry::Connecting { .. }
                    | AgentConnectionEntry::Error { .. } => None,
                });
            let requested_here = self
                .pending_connections
                .iter()
                .any(|(pending, pending_store)| {
                    pending == &agent_id && pending_store.entity_id() == store.entity_id()
                });
            if requested_here && !matches!(connection, Some(None)) {
                self.pending_connections.retain(|(pending, pending_store)| {
                    pending != &agent_id || pending_store.entity_id() != store.entity_id()
                });
            }

            match connection {
                Some(Some(connection)) => {
                    if let Some(session_list) = connection.session_list(cx) {
                        self.watch(agent_id, remote_connection.clone(), session_list, cx);
                    } else if self.unsupported_agents.insert(agent_id.clone()) {
                        log::warn!(
                            "Not importing sessions of {agent_id}: it doesn't support listing sessions"
                        );
                    }
                }
                Some(None) => {}
                None => {
                    if requested_here {
                        // The store drops entries whose connection failed.
                        log::warn!("Connecting to {agent_id} to import its sessions failed");
                    }
                    let now = cx.background_executor().now();
                    let should_connect = is_local
                        && !self.is_watching(&agent_id, None)
                        && !self
                            .pending_connections
                            .iter()
                            .any(|(pending, _)| pending == &agent_id)
                        && self
                            .connect_attempts
                            .get_mut(&agent_id)
                            .is_none_or(|attempts| attempts.allows_attempt(now));
                    if should_connect {
                        self.connect(store, agent_id, now, cx);
                    }
                }
            }
        }
    }

    fn connect(
        &mut self,
        store: &Entity<AgentConnectionStore>,
        agent_id: AgentId,
        now: Instant,
        cx: &mut Context<Self>,
    ) {
        let attempts = self
            .connect_attempts
            .entry(agent_id.clone())
            .or_insert(ConnectAttempts {
                count: 0,
                last_attempt: now,
            });
        attempts.count += 1;
        attempts.last_attempt = now;
        if attempts.count == MAX_CONNECT_ATTEMPTS {
            log::warn!(
                "Connecting to {agent_id} to import its sessions for the last time before giving up"
            );
        }

        let agent = Agent::from(agent_id.clone());
        let server = agent.server(<dyn Fs>::global(cx), ThreadStore::global(cx));
        self.pending_connections.push((agent_id, store.downgrade()));
        store.update(cx, |store, cx| {
            store.request_connection(agent, server, cx);
        });
    }

    fn is_watching(
        &self,
        agent_id: &AgentId,
        remote_connection: Option<&RemoteConnectionOptions>,
    ) -> bool {
        self.watchers.iter().any(|watcher| {
            watcher.watches(agent_id, remote_connection) && watcher.session_list.strong_count() > 0
        })
    }

    pub fn watch(
        &mut self,
        agent_id: AgentId,
        remote_connection: Option<RemoteConnectionOptions>,
        session_list: Rc<dyn AgentSessionList>,
        cx: &mut Context<Self>,
    ) {
        if self.is_watching(&agent_id, remote_connection.as_ref()) {
            return;
        }
        self.watchers
            .retain(|watcher| !watcher.watches(&agent_id, remote_connection.as_ref()));

        let updates = session_list.watch(cx);
        let poll_interval = (!session_list.notifies_changes()).then_some(POLL_INTERVAL);
        let task = cx.spawn({
            let agent_id = agent_id.clone();
            let remote_connection = remote_connection.clone();
            let session_list = Rc::downgrade(&session_list);
            async move |this, cx| {
                watch_session_list(
                    this,
                    agent_id,
                    remote_connection,
                    session_list,
                    updates,
                    poll_interval,
                    cx,
                )
                .await
            }
        });
        self.watchers.push(SessionListWatcher {
            agent_id,
            remote_connection,
            session_list: Rc::downgrade(&session_list),
            _task: task,
        });
    }

    fn open_thread_ids(&self, cx: &App) -> HashSet<ThreadId> {
        self.conversation_views
            .values()
            .filter_map(|view| view.upgrade())
            .map(|view| view.read(cx).thread_id)
            .collect()
    }

    /// Sessions of `agent_id` that a live thread holds, including drafts whose
    /// metadata doesn't name their session yet.
    fn live_sessions(&self, agent_id: &AgentId, cx: &App) -> HashSet<acp::SessionId> {
        self.conversation_views
            .values()
            .filter_map(|view| view.upgrade())
            .filter_map(|view| view.read(cx).root_thread(cx))
            .filter_map(|thread| {
                let thread = thread.read(cx);
                (&thread.connection().agent_id() == agent_id).then(|| thread.session_id().clone())
            })
            .collect()
    }

    fn sync_sessions(
        &mut self,
        agent_id: &AgentId,
        remote_connection: Option<&RemoteConnectionOptions>,
        listing: SessionListing,
        cx: &mut Context<Self>,
    ) {
        let open_thread_ids = self.open_thread_ids(cx);
        let live_sessions = self.live_sessions(agent_id, cx);
        let store = ThreadMetadataStore::global(cx);
        // The listing is authoritative for the parents of the sessions it contains.
        let parent_links = listing
            .sessions
            .iter()
            .map(|session| {
                (
                    session.session_id.clone(),
                    parent_session_id_from_meta(session.meta.as_ref()),
                )
            })
            .collect::<Vec<_>>();
        let threads = agent_threads(store.read(cx), agent_id);
        let mut sessions_with_threads = threads
            .keys()
            .map(|session_id| (*session_id).clone())
            .collect::<HashSet<_>>();
        let no_known_sessions = HashMap::default();
        let plan = plan_sync(
            agent_id,
            remote_connection,
            listing,
            &threads,
            self.known_sessions
                .of(agent_id)
                .unwrap_or(&no_known_sessions),
            &open_thread_ids,
            &live_sessions,
            Utc::now(),
        );
        sessions_with_threads.extend(
            plan.save
                .iter()
                .filter_map(|thread| thread.session_id.clone()),
        );
        if let Some(parent_store) = ThreadParentStore::try_global(cx) {
            parent_store.update(cx, |parent_store, cx| {
                for (session_id, parent_session_id) in parent_links {
                    if sessions_with_threads.contains(&session_id) {
                        parent_store.set_parent_session_id(
                            agent_id.clone(),
                            session_id,
                            parent_session_id,
                            cx,
                        );
                    }
                }
            });
        }

        for (session_id, imported) in plan.known {
            self.known_sessions.insert(agent_id, session_id, imported);
        }
        for session_id in &plan.forgotten {
            self.known_sessions.remove(agent_id, session_id);
        }
        if !plan.save.is_empty() {
            store.update(cx, |store, cx| store.save_all(plan.save, cx));
        }
        for thread_id in plan.delete {
            delete_thread(thread_id, cx);
        }
    }

    fn apply_session_info(
        &mut self,
        agent_id: &AgentId,
        session_id: acp::SessionId,
        update: AgentSessionInfoUpdate,
        cx: &mut Context<Self>,
    ) {
        let mut info = AgentSessionInfo::new(session_id.clone());
        info.apply_update(update);
        // An update that doesn't name a parent leaves the link alone: agents
        // report the link in full only in the session list.
        let Some(parent_session_id) = parent_session_id_from_meta(info.meta.as_ref()) else {
            return;
        };
        if let Some(parent_store) = ThreadParentStore::try_global(cx) {
            parent_store.update(cx, |parent_store, cx| {
                parent_store.set_parent_session_id(
                    agent_id.clone(),
                    session_id,
                    Some(parent_session_id),
                    cx,
                );
            });
        }
    }

    /// The agent no longer has the session, so its thread could not be loaded
    /// anymore: it goes even from the archive, unless a view of it is open.
    fn remove_session(
        &mut self,
        agent_id: &AgentId,
        session_id: &acp::SessionId,
        cx: &mut Context<Self>,
    ) {
        self.known_sessions.remove(agent_id, session_id);
        let open_thread_ids = self.open_thread_ids(cx);
        let thread_id = agent_threads(ThreadMetadataStore::global(cx).read(cx), agent_id)
            .get(session_id)
            .map(|thread| thread.thread_id)
            .filter(|thread_id| !open_thread_ids.contains(thread_id));
        if let Some(thread_id) = thread_id {
            delete_thread(thread_id, cx);
        }
    }
}

fn delete_thread(thread_id: ThreadId, cx: &mut App) {
    cx.spawn(async move |cx| {
        // As when deleting from the archive, archived worktrees that only this
        // thread references go with it.
        crate::thread_worktree_archive::cleanup_thread_archived_worktrees(thread_id, cx).await;
        cx.update(|cx| {
            ThreadMetadataStore::global(cx).update(cx, |store, cx| store.delete(thread_id, cx));
        });
    })
    .detach();
}

/// The store indexes threads by session alone, but two agents may use the same
/// session id.
fn agent_threads<'a>(
    store: &'a ThreadMetadataStore,
    agent_id: &AgentId,
) -> HashMap<&'a acp::SessionId, &'a ThreadMetadata> {
    store
        .entries()
        .filter(|thread| &thread.agent_id == agent_id)
        .filter_map(|thread| Some((thread.session_id.as_ref()?, thread)))
        .collect()
}

fn auto_import_agent_ids(cx: &App) -> Vec<AgentId> {
    let Some(settings_store) = cx.try_global::<SettingsStore>() else {
        return Vec::new();
    };
    settings_store
        .get::<AllAgentServersSettings>(None)
        .iter()
        .filter(|(_, settings)| settings.auto_import_sessions())
        .map(|(agent_id, _)| AgentId::new(agent_id.clone()))
        .collect()
}

fn changed_agents(
    previous: &HashMap<AgentId, CustomAgentServerSettings>,
    current: &HashMap<AgentId, CustomAgentServerSettings>,
) -> HashSet<AgentId> {
    previous
        .keys()
        .chain(current.keys())
        .filter(|agent_id| previous.get(*agent_id) != current.get(*agent_id))
        .cloned()
        .collect()
}

/// Separators an agent writes differently from Zed make the same folder look like
/// another one in stored and displayed paths.
fn normalize_local_paths(paths: &PathList) -> PathList {
    PathList::new(
        &paths
            .ordered_paths()
            .map(|path| path.components().collect::<PathBuf>())
            .collect::<Vec<_>>(),
    )
}

struct SessionListing {
    sessions: Vec<AgentSessionInfo>,
    /// Whether every page was read, so that a session missing from the listing is
    /// gone from the agent.
    complete: bool,
}

#[derive(Default)]
struct SyncPlan {
    save: Vec<ThreadMetadata>,
    delete: Vec<ThreadId>,
    /// Sessions that had a thread in this listing, with whether it is created now.
    known: Vec<(acp::SessionId, bool)>,
    forgotten: Vec<acp::SessionId>,
}

/// Plans how a listing changes `agent_id`'s threads.
///
/// New sessions become threads, and known threads take the agent's title and
/// newer activity time. A known session without a thread was deleted in Zed and
/// is skipped, and so is a session that a live thread holds before its metadata
/// names it. When the listing is complete, a session missing from it is
/// forgotten, and its thread is deleted if this importer created it, unless the
/// thread is archived or open.
fn plan_sync(
    agent_id: &AgentId,
    remote_connection: Option<&RemoteConnectionOptions>,
    listing: SessionListing,
    threads: &HashMap<&acp::SessionId, &ThreadMetadata>,
    known_sessions: &HashMap<acp::SessionId, bool>,
    open_thread_ids: &HashSet<ThreadId>,
    live_sessions: &HashSet<acp::SessionId>,
    now: DateTime<Utc>,
) -> SyncPlan {
    let mut plan = SyncPlan::default();
    let mut listed = HashSet::default();
    for session in listing.sessions {
        if !listed.insert(session.session_id.clone()) {
            continue;
        }

        if let Some(existing) = threads.get(&session.session_id) {
            if !known_sessions.contains_key(&session.session_id) {
                plan.known.push((session.session_id.clone(), false));
            }
            let mut updated = (*existing).clone();
            if let Some(updated_at) = session.updated_at {
                updated.updated_at = updated.updated_at.max(updated_at);
            }
            // Open threads get their titles from the live session.
            if session.title.is_some() && !open_thread_ids.contains(&existing.thread_id) {
                updated.title = session.title;
            }
            if &updated != *existing {
                plan.save.push(updated);
            }
            continue;
        }

        if known_sessions.contains_key(&session.session_id)
            || live_sessions.contains(&session.session_id)
        {
            continue;
        }
        let Some(work_dirs) = session.work_dirs else {
            continue;
        };
        let work_dirs = match remote_connection {
            Some(_) => work_dirs,
            None => normalize_local_paths(&work_dirs),
        };
        let worktree_paths = WorktreePaths::from_folder_paths(&work_dirs);
        plan.known.push((session.session_id.clone(), true));
        plan.save.push(ThreadMetadata {
            thread_id: ThreadId::new(),
            session_id: Some(session.session_id),
            agent_id: agent_id.clone(),
            title: session.title,
            title_override: None,
            updated_at: session.updated_at.unwrap_or(now),
            created_at: session.created_at,
            interacted_at: None,
            // Like threads started in an empty window, threads without a folder
            // would never show in the sidebar, so they go to the archive.
            archived: worktree_paths.is_empty(),
            worktree_paths,
            remote_connection: remote_connection.cloned(),
        });
    }

    if listing.complete {
        for (session_id, imported) in known_sessions {
            if listed.contains(session_id) {
                continue;
            }
            plan.forgotten.push(session_id.clone());
            if let Some(thread) = threads.get(session_id)
                && *imported
                && !thread.archived
                && !open_thread_ids.contains(&thread.thread_id)
            {
                plan.delete.push(thread.thread_id);
            }
        }
    }
    plan
}

async fn watch_session_list(
    this: WeakEntity<SessionAutoImport>,
    agent_id: AgentId,
    remote_connection: Option<RemoteConnectionOptions>,
    session_list: Weak<dyn AgentSessionList>,
    updates: Option<async_channel::Receiver<SessionListUpdate>>,
    poll_interval: Option<Duration>,
    cx: &mut AsyncApp,
) {
    // Until these have loaded, every session would look unknown.
    let Ok((metadata_loaded, known_sessions_loaded, parents_loaded)) =
        this.read_with(cx, |this, cx| {
            (
                ThreadMetadataStore::global(cx).read(cx).reload_task(),
                this.known_sessions.loaded.clone(),
                ThreadParentStore::try_global(cx).map(|store| store.read(cx).reload_task()),
            )
        })
    else {
        return;
    };
    metadata_loaded.await;
    known_sessions_loaded.await;
    if let Some(parents_loaded) = parents_loaded {
        parents_loaded.await;
    }

    let mut needs_listing = true;
    loop {
        if needs_listing {
            needs_listing = false;
            let Some(list) = session_list.upgrade() else {
                break;
            };
            match list_all_sessions(list, cx).await {
                Ok(listing) => {
                    let synced = this.update(cx, |this, cx| {
                        this.sync_sessions(&agent_id, remote_connection.as_ref(), listing, cx)
                    });
                    if synced.is_err() {
                        break;
                    }
                }
                Err(error) => log::warn!("Failed to list the sessions of {agent_id}: {error:#}"),
            }
        }

        let update = match next_event(updates.as_ref(), poll_interval, cx).await {
            WatchEvent::Closed => break,
            WatchEvent::Poll => {
                needs_listing = true;
                continue;
            }
            WatchEvent::Update(update) => update,
        };
        let mut pending_updates = vec![update];
        if matches!(pending_updates[0], SessionListUpdate::Refresh) {
            cx.background_executor().timer(REFRESH_DEBOUNCE).await;
            if let Some(updates) = updates.as_ref() {
                while let Ok(update) = updates.try_recv() {
                    pending_updates.push(update);
                }
            }
        }
        for update in pending_updates {
            match update {
                SessionListUpdate::Refresh => needs_listing = true,
                SessionListUpdate::SessionRemoved { session_id } => {
                    this.update(cx, |this, cx| {
                        this.remove_session(&agent_id, &session_id, cx)
                    })
                    .ok();
                }
                SessionListUpdate::SessionInfo { session_id, update } => {
                    this.update(cx, |this, cx| {
                        this.apply_session_info(&agent_id, session_id, update, cx)
                    })
                    .ok();
                }
            }
        }
    }
}

enum WatchEvent {
    Update(SessionListUpdate),
    Poll,
    Closed,
}

async fn next_event(
    updates: Option<&async_channel::Receiver<SessionListUpdate>>,
    poll_interval: Option<Duration>,
    cx: &AsyncApp,
) -> WatchEvent {
    let update = async {
        match updates {
            Some(updates) => match updates.recv().await {
                Ok(update) => WatchEvent::Update(update),
                Err(_) => WatchEvent::Closed,
            },
            None => futures::future::pending().await,
        }
    };
    let poll = async {
        match poll_interval {
            Some(poll_interval) => {
                cx.background_executor().timer(poll_interval).await;
                WatchEvent::Poll
            }
            None => futures::future::pending().await,
        }
    };
    futures::pin_mut!(update, poll);
    futures::future::select(update, poll).await.factor_first().0
}

async fn list_all_sessions(
    session_list: Rc<dyn AgentSessionList>,
    cx: &mut AsyncApp,
) -> Result<SessionListing> {
    let mut sessions = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let request = AgentSessionListRequest {
            cursor: cursor.clone(),
            ..Default::default()
        };
        let response = cx
            .update(|cx| session_list.list_sessions(request, cx))
            .await?;
        sessions.extend(response.sessions);
        match response.next_cursor {
            None => {
                return Ok(SessionListing {
                    sessions,
                    complete: true,
                });
            }
            Some(next_cursor) if Some(&next_cursor) != cursor.as_ref() => {
                cursor = Some(next_cursor)
            }
            // An agent that repeats its cursor would loop forever; keep what it listed.
            Some(_) => {
                return Ok(SessionListing {
                    sessions,
                    complete: false,
                });
            }
        }
    }
}

struct SessionAutoImportDb(ThreadSafeConnection);

impl Domain for SessionAutoImportDb {
    const NAME: &str = stringify!(SessionAutoImportDb);

    const MIGRATIONS: &[&str] = &[sql!(
        CREATE TABLE IF NOT EXISTS auto_import_known_sessions(
            agent_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            imported INTEGER NOT NULL,
            PRIMARY KEY (agent_id, session_id)
        ) STRICT;
    )];
}

db::static_connection!(SessionAutoImportDb, []);

impl SessionAutoImportDb {
    fn list(&self) -> anyhow::Result<Vec<(String, String, bool)>> {
        self.select::<(String, String, bool)>(
            "SELECT agent_id, session_id, imported FROM auto_import_known_sessions",
        )?()
    }

    async fn apply(&self, write: KnownSessionWrite) -> anyhow::Result<()> {
        self.write(move |conn| match write {
            KnownSessionWrite::Save {
                agent_id,
                session_id,
                imported,
            } => {
                let mut statement = Statement::prepare(
                    conn,
                    "INSERT INTO auto_import_known_sessions(agent_id, session_id, imported) \
                     VALUES (?1, ?2, ?3) \
                     ON CONFLICT(agent_id, session_id) DO UPDATE SET imported = excluded.imported",
                )?;
                let mut index = statement.bind(&agent_id, 1)?;
                index = statement.bind(&session_id, index)?;
                statement.bind(&imported, index)?;
                statement.exec()
            }
            KnownSessionWrite::Delete {
                agent_id,
                session_id,
            } => {
                let mut statement = Statement::prepare(
                    conn,
                    "DELETE FROM auto_import_known_sessions WHERE agent_id = ?1 AND session_id = ?2",
                )?;
                let index = statement.bind(&agent_id, 1)?;
                statement.bind(&session_id, index)?;
                statement.exec()
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acp_thread::AgentSessionListResponse;
    use gpui::TestAppContext;
    use std::{
        any::Any,
        cell::{Cell, RefCell},
        path::Path,
    };

    const PAGE_SIZE: usize = 2;

    struct FakeSessionList {
        sessions: RefCell<Vec<AgentSessionInfo>>,
        notifies_changes: bool,
        list_calls: Cell<usize>,
        updates_tx: async_channel::Sender<SessionListUpdate>,
        updates_rx: async_channel::Receiver<SessionListUpdate>,
    }

    impl FakeSessionList {
        fn new(notifies_changes: bool, sessions: Vec<AgentSessionInfo>) -> Rc<Self> {
            let (updates_tx, updates_rx) = async_channel::unbounded();
            Rc::new(Self {
                sessions: RefCell::new(sessions),
                notifies_changes,
                list_calls: Cell::new(0),
                updates_tx,
                updates_rx,
            })
        }

        fn send(&self, update: SessionListUpdate) {
            self.updates_tx
                .try_send(update)
                .expect("send session list update");
        }

        fn refresh(&self, cx: &mut TestAppContext) {
            self.send(SessionListUpdate::Refresh);
            cx.executor().advance_clock(REFRESH_DEBOUNCE);
            cx.run_until_parked();
        }
    }

    impl AgentSessionList for FakeSessionList {
        fn list_sessions(
            &self,
            request: AgentSessionListRequest,
            _cx: &mut App,
        ) -> Task<Result<AgentSessionListResponse>> {
            self.list_calls.set(self.list_calls.get() + 1);
            let start = request
                .cursor
                .and_then(|cursor| cursor.parse::<usize>().ok())
                .unwrap_or(0);
            let sessions = self.sessions.borrow();
            let end = (start + PAGE_SIZE).min(sessions.len());
            let mut response = AgentSessionListResponse::new(sessions[start..end].to_vec());
            if end < sessions.len() {
                response.next_cursor = Some(end.to_string());
            }
            Task::ready(Ok(response))
        }

        fn watch(&self, _cx: &mut App) -> Option<async_channel::Receiver<SessionListUpdate>> {
            Some(self.updates_rx.clone())
        }

        fn notifies_changes(&self) -> bool {
            self.notifies_changes
        }

        fn into_any(self: Rc<Self>) -> Rc<dyn Any> {
            self
        }
    }

    fn session(session_id: &str, title: &str, folder: &str, minute: u32) -> AgentSessionInfo {
        let mut session = AgentSessionInfo::new(session_id);
        session.title = Some(title.to_string().into());
        session.work_dirs = Some(PathList::new(&[Path::new(folder)]));
        session.updated_at = Some(timestamp(minute));
        session
    }

    fn timestamp(minute: u32) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&format!("2026-11-07T10:{minute:02}:00Z"))
            .expect("timestamp")
            .with_timezone(&Utc)
    }

    fn init_test(cx: &mut TestAppContext) -> Entity<SessionAutoImport> {
        let auto_import = cx.update(|cx| {
            let settings_store = settings::SettingsStore::test(cx);
            cx.set_global(settings_store);
            ThreadMetadataStore::init_global(cx);
            ThreadParentStore::init_global(cx);
            cx.new(SessionAutoImport::new)
        });
        // Threads saved before the stores have loaded would be lost on load, which
        // can't happen in Zed, where they load long before anything saves.
        cx.run_until_parked();
        auto_import
    }

    fn watch_as(
        agent: &str,
        auto_import: &Entity<SessionAutoImport>,
        list: &Rc<FakeSessionList>,
        cx: &mut TestAppContext,
    ) {
        auto_import.update(cx, |auto_import, cx| {
            auto_import.watch(AgentId::new(agent), None, list.clone(), cx);
        });
        cx.run_until_parked();
    }

    fn watch(
        auto_import: &Entity<SessionAutoImport>,
        list: &Rc<FakeSessionList>,
        cx: &mut TestAppContext,
    ) {
        watch_as("gorynych", auto_import, list, cx);
    }

    fn thread_of(agent: &str, session_id: &str, cx: &TestAppContext) -> Option<ThreadMetadata> {
        cx.read(|cx| {
            ThreadMetadataStore::global(cx)
                .read(cx)
                .entries()
                .find(|thread| {
                    thread.agent_id == AgentId::new(agent)
                        && thread.session_id == Some(acp::SessionId::new(session_id))
                })
                .cloned()
        })
    }

    fn thread(session_id: &str, cx: &TestAppContext) -> Option<ThreadMetadata> {
        thread_of("gorynych", session_id, cx)
    }

    fn update_store(
        cx: &mut TestAppContext,
        f: impl FnOnce(&mut ThreadMetadataStore, &mut Context<ThreadMetadataStore>),
    ) {
        cx.update(|cx| ThreadMetadataStore::global(cx).update(cx, f));
        cx.run_until_parked();
    }

    #[gpui::test]
    async fn test_imports_listed_sessions_as_unarchived_threads(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(
            true,
            vec![
                session("s1", "First", "/project", 1),
                session("s2", "Second", "/project", 2),
                session("s3", "Elsewhere", "/other/./nested", 3),
            ],
        );
        watch(&auto_import, &list, cx);

        // Three sessions span two pages.
        assert_eq!(list.list_calls.get(), 2);
        let first = thread("s1", cx).expect("s1 imported");
        assert_eq!(first.agent_id, AgentId::new("gorynych"));
        assert_eq!(first.display_title(), "First");
        assert!(!first.archived);
        assert_eq!(
            first.folder_paths(),
            &PathList::new(&[Path::new("/project")])
        );
        assert_eq!(first.updated_at, timestamp(1));
        assert!(thread("s2", cx).is_some_and(|thread| !thread.archived));
        assert_eq!(
            thread("s3", cx).map(|thread| thread.folder_paths().clone()),
            Some(PathList::new(&[Path::new("/other/nested")]))
        );
    }

    #[gpui::test]
    async fn test_refresh_imports_new_sessions_and_keeps_archived_threads_archived(
        cx: &mut TestAppContext,
    ) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);

        let first = thread("s1", cx).expect("s1 imported");
        update_store(cx, |store, cx| store.archive(first.thread_id, None, cx));

        *list.sessions.borrow_mut() = vec![
            session("s1", "Renamed by the agent", "/project", 5),
            session("s2", "Child", "/project", 4),
        ];
        list.send(SessionListUpdate::Refresh);
        list.refresh(cx);

        // Both refreshes are served by one listing.
        assert_eq!(list.list_calls.get(), 2);
        let first = thread("s1", cx).expect("s1 kept");
        assert!(first.archived);
        assert_eq!(first.display_title(), "Renamed by the agent");
        assert_eq!(first.updated_at, timestamp(5));
        let child = thread("s2", cx).expect("s2 imported");
        assert!(!child.archived);
        assert_eq!(child.display_title(), "Child");
    }

    #[gpui::test]
    async fn test_removed_sessions_drop_their_threads_even_from_the_archive(
        cx: &mut TestAppContext,
    ) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(
            true,
            vec![
                session("s1", "First", "/project", 1),
                session("s2", "Archived", "/project", 2),
                session("s3", "Third", "/project", 3),
            ],
        );
        watch(&auto_import, &list, cx);
        let archived = thread("s2", cx).expect("s2 imported");
        update_store(cx, |store, cx| store.archive(archived.thread_id, None, cx));

        list.sessions.borrow_mut().truncate(0);
        for session_id in ["s1", "s2", "unknown"] {
            list.send(SessionListUpdate::SessionRemoved {
                session_id: acp::SessionId::new(session_id),
            });
        }
        cx.run_until_parked();

        assert!(thread("s1", cx).is_none());
        assert!(thread("s2", cx).is_none());
        assert!(thread("s3", cx).is_some());
    }

    #[gpui::test]
    async fn test_complete_listings_drop_only_imported_threads(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        // A thread started in Zed for the same agent.
        let started_in_zed = session("zed", "Started in Zed", "/project", 1);
        update_store(cx, |store, cx| {
            store.save(
                ThreadMetadata {
                    thread_id: ThreadId::new(),
                    session_id: Some(acp::SessionId::new("zed")),
                    agent_id: AgentId::new("gorynych"),
                    title: started_in_zed.title.clone(),
                    title_override: None,
                    updated_at: timestamp(1),
                    created_at: None,
                    interacted_at: None,
                    worktree_paths: WorktreePaths::from_folder_paths(&PathList::new(&[Path::new(
                        "/project",
                    )])),
                    remote_connection: None,
                    archived: false,
                },
                cx,
            )
        });
        let list = FakeSessionList::new(
            false,
            vec![
                started_in_zed,
                session("gone", "Gone", "/project", 2),
                session("archived", "Archived", "/project", 3),
            ],
        );
        watch(&auto_import, &list, cx);
        let archived = thread("archived", cx).expect("imported");
        update_store(cx, |store, cx| store.archive(archived.thread_id, None, cx));

        // An agent that doesn't report changes only shows its deletions in a listing.
        list.sessions.borrow_mut().clear();
        cx.executor().advance_clock(POLL_INTERVAL);
        cx.run_until_parked();

        assert!(thread("gone", cx).is_none());
        assert!(thread("archived", cx).is_some(), "archived threads stay");
        assert!(thread("zed", cx).is_some(), "threads started in Zed stay");
    }

    #[gpui::test]
    async fn test_threads_deleted_in_zed_are_not_imported_again(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = settings::SettingsStore::test(cx);
            cx.set_global(settings_store);
            ThreadMetadataStore::init_global(cx);
        });
        let db = SessionAutoImportDb(
            db::open_test_db::<SessionAutoImportDb>("test_threads_deleted_in_zed").await,
        );
        let auto_import = cx.new(|cx| SessionAutoImport::with_db(db.clone(), cx));
        let list = FakeSessionList::new(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);

        let first = thread("s1", cx).expect("s1 imported");
        update_store(cx, |store, cx| store.delete(first.thread_id, cx));

        // The agent may list a session until it handles the deletion.
        list.refresh(cx);
        assert_eq!(list.list_calls.get(), 2);
        assert!(thread("s1", cx).is_none());

        // Neither a new connection nor a new Zed session imports it again.
        drop(list);
        cx.run_until_parked();
        let list = FakeSessionList::new(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);
        assert!(thread("s1", cx).is_none());

        drop(auto_import);
        let auto_import = cx.new(|cx| SessionAutoImport::with_db(db, cx));
        cx.run_until_parked();
        let list = FakeSessionList::new(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);
        assert_eq!(list.list_calls.get(), 1);
        assert!(thread("s1", cx).is_none());
    }

    #[gpui::test]
    async fn test_agents_sharing_a_session_id_get_a_thread_each(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let claude = FakeSessionList::new(true, vec![session("s1", "Claude's", "/project", 1)]);
        let kimi = FakeSessionList::new(true, vec![session("s1", "Kimi's", "/project", 2)]);
        watch_as("claude", &auto_import, &claude, cx);
        watch_as("kimi", &auto_import, &kimi, cx);

        assert_eq!(
            thread_of("claude", "s1", cx).map(|thread| thread.display_title()),
            Some("Claude's".into())
        );
        assert_eq!(
            thread_of("kimi", "s1", cx).map(|thread| thread.display_title()),
            Some("Kimi's".into())
        );

        // One agent removing its session leaves the other's thread alone.
        kimi.send(SessionListUpdate::SessionRemoved {
            session_id: acp::SessionId::new("s1"),
        });
        cx.run_until_parked();
        assert!(thread_of("kimi", "s1", cx).is_none());
        assert!(thread_of("claude", "s1", cx).is_some());
    }

    #[gpui::test]
    async fn test_agents_without_change_notifications_are_polled(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(false, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);
        assert_eq!(list.list_calls.get(), 1);

        list.sessions
            .borrow_mut()
            .push(session("s2", "Second", "/project", 2));
        cx.executor().advance_clock(POLL_INTERVAL);
        cx.run_until_parked();

        assert_eq!(list.list_calls.get(), 2);
        assert!(thread("s2", cx).is_some());
    }

    fn with_parent(mut session: AgentSessionInfo, parent: &str) -> AgentSessionInfo {
        session.meta = Some(acp::Meta::from_iter([(
            crate::thread_parent_store::PARENT_SESSION_ID_META_KEY.into(),
            parent.into(),
        )]));
        session
    }

    fn parent_of(agent: &str, session_id: &str, cx: &TestAppContext) -> Option<String> {
        cx.read(|cx| {
            ThreadParentStore::global(cx)
                .read(cx)
                .parent_session_id(&AgentId::new(agent), &acp::SessionId::new(session_id))
                .map(|parent| parent.0.to_string())
        })
    }

    fn parent(session_id: &str, cx: &TestAppContext) -> Option<String> {
        parent_of("gorynych", session_id, cx)
    }

    #[gpui::test]
    async fn test_listing_links_children_to_their_parents(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(
            true,
            vec![
                session("coordinator", "Coordinator", "/project", 1),
                with_parent(session("coder", "Coder", "/project", 2), "coordinator"),
            ],
        );
        watch(&auto_import, &list, cx);

        assert!(thread("coder", cx).is_some_and(|thread| !thread.archived));
        assert_eq!(parent("coder", cx).as_deref(), Some("coordinator"));
        assert_eq!(parent("coordinator", cx), None);

        // The listing is authoritative: a session that lost its parent is unlinked.
        *list.sessions.borrow_mut() = vec![
            session("coordinator", "Coordinator", "/project", 1),
            session("coder", "Coder", "/project", 2),
        ];
        list.refresh(cx);
        assert_eq!(parent("coder", cx), None);
    }

    #[gpui::test]
    async fn test_parent_links_belong_to_their_agent_and_go_with_the_thread(
        cx: &mut TestAppContext,
    ) {
        let auto_import = init_test(cx);
        // A Kimi child of a Claude coordinator names the Claude session.
        let claude = FakeSessionList::new(
            true,
            vec![session("coordinator", "Coordinator", "/project", 1)],
        );
        let kimi = FakeSessionList::new(
            true,
            vec![
                with_parent(session("coder", "Coder", "/project", 2), "coordinator"),
                with_parent(session("deleted", "Deleted", "/project", 3), "coordinator"),
            ],
        );
        watch_as("claude", &auto_import, &claude, cx);
        watch_as("kimi", &auto_import, &kimi, cx);
        assert_eq!(
            parent_of("kimi", "coder", cx).as_deref(),
            Some("coordinator")
        );
        assert_eq!(parent_of("claude", "coder", cx), None);

        // A thread deleted in Zed takes its link along, and the link doesn't come back.
        let deleted = thread_of("kimi", "deleted", cx).expect("imported");
        update_store(cx, |store, cx| store.delete(deleted.thread_id, cx));
        assert_eq!(parent_of("kimi", "deleted", cx), None);
        kimi.refresh(cx);
        assert_eq!(parent_of("kimi", "deleted", cx), None);

        kimi.send(SessionListUpdate::SessionRemoved {
            session_id: acp::SessionId::new("coder"),
        });
        cx.run_until_parked();
        assert_eq!(parent_of("kimi", "coder", cx), None);
    }

    #[gpui::test]
    async fn test_session_info_updates_link_children(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(true, vec![session("coder", "Coder", "/project", 2)]);
        watch(&auto_import, &list, cx);

        let update: agent_client_protocol::schema::v1::SessionInfoUpdate =
            serde_json::from_value(serde_json::json!({
                "title": "Coder",
                "_meta": {"gorynych/parentSessionId": "coordinator"}
            }))
            .expect("session info update");
        list.send(SessionListUpdate::SessionInfo {
            session_id: acp::SessionId::new("coder"),
            update: update.into(),
        });
        cx.run_until_parked();
        assert_eq!(parent("coder", cx).as_deref(), Some("coordinator"));

        // An update without the key keeps the link.
        let update: agent_client_protocol::schema::v1::SessionInfoUpdate =
            serde_json::from_value(serde_json::json!({"title": "Renamed"}))
                .expect("session info update");
        list.send(SessionListUpdate::SessionInfo {
            session_id: acp::SessionId::new("coder"),
            update: update.into(),
        });
        cx.run_until_parked();
        assert_eq!(parent("coder", cx).as_deref(), Some("coordinator"));
    }

    #[gpui::test]
    async fn test_watching_stops_when_the_session_list_is_dropped(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);
        assert!(auto_import.read_with(cx, |auto_import, _| {
            auto_import.is_watching(&AgentId::new("gorynych"), None)
        }));

        drop(list);
        cx.run_until_parked();

        assert!(auto_import.read_with(cx, |auto_import, _| {
            !auto_import.is_watching(&AgentId::new("gorynych"), None)
        }));
    }

    #[test]
    fn test_sessions_held_by_live_threads_are_not_imported() {
        let plan = plan_sync(
            &AgentId::new("gorynych"),
            None,
            SessionListing {
                sessions: vec![
                    session("draft", "Draft", "/project", 1),
                    session("new", "New", "/project", 2),
                ],
                complete: true,
            },
            &HashMap::default(),
            &HashMap::default(),
            &HashSet::default(),
            &HashSet::from_iter([acp::SessionId::new("draft")]),
            timestamp(3),
        );
        assert_eq!(
            plan.save
                .iter()
                .filter_map(|thread| thread.session_id.as_ref())
                .map(|session_id| session_id.0.as_ref())
                .collect::<Vec<_>>(),
            ["new"]
        );
        assert_eq!(plan.known, [(acp::SessionId::new("new"), true)]);
    }

    #[test]
    fn test_incomplete_listings_drop_nothing() {
        let thread = ThreadMetadata {
            thread_id: ThreadId::new(),
            session_id: Some(acp::SessionId::new("old")),
            agent_id: AgentId::new("gorynych"),
            title: None,
            title_override: None,
            updated_at: timestamp(1),
            created_at: None,
            interacted_at: None,
            worktree_paths: WorktreePaths::default(),
            remote_connection: None,
            archived: false,
        };
        let session_id = acp::SessionId::new("old");
        let threads = HashMap::from_iter([(&session_id, &thread)]);
        let known = HashMap::from_iter([(session_id.clone(), true)]);
        let plan = |complete| {
            plan_sync(
                &AgentId::new("gorynych"),
                None,
                SessionListing {
                    sessions: Vec::new(),
                    complete,
                },
                &threads,
                &known,
                &HashSet::default(),
                &HashSet::default(),
                timestamp(2),
            )
        };
        let incomplete = plan(false);
        assert!(incomplete.delete.is_empty() && incomplete.forgotten.is_empty());
        let complete = plan(true);
        assert_eq!(complete.delete, [thread.thread_id]);
        assert_eq!(complete.forgotten, [session_id]);
    }

    #[test]
    fn test_connection_attempts_back_off_and_give_up() {
        let start = Instant::now();
        let mut attempts = ConnectAttempts {
            count: 1,
            last_attempt: start,
        };
        assert!(!attempts.allows_attempt(start + Duration::from_secs(10)));
        assert!(attempts.allows_attempt(start + CONNECT_RETRY_DELAY));
        attempts.count = 2;
        assert!(!attempts.allows_attempt(start + CONNECT_RETRY_DELAY));
        assert!(attempts.allows_attempt(start + CONNECT_RETRY_DELAY * 2));
        attempts.count = MAX_CONNECT_ATTEMPTS;
        assert!(!attempts.allows_attempt(start + Duration::from_secs(9 * 60)));
        // A connection that lasted counts as working again.
        assert!(attempts.allows_attempt(start + STABLE_CONNECTION));
        assert_eq!(attempts.count, 0);
    }

    #[test]
    fn test_only_agents_whose_settings_changed_are_retried() {
        let settings = |args: &[&str]| CustomAgentServerSettings::Custom {
            command: project::agent_server_store::AgentServerCommand {
                path: PathBuf::from("agent"),
                args: args.iter().map(|arg| arg.to_string()).collect(),
                env: None,
            },
            default_mode: None,
            default_config_options: Default::default(),
            favorite_config_option_values: Default::default(),
            auto_import_sessions: true,
        };
        let previous = HashMap::from_iter([
            (AgentId::new("same"), settings(&["a"])),
            (AgentId::new("edited"), settings(&["a"])),
            (AgentId::new("removed"), settings(&["a"])),
        ]);
        let current = HashMap::from_iter([
            (AgentId::new("same"), settings(&["a"])),
            (AgentId::new("edited"), settings(&["b"])),
            (AgentId::new("added"), settings(&["a"])),
        ]);
        assert_eq!(
            changed_agents(&previous, &current),
            HashSet::from_iter([
                AgentId::new("edited"),
                AgentId::new("removed"),
                AgentId::new("added"),
            ])
        );
    }
}
