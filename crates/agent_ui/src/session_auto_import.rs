//! Keeps the sidebar's threads in sync with the sessions of external agents that
//! have `auto_import_sessions` enabled.
//!
//! Sessions an agent creates outside of Zed show up as threads, without going
//! through the manual import modal. The agent's session list is read when Zed
//! connects to it, whenever the agent reports a change, and on a slow timer for
//! agents that don't report changes. Sessions that an agent reports as started by
//! another session are linked to their parent, so the sidebar can nest them, and
//! sessions it reports as open in another app are marked, so the sidebar can say so.
//! A thread is as recent as its agent lists its session: loading the thread in Zed,
//! or the entries the agent replays into it, don't make it look active now.
//!
//! Agents that keep an archive of their own (the Gorynych extension advertised by
//! `gorynych/archive`) share it with Zed's: a session the agent archives or restores
//! is archived or restored here, and the user archiving or restoring its thread is
//! reported to the agent.
//!
//! In a remote project, the agents enabled in the remote machine's settings are
//! imported from that machine. An agent there is another source of sessions than
//! the agent of the same name on this machine: each keeps its own threads.

use std::{
    fmt,
    path::PathBuf,
    rc::{Rc, Weak},
    time::{Duration, Instant},
};

use acp_thread::{
    AgentSessionInfo, AgentSessionInfoUpdate, AgentSessionList, AgentSessionListRequest,
    SessionListUpdate, interactivity::interactivity_from_meta,
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
    App, AppContext as _, AsyncApp, Context, Entity, EntityId, Global, SharedString, Subscription,
    Task, WeakEntity,
};
use project::{
    AgentId,
    agent_server_store::{AllAgentServersSettings, CustomAgentServerSettings},
};
use remote::{RemoteConnectionOptions, remote_connection_identity};
use settings::SettingsStore;
use util::{ResultExt as _, path_list::PathList};

use crate::{
    Agent, ConversationView,
    agent_connection_store::{AgentConnectionEntry, AgentConnectionStore},
    open_elsewhere_store::{OpenElsewhereStore, open_elsewhere_from_meta},
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

/// Key of a session's archived flag in the `_meta` of its info, for agents whose
/// session list supports the archive. A listed session without it isn't archived;
/// a session info update carries it only when the flag changes.
const ARCHIVED_META_KEY: &str = "gorynych/archived";
/// Key in the `_meta` of a `session/list` request that includes archived sessions.
const INCLUDE_ARCHIVED_META_KEY: &str = "gorynych/includeArchived";

fn archived_from_meta(meta: Option<&acp::Meta>) -> Option<bool> {
    meta?.get(ARCHIVED_META_KEY)?.as_bool()
}

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

/// Where sessions come from: an agent, on this machine or on the remote machine
/// of a remote project. The same agent on two machines has two session lists.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionSource {
    pub agent_id: AgentId,
    /// The remote machine's persistence key, or `None` for this machine.
    remote: Option<SharedString>,
}

impl SessionSource {
    pub fn new(agent_id: AgentId, remote_connection: Option<&RemoteConnectionOptions>) -> Self {
        Self {
            agent_id,
            remote: remote_connection.map(|remote_connection| {
                remote_connection_identity(remote_connection)
                    .persistence_key()
                    .into()
            }),
        }
    }

    pub fn local(agent_id: AgentId) -> Self {
        Self::new(agent_id, None)
    }

    /// The source of the thread's session.
    pub fn of_thread(thread: &ThreadMetadata) -> Self {
        Self::new(thread.agent_id.clone(), thread.remote_connection.as_ref())
    }

    fn is_local(&self) -> bool {
        self.remote.is_none()
    }
}

impl fmt::Display for SessionSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.remote {
            Some(remote) => write!(f, "{} on {remote}", self.agent_id),
            None => write!(f, "{}", self.agent_id),
        }
    }
}

pub struct SessionAutoImport {
    watchers: Vec<SessionListWatcher>,
    /// Connections this importer requested and that haven't been established yet.
    pending_connections: Vec<(SessionSource, WeakEntity<AgentConnectionStore>)>,
    connect_attempts: HashMap<SessionSource, ConnectAttempts>,
    unsupported_sources: HashSet<SessionSource>,
    /// Each local agent's settings as last seen, so that editing one agent's entry
    /// retries only that agent.
    agent_settings: HashMap<AgentId, CustomAgentServerSettings>,
    stores: HashMap<EntityId, (WeakEntity<AgentConnectionStore>, [Subscription; 2])>,
    conversation_views: HashMap<EntityId, WeakEntity<ConversationView>>,
    known_sessions: KnownSessions,
    /// The archived state last requested from an agent for a session, until the
    /// agent reports the session's flag again.
    archive_requests: HashMap<(SessionSource, acp::SessionId), bool>,
    /// Syncs the stores when a source's backoff expires: nothing else may.
    retry_timers: HashMap<SessionSource, Task<()>>,
    next_watcher_id: usize,
    _metadata_subscription: Option<Subscription>,
}

struct SessionListWatcher {
    id: usize,
    source: SessionSource,
    session_list: Weak<dyn AgentSessionList>,
    task: Task<()>,
}

#[derive(Clone, Copy, Debug)]
struct ConnectAttempts {
    count: u32,
    last_attempt: Instant,
}

impl ConnectAttempts {
    /// When the next attempt may be made, or `None` while attempts are given up.
    fn next_attempt(&mut self, now: Instant) -> Option<Instant> {
        if now.saturating_duration_since(self.last_attempt) >= STABLE_CONNECTION {
            self.count = 0;
            return Some(now);
        }
        (self.count < MAX_CONNECT_ATTEMPTS).then(|| {
            self.last_attempt + CONNECT_RETRY_DELAY * 2u32.pow(self.count.saturating_sub(1))
        })
    }
}

/// The sessions of each source that had a thread in a listing.
///
/// They are persisted, so that a session whose thread was deleted in Zed is not
/// imported again, so that only threads created here are dropped when their
/// sessions disappear from a listing, and so that archiving a thread while its
/// agent is away still reaches the agent later.
struct KnownSessions {
    sessions: HashMap<SessionSource, HashMap<acp::SessionId, KnownSession>>,
    loaded: Shared<Task<()>>,
    /// Writes go through one queue so that they apply in the order they are made.
    pending_writes: async_channel::Sender<KnownSessionWrite>,
    _write_task: Task<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct KnownSession {
    /// Whether this importer created the session's thread.
    imported: bool,
    /// The session's archived flag as the agent last reported it, for agents that
    /// keep an archive.
    archived: Option<bool>,
}

/// A source as its known sessions are stored: the agent, and the remote
/// machine's persistence key, empty for this machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredSource {
    pub(crate) agent_id: String,
    pub(crate) remote: String,
}

impl From<&SessionSource> for StoredSource {
    fn from(source: &SessionSource) -> Self {
        Self {
            agent_id: source.agent_id.to_string(),
            remote: source.remote.as_deref().unwrap_or_default().to_string(),
        }
    }
}

impl From<StoredSource> for SessionSource {
    fn from(stored: StoredSource) -> Self {
        Self {
            agent_id: AgentId::new(stored.agent_id),
            remote: (!stored.remote.is_empty()).then(|| stored.remote.into()),
        }
    }
}

enum KnownSessionWrite {
    Save {
        source: StoredSource,
        session_id: String,
        session: KnownSession,
    },
    Delete {
        source: StoredSource,
        session_id: String,
    },
}

impl KnownSessions {
    fn of(&self, source: &SessionSource) -> Option<&HashMap<acp::SessionId, KnownSession>> {
        self.sessions.get(source)
    }

    fn get(&self, source: &SessionSource, session_id: &acp::SessionId) -> Option<KnownSession> {
        self.of(source)?.get(session_id).copied()
    }

    fn insert(
        &mut self,
        source: &SessionSource,
        session_id: acp::SessionId,
        session: KnownSession,
    ) {
        let previous = self
            .sessions
            .entry(source.clone())
            .or_default()
            .insert(session_id.clone(), session);
        if previous != Some(session) {
            self.pending_writes
                .try_send(KnownSessionWrite::Save {
                    source: source.into(),
                    session_id: session_id.0.to_string(),
                    session,
                })
                .log_err();
        }
    }

    fn remove(&mut self, source: &SessionSource, session_id: &acp::SessionId) {
        let removed = self
            .sessions
            .get_mut(source)
            .and_then(|sessions| sessions.remove(session_id));
        if removed.is_some() {
            self.pending_writes
                .try_send(KnownSessionWrite::Delete {
                    source: source.into(),
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

        cx.observe_new::<ConversationView>(move |_view, _window, cx| {
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
        })
        .detach();

        cx.observe_global::<SettingsStore>(|this, cx| {
            this.forget_attempts_of_changed_agents(cx);
            this.sync_all_stores(cx);
        })
        .detach();

        let metadata_subscription = ThreadMetadataStore::try_global(cx).map(|store| {
            cx.observe(&store, |this, _store, cx| {
                this.send_local_archive_changes(cx)
            })
        });

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
                    for (source, session_id, imported, archived) in rows {
                        // Sessions recorded while loading are newer than the stored ones.
                        this.known_sessions
                            .sessions
                            .entry(SessionSource::from(source))
                            .or_default()
                            .entry(acp::SessionId::new(session_id))
                            .or_insert(KnownSession { imported, archived });
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
            unsupported_sources: HashSet::default(),
            agent_settings: HashMap::default(),
            stores: HashMap::default(),
            conversation_views: HashMap::default(),
            known_sessions: KnownSessions {
                sessions: HashMap::default(),
                loaded,
                pending_writes,
                _write_task: write_task,
            },
            archive_requests: HashMap::default(),
            retry_timers: HashMap::default(),
            next_watcher_id: 0,
            _metadata_subscription: metadata_subscription,
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
            let source = SessionSource::local(agent_id);
            self.connect_attempts.remove(&source);
            self.unsupported_sources.remove(&source);
        }
        self.agent_settings = agent_settings;
    }

    /// The sources whose sessions the store's project imports: the enabled agents
    /// of the machine its agents run on. Projects shared over collab have none.
    fn store_sources(
        store: &Entity<AgentConnectionStore>,
        cx: &App,
    ) -> (Option<RemoteConnectionOptions>, Vec<SessionSource>) {
        let project = store.read(cx).project().read(cx);
        let remote_connection = project.remote_connection_options(cx);
        if !project.is_local() && remote_connection.is_none() {
            return (None, Vec::new());
        }
        let sources = project
            .agent_server_store()
            .read(cx)
            .auto_import_agent_ids(cx)
            .into_iter()
            .map(|agent_id| SessionSource::new(agent_id, remote_connection.as_ref()))
            .collect();
        (remote_connection, sources)
    }

    /// Watches the session lists of the agents this store is connected to, and
    /// connects to enabled agents nobody is connected to yet.
    fn sync_store(&mut self, store: &Entity<AgentConnectionStore>, cx: &mut Context<Self>) {
        let (remote_connection, sources) = Self::store_sources(store, cx);
        let mut enabled = self
            .stores
            .values()
            .filter_map(|(other, _)| other.upgrade())
            .filter(|other| other != store)
            .flat_map(|other| Self::store_sources(&other, cx).1)
            .collect::<HashSet<_>>();
        enabled.extend(sources.iter().cloned());

        let mut unwatched = Vec::new();
        self.watchers.retain(|watcher| {
            let keep = watcher.session_list.strong_count() > 0 && enabled.contains(&watcher.source);
            if !keep {
                unwatched.push(watcher.source.clone());
            }
            keep
        });
        unwatched.retain(|source| {
            !self
                .watchers
                .iter()
                .any(|watcher| &watcher.source == source)
        });
        for source in unwatched {
            forget_open_elsewhere(&source, cx);
        }
        self.pending_connections
            .retain(|(source, store)| store.upgrade().is_some() && enabled.contains(source));
        // A remote agent that gets enabled again, or a remote machine that gets
        // opened again, is retried from scratch, like a local agent whose settings
        // change.
        self.connect_attempts
            .retain(|source, _| source.is_local() || enabled.contains(source));
        self.unsupported_sources
            .retain(|source| source.is_local() || enabled.contains(source));
        self.retry_timers
            .retain(|source, _| enabled.contains(source));

        for source in sources {
            let agent = Agent::from(source.agent_id.clone());
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
                    pending == &source && pending_store.entity_id() == store.entity_id()
                });
            if requested_here && !matches!(connection, Some(None)) {
                self.pending_connections.retain(|(pending, pending_store)| {
                    pending != &source || pending_store.entity_id() != store.entity_id()
                });
            }

            match connection {
                Some(Some(connection)) => {
                    if let Some(session_list) = connection.session_list(cx) {
                        self.watch(
                            source.agent_id.clone(),
                            remote_connection.clone(),
                            session_list,
                            cx,
                        );
                    } else if self.unsupported_sources.insert(source.clone()) {
                        log::warn!(
                            "Not importing sessions of {source}: it doesn't support listing sessions"
                        );
                    }
                }
                Some(None) => {}
                None => {
                    if requested_here {
                        // The store drops entries whose connection failed.
                        log::warn!("Connecting to {source} to import its sessions failed");
                    }
                    if self.is_watching(&source)
                        || self
                            .pending_connections
                            .iter()
                            .any(|(pending, _)| pending == &source)
                    {
                        continue;
                    }
                    let now = cx.background_executor().now();
                    let next_attempt = match self.connect_attempts.get_mut(&source) {
                        Some(attempts) => attempts.next_attempt(now),
                        None => Some(now),
                    };
                    match next_attempt {
                        Some(next_attempt) if next_attempt <= now => {
                            self.connect(store, source, now, cx)
                        }
                        Some(next_attempt) => self.retry_at(source, next_attempt - now, cx),
                        // Given up until the agent's settings change or a while passes.
                        None => {}
                    }
                }
            }
        }
    }

    /// Syncs the stores once the source's backoff expires, so that a connection
    /// that failed is retried without waiting for something else to change.
    fn retry_at(&mut self, source: SessionSource, delay: Duration, cx: &mut Context<Self>) {
        let task = cx.spawn({
            let source = source.clone();
            async move |this, cx| {
                cx.background_executor().timer(delay).await;
                this.update(cx, |this, cx| {
                    this.retry_timers.remove(&source);
                    this.sync_all_stores(cx);
                })
                .ok();
            }
        });
        self.retry_timers.insert(source, task);
    }

    fn connect(
        &mut self,
        store: &Entity<AgentConnectionStore>,
        source: SessionSource,
        now: Instant,
        cx: &mut Context<Self>,
    ) {
        let attempts = self
            .connect_attempts
            .entry(source.clone())
            .or_insert(ConnectAttempts {
                count: 0,
                last_attempt: now,
            });
        attempts.count += 1;
        attempts.last_attempt = now;
        if attempts.count == MAX_CONNECT_ATTEMPTS {
            log::warn!(
                "Connecting to {source} to import its sessions for the last time before giving up"
            );
        }

        let agent = Agent::from(source.agent_id.clone());
        let server = agent.server(<dyn Fs>::global(cx), ThreadStore::global(cx));
        self.pending_connections.push((source, store.downgrade()));
        store.update(cx, |store, cx| {
            store.request_connection(agent, server, cx);
        });
    }

    fn is_watching(&self, source: &SessionSource) -> bool {
        self.watchers
            .iter()
            .any(|watcher| &watcher.source == source && watcher.session_list.strong_count() > 0)
    }

    pub fn watch(
        &mut self,
        agent_id: AgentId,
        remote_connection: Option<RemoteConnectionOptions>,
        session_list: Rc<dyn AgentSessionList>,
        cx: &mut Context<Self>,
    ) {
        let source = SessionSource::new(agent_id, remote_connection.as_ref());
        if self.is_watching(&source) {
            return;
        }
        self.watchers.retain(|watcher| watcher.source != source);
        self.retry_timers.remove(&source);
        let id = self.next_watcher_id;
        self.next_watcher_id += 1;

        // A list whose updates have ended is polled, so that a watcher that stops
        // when they end isn't started again and again on the same list.
        let updates = session_list
            .watch(cx)
            .filter(|updates| !updates.is_closed());
        let poll_interval =
            (updates.is_none() || !session_list.notifies_changes()).then_some(POLL_INTERVAL);
        let task = cx.spawn({
            let source = source.clone();
            let session_list = Rc::downgrade(&session_list);
            async move |this, cx| {
                watch_session_list(
                    this,
                    id,
                    source,
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
            id,
            source,
            session_list: Rc::downgrade(&session_list),
            task,
        });
    }

    /// A watcher stops when its session list goes, which may be after the
    /// windows that need its agent last synced, as when one of two windows on a
    /// remote machine closes: they sync again, and the window left connects.
    fn stop_watcher(&mut self, id: usize, cx: &mut Context<Self>) {
        if let Some(index) = self.watchers.iter().position(|watcher| watcher.id == id) {
            let watcher = self.watchers.remove(index);
            // The task is finishing: dropping it would cancel it while it runs.
            watcher.task.detach();
            if !self
                .watchers
                .iter()
                .any(|other| other.source == watcher.source)
            {
                forget_open_elsewhere(&watcher.source, cx);
            }
        }
        self.sync_all_stores(cx);
    }

    fn open_thread_ids(&self, cx: &App) -> HashSet<ThreadId> {
        self.conversation_views
            .values()
            .filter_map(|view| view.upgrade())
            .map(|view| view.read(cx).thread_id)
            .collect()
    }

    /// Sessions of the source that a live thread holds, including drafts whose
    /// metadata doesn't name their session yet.
    fn live_sessions(&self, source: &SessionSource, cx: &App) -> HashSet<acp::SessionId> {
        self.conversation_views
            .values()
            .filter_map(|view| view.upgrade())
            .filter_map(|view| view.read(cx).root_thread(cx))
            .filter_map(|thread| {
                let thread = thread.read(cx);
                let thread_source = SessionSource::new(
                    thread.connection().agent_id(),
                    thread
                        .project()
                        .read(cx)
                        .remote_connection_options(cx)
                        .as_ref(),
                );
                (&thread_source == source).then(|| thread.session_id().clone())
            })
            .collect()
    }

    fn sync_sessions(
        &mut self,
        source: &SessionSource,
        remote_connection: Option<&RemoteConnectionOptions>,
        session_list: &Rc<dyn AgentSessionList>,
        listing: SessionListing,
        cx: &mut Context<Self>,
    ) {
        if listing.archive {
            // The listing reports the flags of these sessions, so earlier requests
            // either took effect or are made again.
            let listed = listing
                .sessions
                .iter()
                .map(|session| &session.session_id)
                .collect::<HashSet<_>>();
            self.archive_requests
                .retain(|(requested_source, session_id), _| {
                    requested_source != source || !listed.contains(session_id)
                });
        }
        if let Some(open_elsewhere_store) = OpenElsewhereStore::try_global(cx) {
            let holders = listing
                .sessions
                .iter()
                .map(|session| {
                    (
                        session.session_id.clone(),
                        open_elsewhere_from_meta(session.meta.as_ref()),
                    )
                })
                .collect::<Vec<_>>();
            open_elsewhere_store.update(cx, |open_elsewhere_store, cx| {
                open_elsewhere_store.apply_listing(source, holders, listing.complete, cx);
            });
        }
        let open_thread_ids = self.open_thread_ids(cx);
        let live_sessions = self.live_sessions(source, cx);
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
        let threads = agent_threads(store.read(cx), source);
        let mut sessions_with_threads = threads
            .keys()
            .map(|session_id| (*session_id).clone())
            .collect::<HashSet<_>>();
        let no_known_sessions = HashMap::default();
        let plan = plan_sync(
            &source.agent_id,
            remote_connection,
            listing,
            &threads,
            self.known_sessions.of(source).unwrap_or(&no_known_sessions),
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
                            source.clone(),
                            session_id,
                            parent_session_id,
                            cx,
                        );
                    }
                }
            });
        }

        // Recorded first, so that the archive changes below don't look like the
        // user's own.
        for (session_id, session) in plan.known {
            self.known_sessions.insert(source, session_id, session);
        }
        for session_id in &plan.forgotten {
            self.known_sessions.remove(source, session_id);
        }
        store.update(cx, |store, cx| {
            for (thread_id, from_agent) in plan.times_from_agent {
                store.set_time_from_agent(thread_id, from_agent);
            }
            if !plan.save.is_empty() {
                store.save_all(plan.save, cx);
            }
        });
        for (thread_id, archived) in plan.archive {
            set_thread_archived(thread_id, archived, cx);
        }
        for (session_id, archived) in plan.send {
            self.request_archive(session_list, source, session_id, archived, cx);
        }
        for thread_id in plan.delete {
            delete_thread(thread_id, cx);
        }
    }

    fn apply_session_info(
        &mut self,
        source: &SessionSource,
        session_list: &Rc<dyn AgentSessionList>,
        session_id: acp::SessionId,
        update: AgentSessionInfoUpdate,
        cx: &mut Context<Self>,
    ) {
        let mut info = AgentSessionInfo::new(session_id.clone());
        info.apply_update(update);
        if session_list.supports_archive()
            && let Some(archived) = archived_from_meta(info.meta.as_ref())
        {
            self.apply_reported_archive(source, session_list, &session_id, archived, cx);
        }

        // A session's interactivity, when the agent reports it, keeps the mark of
        // another app holding the session in step with it: a writable session is
        // free, and a read-only one is held by the app the agent names.
        if let Some(interactivity) = interactivity_from_meta(info.meta.as_ref()) {
            let holder = open_elsewhere_from_meta(info.meta.as_ref())
                .filter(|_| interactivity.is_read_only());
            if (holder.is_some() || !interactivity.is_read_only())
                && let Some(store) = OpenElsewhereStore::try_global(cx)
            {
                store.update(cx, |store, cx| {
                    store.set_holder(source, session_id.clone(), holder, cx)
                });
            }
        }

        // An update that doesn't name a parent leaves the link alone: agents
        // report the link in full only in the session list.
        let Some(parent_session_id) = parent_session_id_from_meta(info.meta.as_ref()) else {
            return;
        };
        if let Some(parent_store) = ThreadParentStore::try_global(cx) {
            parent_store.update(cx, |parent_store, cx| {
                parent_store.set_parent_session_id(
                    source.clone(),
                    session_id,
                    Some(parent_session_id),
                    cx,
                );
            });
        }
    }

    /// Reconciles a thread with the archived flag its agent reported for the
    /// session outside a listing.
    fn apply_reported_archive(
        &mut self,
        source: &SessionSource,
        session_list: &Rc<dyn AgentSessionList>,
        session_id: &acp::SessionId,
        reported: bool,
        cx: &mut Context<Self>,
    ) {
        self.archive_requests
            .remove(&(source.clone(), session_id.clone()));
        let Some((thread_id, local)) =
            agent_threads(ThreadMetadataStore::global(cx).read(cx), source)
                .get(session_id)
                .filter(|thread| syncs_archive(thread))
                .map(|thread| (thread.thread_id, thread.archived))
        else {
            return;
        };
        let mut known = self
            .known_sessions
            .get(source, session_id)
            .unwrap_or(KnownSession {
                imported: false,
                archived: None,
            });
        let sync = reconcile_archive(local, known.archived, reported);
        known.archived = Some(reported);
        self.known_sessions
            .insert(source, session_id.clone(), known);
        match sync {
            ArchiveSync::InSync => {}
            ArchiveSync::Apply(archived) => set_thread_archived(thread_id, archived, cx),
            ArchiveSync::Send(archived) => {
                self.request_archive(session_list, source, session_id.clone(), archived, cx)
            }
        }
    }

    /// Tells agents about threads the user archived or restored since the agent
    /// last reported their sessions' flags.
    fn send_local_archive_changes(&mut self, cx: &mut Context<Self>) {
        let session_lists = self
            .watchers
            .iter()
            .filter_map(|watcher| {
                let session_list = watcher.session_list.upgrade()?;
                session_list
                    .supports_archive()
                    .then(|| (watcher.source.clone(), session_list))
            })
            .collect::<Vec<_>>();
        if session_lists.is_empty() {
            return;
        }

        let mut requests = Vec::new();
        let store = ThreadMetadataStore::global(cx).read(cx);
        for (source, session_list) in &session_lists {
            for (session_id, thread) in agent_threads(store, source) {
                if !syncs_archive(thread) {
                    continue;
                }
                let Some(reported) = self
                    .known_sessions
                    .get(source, session_id)
                    .and_then(|known| known.archived)
                else {
                    continue;
                };
                let key = (source.clone(), session_id.clone());
                if thread.archived == reported {
                    self.archive_requests.remove(&key);
                } else if self.archive_requests.get(&key) != Some(&thread.archived) {
                    requests.push((session_list.clone(), key, thread.archived));
                }
            }
        }
        for (session_list, (source, session_id), archived) in requests {
            self.request_archive(&session_list, &source, session_id, archived, cx);
        }
    }

    /// Asks the agent to archive or restore a session, unless that is what was
    /// last asked. A failure is only logged: the thread keeps its state in Zed.
    fn request_archive(
        &mut self,
        session_list: &Rc<dyn AgentSessionList>,
        source: &SessionSource,
        session_id: acp::SessionId,
        archived: bool,
        cx: &mut Context<Self>,
    ) {
        let key = (source.clone(), session_id.clone());
        if self.archive_requests.insert(key, archived) == Some(archived) {
            return;
        }
        let request = session_list.set_session_archived(&session_id, archived, cx);
        let source = source.clone();
        cx.spawn(async move |_, _| {
            if let Err(error) = request.await {
                let action = if archived { "archive" } else { "restore" };
                log::warn!("Failed to {action} session {session_id} of {source}: {error:#}");
            }
        })
        .detach();
    }

    /// The agent no longer has the session, so its thread could not be loaded
    /// anymore: it goes even from the archive, unless a view of it is open.
    fn remove_session(
        &mut self,
        source: &SessionSource,
        session_id: &acp::SessionId,
        cx: &mut Context<Self>,
    ) {
        self.known_sessions.remove(source, session_id);
        let store = ThreadMetadataStore::global(cx);
        let Some(thread_id) = agent_threads(store.read(cx), source)
            .get(session_id)
            .map(|thread| thread.thread_id)
        else {
            return;
        };
        if self.open_thread_ids(cx).contains(&thread_id) {
            // The agent no longer lists the session: Zed times its thread.
            store.update(cx, |store, _| store.set_time_from_agent(thread_id, false));
        } else {
            delete_thread(thread_id, cx);
        }
    }
}

/// Archives or restores a thread as the archive view does, leaving its
/// worktrees alone.
fn set_thread_archived(thread_id: ThreadId, archived: bool, cx: &mut App) {
    ThreadMetadataStore::global(cx).update(cx, |store, cx| {
        if archived {
            store.archive(thread_id, None, cx);
        } else {
            store.unarchive(thread_id, cx);
        }
    });
}

/// Threads without a folder are archived by Zed so that they don't get lost,
/// not by the user, so their state isn't shared with the agent.
fn syncs_archive(thread: &ThreadMetadata) -> bool {
    !thread.worktree_paths.is_empty()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArchiveSync {
    InSync,
    /// The agent changed the flag: the thread follows it.
    Apply(bool),
    /// The user changed the thread: the agent is told.
    Send(bool),
}

/// Reconciles a thread's archived state with the flag the agent reports for its
/// session. A flag that changed since the agent last reported it wins; otherwise
/// the thread was archived or restored in Zed. Without an earlier report, an
/// archive on either side wins.
fn reconcile_archive(local: bool, last_reported: Option<bool>, reported: bool) -> ArchiveSync {
    if local == reported {
        return ArchiveSync::InSync;
    }
    match last_reported {
        Some(last_reported) if last_reported == reported => ArchiveSync::Send(local),
        None if local => ArchiveSync::Send(local),
        _ => ArchiveSync::Apply(reported),
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

/// The source's threads by session. The store indexes threads by session alone,
/// but two agents, or one agent on two machines, may use the same session id.
fn agent_threads<'a>(
    store: &'a ThreadMetadataStore,
    source: &SessionSource,
) -> HashMap<&'a acp::SessionId, &'a ThreadMetadata> {
    store
        .entries()
        .filter(|thread| {
            thread.agent_id == source.agent_id
                && match (&source.remote, &thread.remote_connection) {
                    (None, None) => true,
                    (Some(remote), Some(remote_connection)) => {
                        remote_connection_identity(remote_connection).persistence_key()
                            == remote.as_ref()
                    }
                    _ => false,
                }
        })
        .filter_map(|thread| Some((thread.session_id.as_ref()?, thread)))
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
    /// Whether the agent keeps an archive, so that the listing includes archived
    /// sessions and flags them.
    archive: bool,
}

#[derive(Default)]
struct SyncPlan {
    save: Vec<ThreadMetadata>,
    delete: Vec<ThreadId>,
    /// Sessions that had a thread in this listing and whose record changes.
    known: Vec<(acp::SessionId, KnownSession)>,
    forgotten: Vec<acp::SessionId>,
    /// Threads to archive or restore, as their agent did.
    archive: Vec<(ThreadId, bool)>,
    /// Sessions to archive or restore in the agent, as the user did.
    send: Vec<(acp::SessionId, bool)>,
    /// Threads whose agent reports, or no longer reports, when their sessions
    /// were last active.
    times_from_agent: Vec<(ThreadId, bool)>,
}

/// Plans how a listing changes `agent_id`'s threads.
///
/// New sessions become threads, and known threads take the agent's title and
/// activity time. The agent's time wins over a newer one of Zed's: Zed stamps a
/// thread when it loads it and when the agent replays entries into it, which is
/// no activity of the session. A known session without a thread was deleted in Zed and
/// is skipped, and so is a session that a live thread holds before its metadata
/// names it. When the listing is complete, a session missing from it is
/// forgotten, and its thread is deleted if this importer created it, unless the
/// thread is archived or open. For agents that keep an archive, new archived
/// sessions are imported into the archive, and known threads are reconciled
/// with their sessions' flags.
fn plan_sync(
    agent_id: &AgentId,
    remote_connection: Option<&RemoteConnectionOptions>,
    listing: SessionListing,
    threads: &HashMap<&acp::SessionId, &ThreadMetadata>,
    known_sessions: &HashMap<acp::SessionId, KnownSession>,
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
        let reported_archived = listing
            .archive
            .then(|| archived_from_meta(session.meta.as_ref()).unwrap_or(false));

        if let Some(existing) = threads.get(&session.session_id) {
            let known = known_sessions.get(&session.session_id).copied();
            let mut record = known.unwrap_or(KnownSession {
                imported: false,
                archived: None,
            });
            if let Some(reported) = reported_archived
                && syncs_archive(existing)
            {
                match reconcile_archive(existing.archived, record.archived, reported) {
                    ArchiveSync::InSync => {}
                    ArchiveSync::Apply(archived) => {
                        plan.archive.push((existing.thread_id, archived))
                    }
                    ArchiveSync::Send(archived) => {
                        plan.send.push((session.session_id.clone(), archived))
                    }
                }
                record.archived = Some(reported);
            }
            if known != Some(record) {
                plan.known.push((session.session_id.clone(), record));
            }
            plan.times_from_agent
                .push((existing.thread_id, session.updated_at.is_some()));
            let mut updated = (*existing).clone();
            if let Some(updated_at) = session.updated_at {
                updated.updated_at = updated_at;
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
        plan.known.push((
            session.session_id.clone(),
            KnownSession {
                imported: true,
                archived: reported_archived,
            },
        ));
        let thread_id = ThreadId::new();
        plan.times_from_agent
            .push((thread_id, session.updated_at.is_some()));
        plan.save.push(ThreadMetadata {
            thread_id,
            session_id: Some(session.session_id),
            agent_id: agent_id.clone(),
            title: session.title,
            title_override: None,
            updated_at: session.updated_at.unwrap_or(now),
            created_at: session.created_at,
            interacted_at: None,
            // Like threads started in an empty window, threads without a folder
            // would never show in the sidebar, so they go to the archive.
            archived: worktree_paths.is_empty() || reported_archived == Some(true),
            worktree_paths,
            remote_connection: remote_connection.cloned(),
        });
    }

    if listing.complete {
        for (session_id, known) in known_sessions {
            if listed.contains(session_id) {
                continue;
            }
            plan.forgotten.push(session_id.clone());
            let Some(thread) = threads.get(session_id) else {
                continue;
            };
            if known.imported && !thread.archived && !open_thread_ids.contains(&thread.thread_id) {
                plan.delete.push(thread.thread_id);
            } else {
                // The agent no longer lists the session: Zed times its thread.
                plan.times_from_agent.push((thread.thread_id, false));
            }
        }
    }
    plan
}

async fn watch_session_list(
    this: WeakEntity<SessionAutoImport>,
    watcher_id: usize,
    source: SessionSource,
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
            match list_all_sessions(list.clone(), cx).await {
                Ok(listing) => {
                    let synced = this.update(cx, |this, cx| {
                        this.sync_sessions(&source, remote_connection.as_ref(), &list, listing, cx)
                    });
                    if synced.is_err() {
                        break;
                    }
                }
                Err(error) => log::warn!("Failed to list the sessions of {source}: {error:#}"),
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
                    this.update(cx, |this, cx| this.remove_session(&source, &session_id, cx))
                        .ok();
                }
                SessionListUpdate::SessionInfo { session_id, update } => {
                    let Some(list) = session_list.upgrade() else {
                        continue;
                    };
                    this.update(cx, |this, cx| {
                        this.apply_session_info(&source, &list, session_id, update, cx)
                    })
                    .ok();
                }
            }
        }
    }
    if this
        .update(cx, |this, cx| this.stop_watcher(watcher_id, cx))
        .is_err()
    {
        cx.update(|cx| forget_open_elsewhere(&source, cx));
    }
}

/// Without a listing to keep them up to date, the marks of a source's sessions
/// open in other apps would go stale.
fn forget_open_elsewhere(source: &SessionSource, cx: &mut App) {
    if let Some(store) = OpenElsewhereStore::try_global(cx) {
        store.update(cx, |store, cx| store.forget_source(source, cx));
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
    // Without archived sessions, a session archived elsewhere would look gone.
    let archive = session_list.supports_archive();
    let meta =
        archive.then(|| acp::Meta::from_iter([(INCLUDE_ARCHIVED_META_KEY.into(), true.into())]));
    let mut sessions = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let request = AgentSessionListRequest {
            cursor: cursor.clone(),
            meta: meta.clone(),
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
                    archive,
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
                    archive,
                });
            }
        }
    }
}

struct SessionAutoImportDb(ThreadSafeConnection);

impl Domain for SessionAutoImportDb {
    const NAME: &str = stringify!(SessionAutoImportDb);

    const MIGRATIONS: &[&str] = &[
        sql!(
            CREATE TABLE IF NOT EXISTS auto_import_known_sessions(
                agent_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                imported INTEGER NOT NULL,
                PRIMARY KEY (agent_id, session_id)
            ) STRICT;
        ),
        sql!(ALTER TABLE auto_import_known_sessions ADD COLUMN archived INTEGER;),
        // Sessions are known per source: `remote` is the remote machine's
        // persistence key, empty for this machine. The table of earlier builds
        // stays as they expect it and keeps this machine's sessions, so that such
        // a build, run again on this database, takes no remote session for one of
        // its own.
        sql!(
            CREATE TABLE auto_import_known_sessions_by_source(
                agent_id TEXT NOT NULL,
                remote TEXT NOT NULL DEFAULT "",
                session_id TEXT NOT NULL,
                imported INTEGER NOT NULL,
                archived INTEGER,
                PRIMARY KEY (agent_id, remote, session_id)
            ) STRICT;

            INSERT INTO auto_import_known_sessions_by_source(agent_id, session_id, imported, archived)
            SELECT agent_id, session_id, imported, archived
            FROM auto_import_known_sessions;
        ),
    ];
}

db::static_connection!(SessionAutoImportDb, []);

impl SessionAutoImportDb {
    fn list(&self) -> anyhow::Result<Vec<(StoredSource, String, bool, Option<bool>)>> {
        let rows = self.select::<(String, String, String, bool, Option<bool>)>(
            "SELECT agent_id, remote, session_id, imported, archived \
             FROM auto_import_known_sessions_by_source",
        )?()?;
        Ok(rows
            .into_iter()
            .map(|(agent_id, remote, session_id, imported, archived)| {
                (
                    StoredSource { agent_id, remote },
                    session_id,
                    imported,
                    archived,
                )
            })
            .collect())
    }

    async fn apply(&self, write: KnownSessionWrite) -> anyhow::Result<()> {
        self.write(move |conn| match write {
            KnownSessionWrite::Save {
                source,
                session_id,
                session,
            } => {
                let mut statement = Statement::prepare(
                    conn,
                    "INSERT INTO auto_import_known_sessions_by_source(agent_id, remote, session_id, imported, archived) \
                     VALUES (?1, ?2, ?3, ?4, ?5) \
                     ON CONFLICT(agent_id, remote, session_id) DO UPDATE SET \
                     imported = excluded.imported, archived = excluded.archived",
                )?;
                let mut index = statement.bind(&source.agent_id, 1)?;
                index = statement.bind(&source.remote, index)?;
                index = statement.bind(&session_id, index)?;
                index = statement.bind(&session.imported, index)?;
                statement.bind(&session.archived, index)?;
                statement.exec()?;
                if !source.remote.is_empty() {
                    return Ok(());
                }
                // Earlier builds read this machine's sessions from their own table.
                let mut statement = Statement::prepare(
                    conn,
                    "INSERT INTO auto_import_known_sessions(agent_id, session_id, imported, archived) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(agent_id, session_id) DO UPDATE SET \
                     imported = excluded.imported, archived = excluded.archived",
                )?;
                let mut index = statement.bind(&source.agent_id, 1)?;
                index = statement.bind(&session_id, index)?;
                index = statement.bind(&session.imported, index)?;
                statement.bind(&session.archived, index)?;
                statement.exec()
            }
            KnownSessionWrite::Delete { source, session_id } => {
                let mut statement = Statement::prepare(
                    conn,
                    "DELETE FROM auto_import_known_sessions_by_source \
                     WHERE agent_id = ?1 AND remote = ?2 AND session_id = ?3",
                )?;
                let mut index = statement.bind(&source.agent_id, 1)?;
                index = statement.bind(&source.remote, index)?;
                statement.bind(&session_id, index)?;
                statement.exec()?;
                if !source.remote.is_empty() {
                    return Ok(());
                }
                let mut statement = Statement::prepare(
                    conn,
                    "DELETE FROM auto_import_known_sessions WHERE agent_id = ?1 AND session_id = ?2",
                )?;
                let index = statement.bind(&source.agent_id, 1)?;
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
        supports_archive: bool,
        fail_archive_requests: Cell<bool>,
        list_calls: Cell<usize>,
        listing_metas: RefCell<Vec<Option<acp::Meta>>>,
        archive_requests: RefCell<Vec<(String, bool)>>,
        updates_tx: async_channel::Sender<SessionListUpdate>,
        updates_rx: async_channel::Receiver<SessionListUpdate>,
    }

    impl FakeSessionList {
        fn new(notifies_changes: bool, sessions: Vec<AgentSessionInfo>) -> Rc<Self> {
            Self::build(notifies_changes, false, sessions)
        }

        /// An agent that keeps an archive and, like Gorynych, lists archived
        /// sessions only on request.
        fn with_archive(notifies_changes: bool, sessions: Vec<AgentSessionInfo>) -> Rc<Self> {
            Self::build(notifies_changes, true, sessions)
        }

        fn build(
            notifies_changes: bool,
            supports_archive: bool,
            sessions: Vec<AgentSessionInfo>,
        ) -> Rc<Self> {
            let (updates_tx, updates_rx) = async_channel::unbounded();
            Rc::new(Self {
                sessions: RefCell::new(sessions),
                notifies_changes,
                supports_archive,
                fail_archive_requests: Cell::new(false),
                list_calls: Cell::new(0),
                listing_metas: RefCell::default(),
                archive_requests: RefCell::default(),
                updates_tx,
                updates_rx,
            })
        }

        fn requests(&self) -> Vec<(String, bool)> {
            self.archive_requests.borrow().clone()
        }

        fn set_archived(&self, session_id: &str, archived: bool) {
            for session in self.sessions.borrow_mut().iter_mut() {
                if session.session_id.0.as_ref() == session_id {
                    set_archived_meta(session, archived);
                }
            }
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
            let include_archived = request
                .meta
                .as_ref()
                .and_then(|meta| meta.get(INCLUDE_ARCHIVED_META_KEY))
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            self.listing_metas.borrow_mut().push(request.meta);
            let start = request
                .cursor
                .and_then(|cursor| cursor.parse::<usize>().ok())
                .unwrap_or(0);
            let sessions = self
                .sessions
                .borrow()
                .iter()
                .filter(|session| {
                    !self.supports_archive
                        || include_archived
                        || archived_from_meta(session.meta.as_ref()) != Some(true)
                })
                .cloned()
                .collect::<Vec<_>>();
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

        fn supports_archive(&self) -> bool {
            self.supports_archive
        }

        fn set_session_archived(
            &self,
            session_id: &acp::SessionId,
            archived: bool,
            _cx: &mut App,
        ) -> Task<Result<()>> {
            self.archive_requests
                .borrow_mut()
                .push((session_id.0.to_string(), archived));
            if self.fail_archive_requests.get() {
                return Task::ready(Err(anyhow::anyhow!("the agent is unavailable")));
            }
            self.set_archived(&session_id.0, archived);
            Task::ready(Ok(()))
        }

        fn into_any(self: Rc<Self>) -> Rc<dyn Any> {
            self
        }
    }

    fn set_archived_meta(session: &mut AgentSessionInfo, archived: bool) {
        session
            .meta
            .get_or_insert_default()
            .insert(ARCHIVED_META_KEY.into(), archived.into());
    }

    fn archived(mut session: AgentSessionInfo) -> AgentSessionInfo {
        set_archived_meta(&mut session, true);
        session
    }

    fn info_update(update: serde_json::Value) -> AgentSessionInfoUpdate {
        serde_json::from_value::<agent_client_protocol::schema::v1::SessionInfoUpdate>(update)
            .expect("session info update")
            .into()
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
            OpenElsewhereStore::init_global(cx);
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

        list.sessions.borrow_mut().clear();
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

    fn mock_remote(id: u64) -> RemoteConnectionOptions {
        RemoteConnectionOptions::Mock(remote::MockConnectionOptions { id })
    }

    fn watch_remote(
        remote_connection: &RemoteConnectionOptions,
        auto_import: &Entity<SessionAutoImport>,
        list: &Rc<FakeSessionList>,
        cx: &mut TestAppContext,
    ) {
        auto_import.update(cx, |auto_import, cx| {
            auto_import.watch(
                AgentId::new("gorynych"),
                Some(remote_connection.clone()),
                list.clone(),
                cx,
            );
        });
        cx.run_until_parked();
    }

    /// The machine each of the session's threads belongs to, by remote id.
    fn machines_of(session_id: &str, cx: &TestAppContext) -> Vec<Option<u64>> {
        cx.read(|cx| {
            let mut machines = ThreadMetadataStore::global(cx)
                .read(cx)
                .entries()
                .filter(|thread| thread.session_id == Some(acp::SessionId::new(session_id)))
                .map(|thread| match &thread.remote_connection {
                    Some(RemoteConnectionOptions::Mock(options)) => Some(options.id),
                    Some(_) => panic!("unexpected remote connection"),
                    None => None,
                })
                .collect::<Vec<_>>();
            machines.sort();
            machines
        })
    }

    #[gpui::test]
    async fn test_the_same_agent_on_a_remote_machine_keeps_its_own_threads(
        cx: &mut TestAppContext,
    ) {
        let auto_import = init_test(cx);
        let remote_connection = mock_remote(7);
        let here = FakeSessionList::new(
            false,
            vec![
                session("shared", "Here", "/project", 1),
                session("only-here", "Only here", "/project", 2),
            ],
        );
        let there = FakeSessionList::new(
            false,
            vec![
                session("shared", "There", "/home/me/project", 3),
                session("only-there", "Only there", "/home/me/project", 4),
            ],
        );
        watch(&auto_import, &here, cx);
        watch_remote(&remote_connection, &auto_import, &there, cx);

        assert_eq!(machines_of("shared", cx), [None, Some(7)]);
        assert_eq!(machines_of("only-here", cx), [None]);
        assert_eq!(machines_of("only-there", cx), [Some(7)]);
        assert_eq!(
            thread("only-there", cx).map(|thread| thread.folder_paths().clone()),
            Some(PathList::new(&[Path::new("/home/me/project")])),
            "remote threads keep the folders the remote agent reported"
        );

        // Each machine's complete listing speaks only for its own sessions.
        cx.executor().advance_clock(POLL_INTERVAL);
        cx.run_until_parked();
        assert_eq!(machines_of("shared", cx), [None, Some(7)]);
        assert_eq!(machines_of("only-there", cx), [Some(7)]);

        // A session gone from one machine drops only that machine's thread.
        there
            .sessions
            .borrow_mut()
            .retain(|session| session.session_id.0.as_ref() != "shared");
        cx.executor().advance_clock(POLL_INTERVAL);
        cx.run_until_parked();
        assert_eq!(machines_of("shared", cx), [None]);
        here.send(SessionListUpdate::SessionRemoved {
            session_id: acp::SessionId::new("only-there"),
        });
        cx.run_until_parked();
        assert_eq!(machines_of("only-there", cx), [Some(7)]);

        // With its connection gone, the remote machine's threads stay while this
        // machine's agent keeps listing its own sessions.
        drop(there);
        cx.run_until_parked();
        here.sessions.borrow_mut().clear();
        cx.executor().advance_clock(POLL_INTERVAL);
        cx.run_until_parked();
        assert_eq!(machines_of("shared", cx), Vec::<Option<u64>>::new());
        assert_eq!(machines_of("only-here", cx), Vec::<Option<u64>>::new());
        assert_eq!(machines_of("only-there", cx), [Some(7)]);
    }

    #[gpui::test]
    async fn test_known_sessions_are_kept_per_machine(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = settings::SettingsStore::test(cx);
            cx.set_global(settings_store);
            ThreadMetadataStore::init_global(cx);
        });
        let db = SessionAutoImportDb(
            db::open_test_db::<SessionAutoImportDb>("test_known_sessions_are_kept_per_machine")
                .await,
        );
        let auto_import = cx.new(|cx| SessionAutoImport::with_db(db.clone(), cx));
        cx.run_until_parked();
        let remote_connection = mock_remote(3);
        let there = FakeSessionList::new(true, vec![session("s1", "There", "/remote", 1)]);
        watch_remote(&remote_connection, &auto_import, &there, cx);
        let remote_thread = thread("s1", cx).expect("imported from the remote machine");
        update_store(cx, |store, cx| store.delete(remote_thread.thread_id, cx));
        drop(there);
        drop(auto_import);
        cx.run_until_parked();

        // After a restart, the session deleted from the remote machine's threads
        // stays deleted there, while this machine's session of the same id is new.
        let auto_import = cx.new(|cx| SessionAutoImport::with_db(db, cx));
        cx.run_until_parked();
        let there = FakeSessionList::new(true, vec![session("s1", "There", "/remote", 1)]);
        watch_remote(&remote_connection, &auto_import, &there, cx);
        let here = FakeSessionList::new(true, vec![session("s1", "Here", "/project", 2)]);
        watch(&auto_import, &here, cx);
        assert_eq!(machines_of("s1", cx), [None]);
    }

    /// What builds from before remote sources read, and how they write.
    const EARLIER_BUILDS_LIST: &str =
        "SELECT agent_id, session_id, imported, archived FROM auto_import_known_sessions";
    const EARLIER_BUILDS_SAVE: &str = "INSERT INTO auto_import_known_sessions(agent_id, session_id, imported, archived) \
         VALUES ('gorynych', 'saved-by-an-earlier-build', 1, NULL) \
         ON CONFLICT(agent_id, session_id) DO UPDATE SET \
         imported = excluded.imported, archived = excluded.archived";

    #[test]
    fn test_known_sessions_from_before_remote_sources_belong_to_this_machine() {
        let connection = db::sqlez::connection::Connection::open_memory(Some(
            "test_known_sessions_from_before_remote_sources",
        ));
        let migrations = <SessionAutoImportDb as Domain>::MIGRATIONS;
        connection
            .migrate(
                SessionAutoImportDb::NAME,
                &migrations[..2],
                &mut |_, _, _| false,
            )
            .unwrap();
        connection
            .exec(
                "INSERT INTO auto_import_known_sessions(agent_id, session_id, imported, archived) \
                 VALUES ('Gorynych: Claude', 's1', 1, 0)",
            )
            .unwrap()()
        .unwrap();
        connection
            .migrate(SessionAutoImportDb::NAME, migrations, &mut |_, _, _| false)
            .unwrap();
        let rows = connection
            .select::<(String, String, String, bool, Option<bool>)>(
                "SELECT agent_id, remote, session_id, imported, archived \
                 FROM auto_import_known_sessions_by_source",
            )
            .unwrap()()
        .unwrap();
        assert_eq!(
            rows,
            [(
                "Gorynych: Claude".to_string(),
                String::new(),
                "s1".to_string(),
                true,
                Some(false)
            )]
        );
        let source = SessionSource::from(StoredSource {
            agent_id: rows[0].0.clone(),
            remote: rows[0].1.clone(),
        });
        assert_eq!(
            source,
            SessionSource::local(AgentId::new("Gorynych: Claude"))
        );

        // Earlier builds still find their table as they left it.
        let earlier = connection
            .select::<(String, String, bool, Option<bool>)>(EARLIER_BUILDS_LIST)
            .unwrap()()
        .unwrap();
        assert_eq!(
            earlier,
            [(
                "Gorynych: Claude".to_string(),
                "s1".to_string(),
                true,
                Some(false)
            )]
        );
        connection.exec(EARLIER_BUILDS_SAVE).unwrap()().unwrap();
    }

    #[gpui::test]
    async fn test_earlier_builds_see_only_this_machines_sessions(_cx: &mut TestAppContext) {
        let db = SessionAutoImportDb(
            db::open_test_db::<SessionAutoImportDb>("test_earlier_builds_see_only_this_machine")
                .await,
        );
        let here = SessionSource::local(AgentId::new("gorynych"));
        let there = SessionSource::new(AgentId::new("gorynych"), Some(&mock_remote(5)));
        let imported = KnownSession {
            imported: true,
            archived: None,
        };
        for (source, session_id) in [(&here, "local"), (&there, "remote"), (&here, "gone")] {
            db.apply(KnownSessionWrite::Save {
                source: source.into(),
                session_id: session_id.to_string(),
                session: imported,
            })
            .await
            .unwrap();
        }
        db.apply(KnownSessionWrite::Delete {
            source: (&here).into(),
            session_id: "gone".to_string(),
        })
        .await
        .unwrap();
        db.apply(KnownSessionWrite::Delete {
            source: (&there).into(),
            session_id: "local".to_string(),
        })
        .await
        .unwrap();

        let mut known = db
            .list()
            .unwrap()
            .into_iter()
            .map(|(source, session_id, _, _)| (SessionSource::from(source), session_id))
            .collect::<Vec<_>>();
        known.sort_by_key(|(_, session_id)| session_id.clone());
        assert_eq!(
            known,
            [
                (here.clone(), "local".to_string()),
                (there.clone(), "remote".to_string())
            ]
        );
        // A build from before remote sources never sees the remote session as
        // one of this machine's, so its listings can't drop the remote thread.
        let earlier = db
            .select::<(String, String, bool, Option<bool>)>(EARLIER_BUILDS_LIST)
            .unwrap()()
        .unwrap();
        assert_eq!(
            earlier,
            [("gorynych".to_string(), "local".to_string(), true, None)]
        );
        db.write(|conn| conn.exec(EARLIER_BUILDS_SAVE).unwrap()())
            .await
            .unwrap();
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
                .parent_session_id(
                    &SessionSource::local(AgentId::new(agent)),
                    &acp::SessionId::new(session_id),
                )
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

    fn open_elsewhere(mut session: AgentSessionInfo, holder: &str) -> AgentSessionInfo {
        session.meta.get_or_insert_default().insert(
            crate::open_elsewhere_store::OPEN_ELSEWHERE_META_KEY.into(),
            holder.into(),
        );
        session
    }

    fn holder(session_id: &str, cx: &TestAppContext) -> Option<String> {
        cx.read(|cx| {
            OpenElsewhereStore::try_global(cx)?
                .read(cx)
                .holder(
                    &SessionSource::local(AgentId::new("gorynych")),
                    &acp::SessionId::new(session_id),
                )
                .map(|holder| holder.to_string())
        })
    }

    #[gpui::test]
    async fn test_listings_mark_sessions_open_in_other_apps(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(
            true,
            vec![
                open_elsewhere(
                    session("desktop", "Desktop", "/project", 1),
                    "Codex Desktop",
                ),
                session("free", "Free", "/project", 2),
                open_elsewhere(
                    session("terminal", "Terminal", "/project", 3),
                    "a terminal codex (pid 20796)",
                ),
            ],
        );
        watch(&auto_import, &list, cx);
        assert_eq!(holder("desktop", cx).as_deref(), Some("Codex Desktop"));
        assert_eq!(holder("free", cx), None);
        assert_eq!(
            holder("terminal", cx).as_deref(),
            Some("a terminal codex (pid 20796)")
        );
        // A session open elsewhere is imported like any other.
        assert!(thread("desktop", cx).is_some_and(|thread| !thread.archived));

        // Each listing is authoritative: closed there, opened elsewhere, gone.
        *list.sessions.borrow_mut() = vec![
            session("desktop", "Desktop", "/project", 1),
            open_elsewhere(session("free", "Free", "/project", 2), "VS Code"),
        ];
        list.refresh(cx);
        assert_eq!(holder("desktop", cx), None);
        assert_eq!(holder("free", cx).as_deref(), Some("VS Code"));
        assert_eq!(holder("terminal", cx), None);

        // The marks go when Zed stops hearing from the agent.
        drop(list);
        cx.run_until_parked();
        assert_eq!(holder("free", cx), None);
    }

    #[gpui::test]
    async fn test_interactivity_updates_keep_open_elsewhere_marks_in_step(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(true, vec![session("held", "Held", "/project", 1)]);
        watch(&auto_import, &list, cx);
        assert_eq!(holder("held", cx), None);
        let send = |update: serde_json::Value, cx: &mut TestAppContext| {
            list.send(SessionListUpdate::SessionInfo {
                session_id: acp::SessionId::new("held"),
                update: info_update(update),
            });
            cx.run_until_parked();
        };

        send(
            serde_json::json!({"_meta": {
                "gorynych/interactivity": "read-only",
                "gorynych/openElsewhere": "Codex Desktop",
            }}),
            cx,
        );
        assert_eq!(holder("held", cx).as_deref(), Some("Codex Desktop"));

        // Read-only without a holder, or a holder without interactivity, leaves the
        // mark to the listings.
        send(
            serde_json::json!({"_meta": {"gorynych/interactivity": "read-only"}}),
            cx,
        );
        send(
            serde_json::json!({"title": "Renamed", "_meta": {"gorynych/openElsewhere": "VS Code"}}),
            cx,
        );
        assert_eq!(holder("held", cx).as_deref(), Some("Codex Desktop"));

        // A session that takes prompts again is free.
        send(
            serde_json::json!({"_meta": {"gorynych/interactivity": "full"}}),
            cx,
        );
        assert_eq!(holder("held", cx), None);
    }

    fn is_archived(session_id: &str, cx: &TestAppContext) -> Option<bool> {
        thread(session_id, cx).map(|thread| thread.archived)
    }

    fn save_thread(
        session_id: &str,
        folder: Option<&str>,
        archived: bool,
        cx: &mut TestAppContext,
    ) -> ThreadId {
        let thread_id = ThreadId::new();
        let worktree_paths = folder.map_or_else(WorktreePaths::default, |folder| {
            WorktreePaths::from_folder_paths(&PathList::new(&[Path::new(folder)]))
        });
        update_store(cx, |store, cx| {
            store.save(
                ThreadMetadata {
                    thread_id,
                    session_id: Some(acp::SessionId::new(session_id)),
                    agent_id: AgentId::new("gorynych"),
                    title: None,
                    title_override: None,
                    updated_at: timestamp(1),
                    created_at: None,
                    interacted_at: None,
                    worktree_paths,
                    remote_connection: None,
                    archived,
                },
                cx,
            )
        });
        thread_id
    }

    fn request(session_id: &str, archived: bool) -> (String, bool) {
        (session_id.to_string(), archived)
    }

    #[gpui::test]
    async fn test_threads_follow_the_sessions_their_agent_archives_and_restores(
        cx: &mut TestAppContext,
    ) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::with_archive(
            true,
            vec![
                session("active", "Active", "/project", 1),
                archived(session("shelved", "Shelved", "/project", 2)),
            ],
        );
        watch(&auto_import, &list, cx);

        // Archived sessions are listed on request, and arrive in the archive.
        let include_archived = serde_json::Value::Bool(true);
        assert!(list.listing_metas.borrow().iter().all(|meta| {
            meta.as_ref()
                .and_then(|meta| meta.get(INCLUDE_ARCHIVED_META_KEY))
                == Some(&include_archived)
        }));
        assert_eq!(is_archived("active", cx), Some(false));
        assert_eq!(is_archived("shelved", cx), Some(true));

        // A listing reports an archive.
        list.set_archived("active", true);
        list.refresh(cx);
        assert_eq!(is_archived("active", cx), Some(true));

        // A live session reports a restore.
        list.set_archived("shelved", false);
        list.send(SessionListUpdate::SessionInfo {
            session_id: acp::SessionId::new("shelved"),
            update: info_update(serde_json::json!({"_meta": {"gorynych/archived": false}})),
        });
        cx.run_until_parked();
        assert_eq!(is_archived("shelved", cx), Some(false));

        // Following the agent tells it nothing back.
        assert!(list.requests().is_empty());
    }

    #[gpui::test]
    async fn test_archiving_in_zed_reaches_the_agent_without_echo(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::with_archive(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);
        let thread_id = thread("s1", cx).expect("imported").thread_id;

        update_store(cx, |store, cx| store.archive(thread_id, None, cx));
        assert_eq!(list.requests(), [request("s1", true)]);

        // Later changes to the thread don't repeat the request, and the agent
        // reporting the archive, in a listing or live, doesn't bounce it back.
        update_store(cx, |store, cx| {
            store.update_interacted_at(&thread_id, Utc::now(), cx)
        });
        list.refresh(cx);
        list.send(SessionListUpdate::SessionInfo {
            session_id: acp::SessionId::new("s1"),
            update: info_update(serde_json::json!({"_meta": {"gorynych/archived": true}})),
        });
        cx.run_until_parked();
        assert_eq!(list.requests(), [request("s1", true)]);
        assert_eq!(is_archived("s1", cx), Some(true));

        // Opening an archived thread restores it in stock Zed, so the agent hears
        // of that too.
        update_store(cx, |store, cx| store.unarchive(thread_id, cx));
        list.refresh(cx);
        assert_eq!(list.requests(), [request("s1", true), request("s1", false)]);
        assert_eq!(is_archived("s1", cx), Some(false));
    }

    #[gpui::test]
    async fn test_failed_archive_requests_leave_the_thread_archived(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::with_archive(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);
        let thread_id = thread("s1", cx).expect("imported").thread_id;

        list.fail_archive_requests.set(true);
        update_store(cx, |store, cx| store.archive(thread_id, None, cx));
        update_store(cx, |store, cx| {
            store.update_interacted_at(&thread_id, Utc::now(), cx)
        });
        assert_eq!(list.requests(), [request("s1", true)]);
        assert_eq!(is_archived("s1", cx), Some(true));

        // The next listing still has the session unarchived, so it is asked again.
        list.fail_archive_requests.set(false);
        list.refresh(cx);
        assert_eq!(list.requests(), [request("s1", true), request("s1", true)]);
        assert_eq!(is_archived("s1", cx), Some(true));
    }

    #[gpui::test]
    async fn test_threads_archived_before_their_agent_reported_flags_reach_it(
        cx: &mut TestAppContext,
    ) {
        let auto_import = init_test(cx);
        save_thread("archived-in-zed", Some("/project"), true, cx);
        save_thread("no-folder", None, true, cx);
        let list = FakeSessionList::with_archive(
            true,
            vec![
                session("archived-in-zed", "Archived in Zed", "/project", 1),
                session("no-folder", "No folder", "/project", 2),
            ],
        );
        watch(&auto_import, &list, cx);

        // Zed archives threads without a folder on its own, so they aren't the
        // user's archive.
        assert_eq!(list.requests(), [request("archived-in-zed", true)]);
        assert_eq!(is_archived("archived-in-zed", cx), Some(true));
        assert_eq!(is_archived("no-folder", cx), Some(true));
    }

    #[gpui::test]
    async fn test_threads_restored_while_their_agent_is_away_reach_it_later(
        cx: &mut TestAppContext,
    ) {
        cx.update(|cx| {
            let settings_store = settings::SettingsStore::test(cx);
            cx.set_global(settings_store);
            ThreadMetadataStore::init_global(cx);
        });
        let db = SessionAutoImportDb(
            db::open_test_db::<SessionAutoImportDb>("test_threads_restored_while_away").await,
        );
        let auto_import = cx.new(|cx| SessionAutoImport::with_db(db.clone(), cx));
        let list = FakeSessionList::with_archive(
            true,
            vec![archived(session("s1", "First", "/project", 1))],
        );
        watch(&auto_import, &list, cx);
        let thread_id = thread("s1", cx).expect("imported").thread_id;
        assert_eq!(is_archived("s1", cx), Some(true));

        // Zed restarts, and the user restores the thread before the agent is back.
        drop(list);
        drop(auto_import);
        cx.run_until_parked();
        let auto_import = cx.new(|cx| SessionAutoImport::with_db(db, cx));
        cx.run_until_parked();
        update_store(cx, |store, cx| store.unarchive(thread_id, cx));

        let list = FakeSessionList::with_archive(
            true,
            vec![archived(session("s1", "First", "/project", 1))],
        );
        watch(&auto_import, &list, cx);
        assert_eq!(list.requests(), [request("s1", false)]);
        assert_eq!(is_archived("s1", cx), Some(false));
    }

    #[gpui::test]
    async fn test_complete_listings_keep_sessions_archived_elsewhere(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::with_archive(
            false,
            vec![
                session("archived-elsewhere", "Archived elsewhere", "/project", 1),
                session("gone", "Gone", "/project", 2),
            ],
        );
        watch(&auto_import, &list, cx);

        list.set_archived("archived-elsewhere", true);
        list.sessions
            .borrow_mut()
            .retain(|session| session.session_id.0.as_ref() != "gone");
        cx.executor().advance_clock(POLL_INTERVAL);
        cx.run_until_parked();

        assert_eq!(is_archived("archived-elsewhere", cx), Some(true));
        assert!(thread("gone", cx).is_none());
        assert!(list.requests().is_empty());
    }

    #[gpui::test]
    async fn test_agents_without_an_archive_leave_archiving_to_zed(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        // The flag means nothing without the advertisement.
        let list =
            FakeSessionList::new(true, vec![archived(session("s1", "First", "/project", 1))]);
        watch(&auto_import, &list, cx);
        assert!(list.listing_metas.borrow().iter().all(Option::is_none));
        assert_eq!(is_archived("s1", cx), Some(false));

        let thread_id = thread("s1", cx).expect("imported").thread_id;
        update_store(cx, |store, cx| store.archive(thread_id, None, cx));
        list.send(SessionListUpdate::SessionInfo {
            session_id: acp::SessionId::new("s1"),
            update: info_update(serde_json::json!({"_meta": {"gorynych/archived": false}})),
        });
        list.refresh(cx);
        assert_eq!(is_archived("s1", cx), Some(true));
        assert!(list.requests().is_empty());
    }

    #[test]
    fn test_reconcile_archive() {
        // Both sides agree.
        assert_eq!(
            reconcile_archive(true, Some(false), true),
            ArchiveSync::InSync
        );
        // The agent changed its flag.
        assert_eq!(
            reconcile_archive(false, Some(false), true),
            ArchiveSync::Apply(true)
        );
        assert_eq!(
            reconcile_archive(true, Some(true), false),
            ArchiveSync::Apply(false)
        );
        // The user changed the thread.
        assert_eq!(
            reconcile_archive(true, Some(false), false),
            ArchiveSync::Send(true)
        );
        assert_eq!(
            reconcile_archive(false, Some(true), true),
            ArchiveSync::Send(false)
        );
        // Without an earlier report, an archive on either side wins.
        assert_eq!(
            reconcile_archive(true, None, false),
            ArchiveSync::Send(true)
        );
        assert_eq!(
            reconcile_archive(false, None, true),
            ArchiveSync::Apply(true)
        );
    }

    #[gpui::test]
    async fn test_watching_stops_when_the_session_list_is_dropped(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);
        assert!(auto_import.read_with(cx, |auto_import, _| {
            auto_import.is_watching(&SessionSource::local(AgentId::new("gorynych")))
        }));

        drop(list);
        cx.run_until_parked();

        assert!(auto_import.read_with(cx, |auto_import, _| {
            !auto_import.is_watching(&SessionSource::local(AgentId::new("gorynych")))
        }));
    }

    #[gpui::test]
    async fn test_watching_stops_when_the_session_list_stops_reporting(cx: &mut TestAppContext) {
        let auto_import = init_test(cx);
        let list = FakeSessionList::new(true, vec![session("s1", "First", "/project", 1)]);
        watch(&auto_import, &list, cx);

        // The connection is still around, but its session list no longer reports.
        list.updates_tx.close();
        cx.run_until_parked();
        assert!(auto_import.read_with(cx, |auto_import, _| {
            auto_import.watchers.is_empty()
                && !auto_import.is_watching(&SessionSource::local(AgentId::new("gorynych")))
        }));

        // Another connection to the agent, as from another window, takes over.
        let other = FakeSessionList::new(true, vec![session("s2", "Second", "/project", 2)]);
        watch(&auto_import, &other, cx);
        assert!(thread("s2", cx).is_some());

        // Watching a list whose updates have ended falls back to polling it.
        drop(other);
        cx.run_until_parked();
        let calls = list.list_calls.get();
        list.sessions
            .borrow_mut()
            .push(session("s3", "Third", "/project", 3));
        watch(&auto_import, &list, cx);
        assert_eq!(list.list_calls.get(), calls + 1);
        assert!(thread("s3", cx).is_some());
        cx.executor().advance_clock(POLL_INTERVAL);
        cx.run_until_parked();
        assert_eq!(list.list_calls.get(), calls + 2);
        assert!(auto_import.read_with(cx, |auto_import, _| {
            auto_import.is_watching(&SessionSource::local(AgentId::new("gorynych")))
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
                archive: false,
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
        assert_eq!(
            plan.known,
            [(
                acp::SessionId::new("new"),
                KnownSession {
                    imported: true,
                    archived: None
                }
            )]
        );
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
        let known = HashMap::from_iter([(
            session_id.clone(),
            KnownSession {
                imported: true,
                archived: None,
            },
        )]);
        let plan = |complete| {
            plan_sync(
                &AgentId::new("gorynych"),
                None,
                SessionListing {
                    sessions: Vec::new(),
                    complete,
                    archive: false,
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
    fn test_threads_take_the_time_their_agent_lists() {
        let thread = |session_id: &str| ThreadMetadata {
            thread_id: ThreadId::new(),
            session_id: Some(acp::SessionId::new(session_id)),
            agent_id: AgentId::new("gorynych"),
            title: None,
            title_override: None,
            // Stamped by Zed when it loaded the thread, after the agent last ran it.
            updated_at: timestamp(30),
            created_at: None,
            interacted_at: None,
            worktree_paths: WorktreePaths::from_folder_paths(&PathList::new(&[Path::new(
                "/project",
            )])),
            remote_connection: None,
            archived: false,
        };
        let (timed, untimed, gone) = (thread("timed"), thread("untimed"), thread("gone"));
        let session_ids = ["timed", "untimed", "gone"].map(acp::SessionId::new);
        let threads = session_ids
            .iter()
            .zip([&timed, &untimed, &gone])
            .collect::<HashMap<_, _>>();
        let known = session_ids
            .iter()
            .map(|session_id| {
                (
                    session_id.clone(),
                    KnownSession {
                        imported: false,
                        archived: None,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let mut untimed_session = session("untimed", "Untimed", "/project", 0);
        untimed_session.updated_at = None;

        let plan = plan_sync(
            &AgentId::new("gorynych"),
            None,
            SessionListing {
                sessions: vec![
                    session("timed", "Timed", "/project", 5),
                    untimed_session,
                    session("new", "New", "/project", 7),
                ],
                complete: true,
                archive: false,
            },
            &threads,
            &known,
            &HashSet::default(),
            &HashSet::default(),
            timestamp(40),
        );

        let saved = |session_id: &str| {
            plan.save
                .iter()
                .find(|thread| thread.session_id == Some(acp::SessionId::new(session_id)))
                .expect("thread saved")
        };
        assert_eq!(saved("timed").updated_at, timestamp(5));
        assert_eq!(saved("untimed").updated_at, timestamp(30));
        let new = saved("new");
        assert_eq!(new.updated_at, timestamp(7));
        assert_eq!(
            plan.times_from_agent
                .iter()
                .copied()
                .collect::<HashMap<_, _>>(),
            HashMap::from_iter([
                (timed.thread_id, true),
                (untimed.thread_id, false),
                (new.thread_id, true),
                (gone.thread_id, false),
            ])
        );
    }

    #[test]
    fn test_connection_attempts_back_off_and_give_up() {
        let start = Instant::now();
        let mut attempts = ConnectAttempts {
            count: 1,
            last_attempt: start,
        };
        assert_eq!(
            attempts.next_attempt(start + Duration::from_secs(10)),
            Some(start + CONNECT_RETRY_DELAY)
        );
        attempts.count = 2;
        assert_eq!(
            attempts.next_attempt(start + CONNECT_RETRY_DELAY),
            Some(start + CONNECT_RETRY_DELAY * 2)
        );
        attempts.count = MAX_CONNECT_ATTEMPTS;
        assert_eq!(
            attempts.next_attempt(start + Duration::from_secs(9 * 60)),
            None
        );
        // A connection that lasted counts as working again.
        let later = start + STABLE_CONNECTION;
        assert_eq!(attempts.next_attempt(later), Some(later));
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
