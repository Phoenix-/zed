//! Remembers which session started which, as reported by agents.
//!
//! An agent may report, in the `_meta` of a session's info, the session that
//! started it. The sidebar uses this to nest such threads under their parent.
//!
//! The links live in their own database domain rather than in a column of the
//! thread metadata table, so that they never interfere with the migrations of
//! that table. A link belongs to a session of one agent, while the parent may be
//! a session of another agent.

use agent_client_protocol::schema::v2 as acp;
use anyhow::Context as _;
use collections::{HashMap, HashSet};
use db::{
    sqlez::{domain::Domain, statement::Statement, thread_safe_connection::ThreadSafeConnection},
    sqlez_macros::sql,
};
use futures::{FutureExt as _, future::Shared};
use gpui::{App, AppContext as _, Context, Entity, Global, Task};
use project::AgentId;
use util::ResultExt as _;

use crate::thread_metadata_store::ThreadMetadataStore;

/// Key in the `_meta` of a session's info that names the session that started it.
pub const PARENT_SESSION_ID_META_KEY: &str = "gorynych/parentSessionId";

/// Returns the parent session named in a session's `_meta`, if any.
pub fn parent_session_id_from_meta(meta: Option<&acp::Meta>) -> Option<acp::SessionId> {
    meta?
        .get(PARENT_SESSION_ID_META_KEY)?
        .as_str()
        .filter(|parent| !parent.is_empty())
        .map(acp::SessionId::new)
}

pub fn init(cx: &mut App) {
    ThreadParentStore::init_global(cx);
}

struct GlobalThreadParentStore(Entity<ThreadParentStore>);

impl Global for GlobalThreadParentStore {}

type SessionKey = (AgentId, acp::SessionId);

pub struct ThreadParentStore {
    parents: HashMap<SessionKey, acp::SessionId>,
    reload_task: Shared<Task<()>>,
    /// The sessions that had a thread when the metadata store last changed, so
    /// that the links of deleted threads go with them. `None` until it has loaded.
    thread_sessions: Option<HashSet<SessionKey>>,
    /// Writes go through one queue so that they apply in the order they are made.
    pending_writes: async_channel::Sender<(String, String, Option<String>)>,
    _write_task: Task<()>,
    _metadata_subscription: Option<gpui::Subscription>,
}

impl ThreadParentStore {
    pub fn init_global(cx: &mut App) {
        #[cfg(any(test, feature = "test-support"))]
        let db = ThreadParentDb(gpui::block_on(db::open_test_db::<ThreadParentDb>(
            &format!("THREAD_PARENTS_{}", uuid::Uuid::new_v4()),
        )));
        #[cfg(not(any(test, feature = "test-support")))]
        let db = ThreadParentDb::global(cx);

        let store = cx.new(|cx| Self::new(db, cx));
        cx.set_global(GlobalThreadParentStore(store));
    }

    pub fn try_global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<GlobalThreadParentStore>()
            .map(|store| store.0.clone())
    }

    pub fn global(cx: &App) -> Entity<Self> {
        cx.global::<GlobalThreadParentStore>().0.clone()
    }

    fn new(db: ThreadParentDb, cx: &mut Context<Self>) -> Self {
        let metadata_store = ThreadMetadataStore::try_global(cx);
        let metadata_loaded = metadata_store
            .as_ref()
            .map(|store| store.read(cx).reload_task());
        let metadata_subscription = metadata_store.as_ref().map(|store| {
            cx.observe(store, |this, store, cx| {
                this.forget_links_of_deleted_threads(store.read(cx));
            })
        });

        let rows = cx.background_spawn({
            let db = db.clone();
            async move { db.list() }
        });
        let reload_task = cx
            .spawn(async move |this, cx| {
                let rows = rows
                    .await
                    .context("Failed to load thread parents")
                    .log_err();
                if let Some(metadata_loaded) = metadata_loaded {
                    metadata_loaded.await;
                }
                this.update(cx, |this, cx| {
                    for (agent_id, session_id, parent_session_id) in rows.into_iter().flatten() {
                        // Links set while loading are newer than the stored ones.
                        this.parents
                            .entry((AgentId::new(agent_id), acp::SessionId::new(session_id)))
                            .or_insert_with(|| acp::SessionId::new(parent_session_id));
                    }
                    if let Some(metadata_store) = ThreadMetadataStore::try_global(cx) {
                        this.thread_sessions = Some(thread_sessions(metadata_store.read(cx)));
                    }
                    cx.notify();
                })
                .ok();
            })
            .shared();

        let (pending_writes, writes) =
            async_channel::unbounded::<(String, String, Option<String>)>();
        let write_task = cx.background_spawn(async move {
            while let Ok((agent_id, session_id, parent_session_id)) = writes.recv().await {
                db.save(agent_id, session_id, parent_session_id)
                    .await
                    .log_err();
            }
        });

        Self {
            parents: HashMap::default(),
            reload_task,
            thread_sessions: None,
            pending_writes,
            _write_task: write_task,
            _metadata_subscription: metadata_subscription,
        }
    }

    pub fn reload_task(&self) -> Shared<Task<()>> {
        self.reload_task.clone()
    }

    pub fn parent_session_id(
        &self,
        agent_id: &AgentId,
        session_id: &acp::SessionId,
    ) -> Option<&acp::SessionId> {
        self.parents.get(&(agent_id.clone(), session_id.clone()))
    }

    pub fn set_parent_session_id(
        &mut self,
        agent_id: AgentId,
        session_id: acp::SessionId,
        parent_session_id: Option<acp::SessionId>,
        cx: &mut Context<Self>,
    ) {
        let parent_session_id = parent_session_id.filter(|parent| parent != &session_id);
        let key = (agent_id, session_id);
        if self.parents.get(&key) == parent_session_id.as_ref() {
            return;
        }
        self.write(&key, parent_session_id.as_ref());
        match parent_session_id {
            Some(parent_session_id) => self.parents.insert(key, parent_session_id),
            None => self.parents.remove(&key),
        };
        cx.notify();
    }

    fn write(&self, (agent_id, session_id): &SessionKey, parent: Option<&acp::SessionId>) {
        self.pending_writes
            .try_send((
                agent_id.to_string(),
                session_id.0.to_string(),
                parent.map(|parent| parent.0.to_string()),
            ))
            .log_err();
    }

    fn forget_links_of_deleted_threads(&mut self, metadata_store: &ThreadMetadataStore) {
        let Some(previous) = self.thread_sessions.as_mut() else {
            return;
        };
        let current = thread_sessions(metadata_store);
        let deleted = previous.difference(&current).cloned().collect::<Vec<_>>();
        *previous = current;
        for key in deleted {
            if self.parents.remove(&key).is_some() {
                self.write(&key, None);
            }
        }
    }
}

fn thread_sessions(metadata_store: &ThreadMetadataStore) -> HashSet<SessionKey> {
    metadata_store
        .entries()
        .filter_map(|thread| Some((thread.agent_id.clone(), thread.session_id.clone()?)))
        .collect()
}

struct ThreadParentDb(ThreadSafeConnection);

impl Domain for ThreadParentDb {
    const NAME: &str = stringify!(ThreadParentDb);

    const MIGRATIONS: &[&str] = &[sql!(
        CREATE TABLE IF NOT EXISTS agent_session_parents(
            agent_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            parent_session_id TEXT NOT NULL,
            PRIMARY KEY (agent_id, session_id)
        ) STRICT;
    )];
}

db::static_connection!(ThreadParentDb, []);

impl ThreadParentDb {
    fn list(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        self.select::<(String, String, String)>(
            "SELECT agent_id, session_id, parent_session_id FROM agent_session_parents",
        )?()
    }

    async fn save(
        &self,
        agent_id: String,
        session_id: String,
        parent_session_id: Option<String>,
    ) -> anyhow::Result<()> {
        self.write(move |conn| match parent_session_id {
            Some(parent_session_id) => {
                let mut statement = Statement::prepare(
                    conn,
                    "INSERT INTO agent_session_parents(agent_id, session_id, parent_session_id) \
                     VALUES (?1, ?2, ?3) \
                     ON CONFLICT(agent_id, session_id) DO UPDATE SET \
                         parent_session_id = excluded.parent_session_id",
                )?;
                let mut index = statement.bind(&agent_id, 1)?;
                index = statement.bind(&session_id, index)?;
                statement.bind(&parent_session_id, index)?;
                statement.exec()
            }
            None => {
                let mut statement = Statement::prepare(
                    conn,
                    "DELETE FROM agent_session_parents WHERE agent_id = ?1 AND session_id = ?2",
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
    use crate::thread_metadata_store::{ThreadId, ThreadMetadata, WorktreePaths};
    use gpui::TestAppContext;

    fn key(agent: &str, session: &str) -> (AgentId, acp::SessionId) {
        (AgentId::new(agent), acp::SessionId::new(session))
    }

    fn parent(store: &ThreadParentStore, agent: &str, session: &str) -> Option<String> {
        let (agent_id, session_id) = key(agent, session);
        store
            .parent_session_id(&agent_id, &session_id)
            .map(|parent| parent.0.to_string())
    }

    #[test]
    fn test_parent_session_id_from_meta() {
        assert_eq!(parent_session_id_from_meta(None), None);
        let meta = acp::Meta::from_iter([(PARENT_SESSION_ID_META_KEY.into(), "parent".into())]);
        assert_eq!(
            parent_session_id_from_meta(Some(&meta)),
            Some(acp::SessionId::new("parent"))
        );
        for value in [serde_json::json!(""), serde_json::json!(7)] {
            let meta = acp::Meta::from_iter([(PARENT_SESSION_ID_META_KEY.into(), value)]);
            assert_eq!(parent_session_id_from_meta(Some(&meta)), None);
        }
    }

    #[gpui::test]
    async fn test_parent_links_persist_per_agent(cx: &mut TestAppContext) {
        let db =
            ThreadParentDb(db::open_test_db::<ThreadParentDb>("test_parent_links_persist").await);
        let store = cx.new(|cx| ThreadParentStore::new(db.clone(), cx));
        cx.run_until_parked();

        store.update(cx, |store, cx| {
            let mut set = |agent: &str, session: &str, parent: Option<&str>| {
                let (agent_id, session_id) = key(agent, session);
                store.set_parent_session_id(
                    agent_id,
                    session_id,
                    parent.map(acp::SessionId::new),
                    cx,
                );
            };
            set("kimi", "coder", Some("coordinator"));
            set("claude", "coder", Some("other-coordinator"));
            set("kimi", "reviewer", Some("coder"));
            set("kimi", "loop", Some("loop"));
            set("kimi", "reviewer", None);
        });
        cx.run_until_parked();

        let reloaded = cx.new(|cx| ThreadParentStore::new(db, cx));
        cx.run_until_parked();
        reloaded.read_with(cx, |store, _| {
            assert_eq!(
                parent(store, "kimi", "coder").as_deref(),
                Some("coordinator")
            );
            assert_eq!(
                parent(store, "claude", "coder").as_deref(),
                Some("other-coordinator")
            );
            assert_eq!(parent(store, "kimi", "reviewer"), None);
            assert_eq!(parent(store, "kimi", "loop"), None);
        });
    }

    #[gpui::test]
    async fn test_links_go_with_their_threads(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = settings::SettingsStore::test(cx);
            cx.set_global(settings_store);
            ThreadMetadataStore::init_global(cx);
            ThreadParentStore::init_global(cx);
        });
        cx.run_until_parked();

        let thread = ThreadMetadata {
            thread_id: ThreadId::new(),
            session_id: Some(acp::SessionId::new("coder")),
            agent_id: AgentId::new("kimi"),
            title: None,
            title_override: None,
            updated_at: chrono::Utc::now(),
            created_at: None,
            interacted_at: None,
            worktree_paths: WorktreePaths::default(),
            remote_connection: None,
            archived: false,
        };
        let thread_id = thread.thread_id;
        cx.update(|cx| {
            ThreadMetadataStore::global(cx).update(cx, |store, cx| store.save(thread, cx));
            ThreadParentStore::global(cx).update(cx, |store, cx| {
                let (agent_id, session_id) = key("kimi", "coder");
                store.set_parent_session_id(
                    agent_id,
                    session_id,
                    Some(acp::SessionId::new("coordinator")),
                    cx,
                );
            });
        });
        cx.run_until_parked();
        let linked = cx.read(|cx| parent(ThreadParentStore::global(cx).read(cx), "kimi", "coder"));
        assert_eq!(linked.as_deref(), Some("coordinator"));

        cx.update(|cx| {
            ThreadMetadataStore::global(cx).update(cx, |store, cx| store.delete(thread_id, cx));
        });
        cx.run_until_parked();
        let linked = cx.read(|cx| parent(ThreadParentStore::global(cx).read(cx), "kimi", "coder"));
        assert_eq!(linked, None);
    }
}
