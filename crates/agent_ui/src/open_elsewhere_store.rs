//! Remembers which sessions are open in another app, as their agents report.
//!
//! Some agents can't share a session between processes: while another app (a
//! desktop app, an editor, a terminal) has a session open, Zed can't open it.
//! Such an agent names that app in the `_meta` of the session's info in its
//! session list, so that the sidebar can say so before the user clicks the
//! thread. The marks live only in memory: each listing reports them afresh, and a
//! listed session without the key is free. A session info update that reports the
//! session's interactivity updates its mark as well.

use agent_client_protocol::schema::v2 as acp;
use collections::{HashMap, HashSet};
use gpui::{App, AppContext as _, Context, Entity, Global, SharedString};

use crate::session_auto_import::SessionSource;

/// Key in the `_meta` of a listed session's info that names the app that has the
/// session open, such as "Codex Desktop". A listed session without it is free.
pub const OPEN_ELSEWHERE_META_KEY: &str = "gorynych/openElsewhere";

/// Returns the app named in a session's `_meta` as having it open, if any.
pub fn open_elsewhere_from_meta(meta: Option<&acp::Meta>) -> Option<SharedString> {
    let holder = meta?.get(OPEN_ELSEWHERE_META_KEY)?.as_str()?.trim();
    (!holder.is_empty()).then(|| SharedString::from(holder.to_string()))
}

pub fn init(cx: &mut App) {
    OpenElsewhereStore::init_global(cx);
}

struct GlobalOpenElsewhereStore(Entity<OpenElsewhereStore>);

impl Global for GlobalOpenElsewhereStore {}

#[derive(Default)]
pub struct OpenElsewhereStore {
    holders: HashMap<(SessionSource, acp::SessionId), SharedString>,
}

impl OpenElsewhereStore {
    pub fn init_global(cx: &mut App) {
        let store = cx.new(|_| Self::default());
        cx.set_global(GlobalOpenElsewhereStore(store));
    }

    pub fn try_global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<GlobalOpenElsewhereStore>()
            .map(|store| store.0.clone())
    }

    /// The app that has the session open, as its agent last listed it.
    pub fn holder(
        &self,
        source: &SessionSource,
        session_id: &acp::SessionId,
    ) -> Option<&SharedString> {
        self.holders.get(&(source.clone(), session_id.clone()))
    }

    /// Records what a listing of the source's sessions reported. A listed session
    /// takes the holder it was listed with, or none. When the listing is
    /// complete, the source's sessions missing from it are gone and lose theirs.
    pub fn apply_listing(
        &mut self,
        source: &SessionSource,
        sessions: impl IntoIterator<Item = (acp::SessionId, Option<SharedString>)>,
        complete: bool,
        cx: &mut Context<Self>,
    ) {
        let mut changed = false;
        let mut listed = HashSet::default();
        for (session_id, holder) in sessions {
            // Like the importer, the first listing of a session wins.
            if !listed.insert(session_id.clone()) {
                continue;
            }
            let key = (source.clone(), session_id);
            changed |= match holder {
                Some(holder) => self.holders.insert(key, holder.clone()) != Some(holder),
                None => self.holders.remove(&key).is_some(),
            };
        }
        if complete {
            let count = self.holders.len();
            self.holders.retain(|(holder_source, session_id), _| {
                holder_source != source || listed.contains(session_id)
            });
            changed |= self.holders.len() != count;
        }
        if changed {
            cx.notify();
        }
    }

    /// Records the holder an agent reported for one session outside a listing.
    pub fn set_holder(
        &mut self,
        source: &SessionSource,
        session_id: acp::SessionId,
        holder: Option<SharedString>,
        cx: &mut Context<Self>,
    ) {
        self.apply_listing(source, [(session_id, holder)], false, cx);
    }

    /// Forgets the marks of a source Zed no longer hears from: without it, they
    /// can't be kept up to date.
    pub fn forget_source(&mut self, source: &SessionSource, cx: &mut Context<Self>) {
        let count = self.holders.len();
        self.holders
            .retain(|(holder_source, _), _| holder_source != source);
        if self.holders.len() != count {
            cx.notify();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;
    use project::AgentId;
    use remote::{MockConnectionOptions, RemoteConnectionOptions};

    fn local(agent: &str) -> SessionSource {
        SessionSource::local(AgentId::new(agent))
    }

    fn holder(store: &OpenElsewhereStore, agent: &str, session: &str) -> Option<String> {
        holder_of(store, &local(agent), session)
    }

    fn holder_of(
        store: &OpenElsewhereStore,
        source: &SessionSource,
        session: &str,
    ) -> Option<String> {
        store
            .holder(source, &acp::SessionId::new(session))
            .map(|holder| holder.to_string())
    }

    fn listing(sessions: &[(&str, Option<&str>)]) -> Vec<(acp::SessionId, Option<SharedString>)> {
        sessions
            .iter()
            .map(|(session, holder)| {
                (
                    acp::SessionId::new(*session),
                    holder.map(|holder| SharedString::from(holder.to_string())),
                )
            })
            .collect()
    }

    #[test]
    fn test_open_elsewhere_from_meta() {
        assert_eq!(open_elsewhere_from_meta(None), None);
        let meta = |value: serde_json::Value| {
            acp::Meta::from_iter([(OPEN_ELSEWHERE_META_KEY.into(), value)])
        };
        assert_eq!(
            open_elsewhere_from_meta(Some(&meta(" Codex Desktop ".into()))).as_deref(),
            Some("Codex Desktop")
        );
        for value in [
            serde_json::json!(""),
            serde_json::json!("  "),
            serde_json::json!(true),
            serde_json::json!(null),
        ] {
            assert_eq!(open_elsewhere_from_meta(Some(&meta(value))), None);
        }
    }

    #[gpui::test]
    fn test_listings_set_and_clear_holders(cx: &mut TestAppContext) {
        let store = cx.new(|_| OpenElsewhereStore::default());
        let notifications = std::rc::Rc::new(std::cell::Cell::new(0));
        cx.update(|cx| {
            let notifications = notifications.clone();
            cx.observe(&store, move |_, _| {
                notifications.set(notifications.get() + 1)
            })
            .detach();
        });
        let codex = local("codex");
        let claude = local("claude");

        store.update(cx, |store, cx| {
            store.apply_listing(
                &codex,
                listing(&[
                    ("desktop", Some("Codex Desktop")),
                    ("terminal", Some("a terminal codex (pid 20796)")),
                    ("free", None),
                ]),
                true,
                cx,
            );
            store.apply_listing(&claude, listing(&[("desktop", Some("VS Code"))]), true, cx);
        });
        cx.run_until_parked();
        let seen = notifications.get();
        store.read_with(cx, |store, _| {
            assert_eq!(
                holder(store, "codex", "desktop").as_deref(),
                Some("Codex Desktop")
            );
            assert_eq!(
                holder(store, "codex", "terminal").as_deref(),
                Some("a terminal codex (pid 20796)")
            );
            assert_eq!(holder(store, "codex", "free"), None);
            assert_eq!(
                holder(store, "claude", "desktop").as_deref(),
                Some("VS Code")
            );
        });

        // The same listing again changes nothing.
        store.update(cx, |store, cx| {
            store.apply_listing(
                &codex,
                listing(&[
                    ("desktop", Some("Codex Desktop")),
                    ("terminal", Some("a terminal codex (pid 20796)")),
                ]),
                false,
                cx,
            );
        });
        cx.run_until_parked();
        assert_eq!(notifications.get(), seen);

        // An incomplete listing updates only the sessions it lists.
        store.update(cx, |store, cx| {
            store.apply_listing(&codex, listing(&[("desktop", None)]), false, cx);
        });
        store.read_with(cx, |store, _| {
            assert_eq!(holder(store, "codex", "desktop"), None);
            assert!(holder(store, "codex", "terminal").is_some());
        });

        // A complete listing clears the sessions it no longer lists, of its agent only.
        store.update(cx, |store, cx| {
            store.apply_listing(&codex, listing(&[("free", None)]), true, cx);
        });
        store.read_with(cx, |store, _| {
            assert_eq!(holder(store, "codex", "terminal"), None);
            assert!(holder(store, "claude", "desktop").is_some());
        });

        store.update(cx, |store, cx| store.forget_source(&claude, cx));
        store.read_with(cx, |store, _| {
            assert_eq!(holder(store, "claude", "desktop"), None);
        });
    }

    #[gpui::test]
    fn test_the_same_agent_on_another_machine_keeps_its_own_marks(cx: &mut TestAppContext) {
        let store = cx.new(|_| OpenElsewhereStore::default());
        let remote_connection = RemoteConnectionOptions::Mock(MockConnectionOptions { id: 1 });
        let here = local("claude");
        let there = SessionSource::new(AgentId::new("claude"), Some(&remote_connection));
        assert_ne!(here, there);

        store.update(cx, |store, cx| {
            store.apply_listing(&here, listing(&[("s1", Some("VS Code"))]), true, cx);
            store.apply_listing(&there, listing(&[("s2", Some("Codex Desktop"))]), true, cx);
        });
        store.read_with(cx, |store, _| {
            assert_eq!(holder_of(store, &here, "s1").as_deref(), Some("VS Code"));
            assert_eq!(holder_of(store, &here, "s2"), None);
            assert_eq!(
                holder_of(store, &there, "s2").as_deref(),
                Some("Codex Desktop")
            );
        });

        // A complete listing on one machine leaves the other machine's marks alone.
        store.update(cx, |store, cx| {
            store.apply_listing(&here, listing(&[]), true, cx);
        });
        store.read_with(cx, |store, _| {
            assert_eq!(holder_of(store, &here, "s1"), None);
            assert!(holder_of(store, &there, "s2").is_some());
        });
        store.update(cx, |store, cx| store.forget_source(&there, cx));
        store.read_with(cx, |store, _| {
            assert_eq!(holder_of(store, &there, "s2"), None);
        });
    }
}
