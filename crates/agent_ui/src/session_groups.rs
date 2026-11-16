//! Persistence belongs to the service so a cleared Zed database can recover the same groups.

use std::rc::{Rc, Weak};

use acp_thread::{AgentSessionList, SessionAttributes, SessionGroup};
use agent_client_protocol::schema::v2 as acp;
use collections::{HashMap, HashSet};
use gpui::{App, AppContext as _, Context, Entity, Global};
use remote::RemoteConnectionOptions;

use crate::{session_auto_import::SessionSource, thread_metadata_store::ThreadId};

struct GlobalSessionGroups(Entity<SessionGroups>);
impl Global for GlobalSessionGroups {}

pub fn init(cx: &mut App) {
    let store = cx.new(|_| SessionGroups::default());
    cx.set_global(GlobalSessionGroups(store));
}

#[derive(Default)]
pub struct SessionGroups {
    connections: HashMap<SessionSource, Weak<dyn AgentSessionList>>,
    attributes: HashMap<(SessionSource, acp::SessionId), SessionAttributes>,
    catalogs: HashMap<(String, String), Vec<SessionGroup>>,
    pending: HashSet<(String, String)>,
    dirty: HashSet<(String, String)>,
    pending_drafts: HashMap<ThreadId, (String, SessionAttributes)>,
}

impl SessionGroups {
    pub fn try_global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<GlobalSessionGroups>()
            .map(|store| store.0.clone())
    }

    pub fn register(
        &mut self,
        source: SessionSource,
        list: &Rc<dyn AgentSessionList>,
        cx: &mut Context<Self>,
    ) {
        if list.supports_groups()
            && list.supports_session_attributes()
            && list.service_id().is_some()
        {
            if self
                .connections
                .get(&source)
                .is_some_and(|old| Weak::ptr_eq(old, &Rc::downgrade(list)))
            {
                return;
            }
            let projects = self
                .catalogs
                .keys()
                .filter(|(id, _)| Some(id.as_str()) == list.service_id())
                .map(|(_, project)| project.clone())
                .collect::<Vec<_>>();
            self.connections.insert(source, Rc::downgrade(list));
            for project in projects {
                self.refresh(list.clone(), project, cx);
            }
            cx.notify();
        }
    }

    pub fn list_for_source(&self, source: &SessionSource) -> Option<Rc<dyn AgentSessionList>> {
        self.connections.get(source)?.upgrade()
    }

    pub fn list_for_service(&self, service_id: &str) -> Option<Rc<dyn AgentSessionList>> {
        self.connections
            .values()
            .filter_map(Weak::upgrade)
            .find(|list| list.service_id() == Some(service_id))
    }

    pub fn source_for_service(
        &self,
        service_id: &str,
        host: Option<&RemoteConnectionOptions>,
    ) -> Option<SessionSource> {
        self.connections
            .iter()
            .filter_map(|(source, list)| {
                let list = list.upgrade()?;
                (source.is_on_host(host) && list.service_id() == Some(service_id))
                    .then(|| source.clone())
            })
            .min_by(|left, right| left.agent_id.as_ref().cmp(right.agent_id.as_ref()))
    }

    pub fn set_pending_draft(
        &mut self,
        thread_id: ThreadId,
        service_id: String,
        attributes: SessionAttributes,
    ) {
        self.pending_drafts
            .insert(thread_id, (service_id, attributes));
    }

    pub fn pending_draft(&self, thread_id: ThreadId) -> Option<(String, SessionAttributes)> {
        self.pending_drafts.get(&thread_id).cloned()
    }

    pub fn finish_pending_draft(&mut self, thread_id: ThreadId) {
        self.pending_drafts.remove(&thread_id);
    }

    pub fn apply_group(
        &mut self,
        service_id: &str,
        project_key: &str,
        group: SessionGroup,
        cx: &mut Context<Self>,
    ) {
        let key = (service_id.to_owned(), project_key.to_owned());
        if self.pending.contains(&key) {
            self.dirty.insert(key.clone());
        }
        let groups = self.catalogs.entry(key).or_default();
        groups.retain(|existing| existing.group_id != group.group_id);
        groups.push(group);
        groups.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then(left.group_id.cmp(&right.group_id))
        });
        cx.notify();
    }

    pub fn attributes(
        &self,
        source: &SessionSource,
        session_id: &acp::SessionId,
    ) -> Option<&SessionAttributes> {
        self.attributes.get(&(source.clone(), session_id.clone()))
    }

    pub fn set_attributes(
        &mut self,
        source: &SessionSource,
        session_id: acp::SessionId,
        attributes: SessionAttributes,
        cx: &mut Context<Self>,
    ) {
        let key = (source.clone(), session_id);
        if self.attributes.get(&key) != Some(&attributes) {
            self.attributes.insert(key, attributes);
            cx.notify();
        }
    }

    pub fn set_initial_attributes(
        &mut self,
        source: &SessionSource,
        session_id: acp::SessionId,
        attributes: SessionAttributes,
        cx: &mut Context<Self>,
    ) {
        if !self
            .attributes
            .contains_key(&(source.clone(), session_id.clone()))
        {
            self.set_attributes(source, session_id, attributes, cx);
        }
    }

    pub fn apply_listing(
        &mut self,
        source: &SessionSource,
        entries: impl IntoIterator<Item = (acp::SessionId, SessionAttributes)>,
        complete: bool,
        live_sessions: &HashSet<acp::SessionId>,
        cx: &mut Context<Self>,
    ) {
        let mut seen = HashSet::default();
        for (session_id, attributes) in entries {
            seen.insert(session_id.clone());
            self.set_attributes(source, session_id, attributes, cx);
        }
        if complete {
            let length = self.attributes.len();
            self.attributes.retain(|(entry_source, session_id), _| {
                entry_source != source
                    || seen.contains(session_id)
                    || live_sessions.contains(session_id)
            });
            if length != self.attributes.len() {
                cx.notify();
            }
        }
    }

    pub fn remove_group(
        &mut self,
        service_id: &str,
        project_key: &str,
        group_id: &str,
        cx: &mut Context<Self>,
    ) {
        let key = (service_id.to_owned(), project_key.to_owned());
        if self.pending.contains(&key) {
            self.dirty.insert(key.clone());
        }
        if let Some(groups) = self.catalogs.get_mut(&key) {
            groups.retain(|group| group.group_id != group_id);
        }
        self.remove_memberships(service_id, project_key, group_id);
        cx.notify();
    }

    fn remove_memberships(&mut self, service_id: &str, project_key: &str, group_id: &str) {
        let sources = self
            .connections
            .iter()
            .filter_map(|(source, list)| {
                list.upgrade()
                    .filter(|list| list.service_id() == Some(service_id))
                    .map(|_| source.clone())
            })
            .collect::<HashSet<_>>();
        let matches = |attributes: &SessionAttributes| {
            attributes
                .get("gorynych/group")
                .and_then(|value| value.as_object())
                .is_some_and(|group| {
                    group.get("projectKey").and_then(|value| value.as_str()) == Some(project_key)
                        && group.get("groupId").and_then(|value| value.as_str()) == Some(group_id)
                })
        };
        for ((source, _), attributes) in &mut self.attributes {
            if sources.contains(source) && matches(attributes) {
                attributes.remove("gorynych/group");
            }
        }
        for (pending_service, attributes) in self.pending_drafts.values_mut() {
            if pending_service == service_id && matches(attributes) {
                attributes.remove("gorynych/group");
            }
        }
    }

    pub fn catalog(&self, service_id: &str, project_key: &str) -> Option<&[SessionGroup]> {
        self.catalogs
            .get(&(service_id.to_owned(), project_key.to_owned()))
            .map(Vec::as_slice)
    }

    pub fn lists_for_host(
        &self,
        host: Option<&RemoteConnectionOptions>,
    ) -> Vec<Rc<dyn AgentSessionList>> {
        let mut seen = HashSet::default();
        self.connections
            .iter()
            .filter(|(source, _)| source.is_on_host(host))
            .filter_map(|(_, list)| list.upgrade())
            .filter(|list| {
                list.service_id()
                    .is_some_and(|id| seen.insert(id.to_owned()))
            })
            .collect()
    }

    pub fn ensure_project(
        &mut self,
        host: Option<&RemoteConnectionOptions>,
        project_key: &str,
        cx: &mut Context<Self>,
    ) {
        for list in self.lists_for_host(host) {
            if list.service_id().is_some_and(|id| {
                !self
                    .catalogs
                    .contains_key(&(id.to_owned(), project_key.to_owned()))
            }) {
                self.refresh(list, project_key.to_owned(), cx);
            }
        }
    }

    pub fn refresh(
        &mut self,
        list: Rc<dyn AgentSessionList>,
        project_key: String,
        cx: &mut Context<Self>,
    ) {
        let Some(service_id) = list.service_id().map(str::to_owned) else {
            return;
        };
        let key = (service_id, project_key.clone());
        if !self.pending.insert(key.clone()) {
            self.dirty.insert(key);
            return;
        }
        let task = list.list_groups(&project_key, cx);
        cx.spawn(async move |this, cx| {
            let result = task.await;
            this.update(cx, |this, cx| {
                this.pending.remove(&key);
                if this.dirty.remove(&key) {
                    let latest = this.list_for_service(&key.0).unwrap_or(list);
                    this.refresh(latest, project_key, cx);
                    return;
                }
                match result {
                    Ok(mut groups) => {
                        groups.sort_by(|left, right| {
                            left.name
                                .cmp(&right.name)
                                .then(left.group_id.cmp(&right.group_id))
                        });
                        let removed = this
                            .catalogs
                            .get(&key)
                            .map(|previous| {
                                previous
                                    .iter()
                                    .filter(|old| {
                                        !groups.iter().any(|new| new.group_id == old.group_id)
                                    })
                                    .map(|group| group.group_id.clone())
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        for group_id in removed {
                            this.remove_memberships(&key.0, &key.1, &group_id);
                        }
                        this.catalogs.insert(key, groups);
                        cx.notify();
                    }
                    Err(error) => log::warn!("Failed to refresh session groups: {error:#}"),
                }
            })
            .ok();
        })
        .detach();
    }

    pub fn refresh_project(&mut self, project_key: &str, cx: &mut Context<Self>) {
        let lists = self
            .connections
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        for list in lists {
            self.refresh(list, project_key.to_owned(), cx);
        }
    }
}

pub fn attributes_from_meta(meta: Option<&acp::Meta>) -> Option<SessionAttributes> {
    meta?.get("gorynych/attributes")?.as_object().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use acp_thread::{AgentSessionListRequest, AgentSessionListResponse};
    use anyhow::Result;
    use gpui::Task;
    use project::AgentId;
    use std::any::Any;

    struct TestList;
    impl AgentSessionList for TestList {
        fn list_sessions(
            &self,
            _: AgentSessionListRequest,
            _: &mut App,
        ) -> Task<Result<AgentSessionListResponse>> {
            Task::ready(Ok(AgentSessionListResponse::new(Vec::new())))
        }
        fn supports_session_attributes(&self) -> bool {
            true
        }
        fn supports_groups(&self) -> bool {
            true
        }
        fn service_id(&self) -> Option<&str> {
            Some("service")
        }
        fn into_any(self: Rc<Self>) -> Rc<dyn Any> {
            self
        }
    }

    struct AuthoritativeList;
    impl AgentSessionList for AuthoritativeList {
        fn list_sessions(
            &self,
            _: AgentSessionListRequest,
            _: &mut App,
        ) -> Task<Result<AgentSessionListResponse>> {
            Task::ready(Ok(AgentSessionListResponse::new(Vec::new())))
        }
        fn supports_session_attributes(&self) -> bool {
            true
        }
        fn supports_groups(&self) -> bool {
            true
        }
        fn service_id(&self) -> Option<&str> {
            Some("service")
        }
        fn list_groups(&self, _: &str, _: &mut App) -> Task<Result<Vec<SessionGroup>>> {
            Task::ready(Ok(vec![SessionGroup {
                group_id: "group".into(),
                name: "newest".into(),
            }]))
        }
        fn into_any(self: Rc<Self>) -> Rc<dyn Any> {
            self
        }
    }

    #[gpui::test]
    fn test_refresh_after_late_mutation_response_restores_newer_catalog(
        cx: &mut gpui::TestAppContext,
    ) {
        let store = cx.new(|_| SessionGroups::default());
        let list: Rc<dyn AgentSessionList> = Rc::new(AuthoritativeList);
        store.update(cx, |store, cx| {
            store.apply_group(
                "service",
                "project",
                SessionGroup {
                    group_id: "group".into(),
                    name: "newest".into(),
                },
                cx,
            );
            // A mutation reply can arrive after that newer catalog snapshot.
            store.apply_group(
                "service",
                "project",
                SessionGroup {
                    group_id: "group".into(),
                    name: "stale".into(),
                },
                cx,
            );
            store.refresh(list, "project".into(), cx);
        });
        cx.run_until_parked();
        store.read_with(cx, |store, _| {
            assert_eq!(
                store.catalog("service", "project").expect("catalog")[0].name,
                "newest"
            )
        });
    }

    #[gpui::test]
    fn test_same_connection_keeps_catalog_and_live_draft_attributes(cx: &mut gpui::TestAppContext) {
        let store = cx.new(|_| SessionGroups::default());
        let list: Rc<dyn AgentSessionList> = Rc::new(TestList);
        let source = SessionSource::local(AgentId::new("agent"));
        let session_id = acp::SessionId::new("empty-draft");
        let attributes = serde_json::Map::from_iter([(
            "gorynych/group".into(),
            serde_json::json!({"projectKey":"project","groupId":"group"}),
        )]);
        store.update(cx, |store, cx| {
            store.register(source.clone(), &list, cx);
            store.apply_group(
                "service",
                "project",
                SessionGroup {
                    group_id: "group".into(),
                    name: "Empty".into(),
                },
                cx,
            );
            store.set_initial_attributes(&source, session_id.clone(), attributes.clone(), cx);
            store.register(source.clone(), &list, cx);
            store.apply_listing(
                &source,
                Vec::new(),
                true,
                &HashSet::from_iter([session_id.clone()]),
                cx,
            );
            assert_eq!(store.catalog("service", "project").map(<[_]>::len), Some(1));
            assert_eq!(store.attributes(&source, &session_id), Some(&attributes));
            let latest =
                serde_json::Map::from_iter([("gorynych/group".into(), serde_json::Value::Null)]);
            store.set_attributes(&source, session_id.clone(), latest.clone(), cx);
            store.set_initial_attributes(&source, session_id.clone(), attributes.clone(), cx);
            assert_eq!(store.attributes(&source, &session_id), Some(&latest));
            let other_session = acp::SessionId::new("other-project");
            let other_attributes = serde_json::Map::from_iter([(
                "gorynych/group".into(),
                serde_json::json!({"projectKey":"other","groupId":"group"}),
            )]);
            store.set_attributes(&source, session_id.clone(), attributes.clone(), cx);
            store.set_attributes(&source, other_session.clone(), other_attributes.clone(), cx);
            store.remove_group("service", "project", "group", cx);
            assert_eq!(
                store.attributes(&source, &session_id),
                Some(&SessionAttributes::new())
            );
            assert_eq!(
                store.attributes(&source, &other_session),
                Some(&other_attributes)
            );
            let replacement: Rc<dyn AgentSessionList> = Rc::new(TestList);
            store.register(source.clone(), &replacement, cx);
            assert_eq!(store.catalog("service", "project").map(<[_]>::len), Some(0));
        });
    }
}
