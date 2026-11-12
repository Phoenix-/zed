//! Nests threads under the threads that spawned them.
//!
//! An agent may report that a session was started by another session (for
//! example, a coordinator that delegates to coders). Such child threads are
//! ordinary threads, but the sidebar shows them indented under their parent,
//! and a parent can be collapsed to hide its children.

use std::collections::{HashMap, HashSet};

use agent_client_protocol::schema::v2 as acp;
use agent_ui::{
    ThreadId, session_auto_import::SessionSource, thread_metadata_store::ThreadMetadataStore,
    thread_parent_store::ThreadParentStore,
};
use chrono::{DateTime, Utc};
use gpui::{AnyElement, App, Context, Pixels, SharedString, px};
use project::AgentId;
use ui::{Disclosure, prelude::*};

use crate::{DraftKind, ListEntry, Sidebar};

const INDENT_PER_LEVEL: Pixels = px(12.);
const DISCLOSURE_WIDTH: Pixels = px(16.);
const MAX_PARENT_TITLE_LEN: usize = 32;

/// How a thread row sits in its project group's thread tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ThreadTreeRow {
    pub depth: usize,
    pub child_count: usize,
    pub collapsed: bool,
    /// Whether any thread in the group is nested, so that every row in it
    /// leaves room for a disclosure.
    pub in_tree_group: bool,
    /// For a thread whose parent isn't shown above it.
    pub parent_hint: Option<SharedString>,
}

pub(crate) struct TreeNode {
    pub agent_id: Option<AgentId>,
    pub session_id: Option<acp::SessionId>,
    pub parent_session_id: Option<acp::SessionId>,
    pub sort_time: DateTime<Utc>,
    pub collapsed: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TreePlacement {
    pub index: usize,
    pub depth: usize,
    pub child_count: usize,
    /// The node names a parent that isn't one of the nodes.
    pub detached: bool,
}

/// Finds the node a node names as its parent. Sessions of different agents may
/// share an id, and a parent may belong to another agent than its child: a
/// parent of the child's agent wins, and another agent's node is accepted only
/// when it is the only one with that id.
fn resolve_parent(
    index: usize,
    nodes: &[TreeNode],
    index_by_session: &HashMap<&acp::SessionId, Vec<usize>>,
) -> Option<usize> {
    let node = &nodes[index];
    let candidates = index_by_session
        .get(node.parent_session_id.as_ref()?)?
        .iter()
        .copied()
        .filter(|candidate| *candidate != index)
        .collect::<Vec<_>>();
    candidates
        .iter()
        .copied()
        .find(|candidate| nodes[*candidate].agent_id == node.agent_id)
        .or(match candidates.as_slice() {
            [only] => Some(*only),
            _ => None,
        })
}

/// Orders `nodes` so that each one follows its parent, most recent first among
/// siblings. A subtree is as recent as its most recent node. Descendants of
/// collapsed nodes are left out.
pub(crate) fn arrange(nodes: &[TreeNode]) -> Vec<TreePlacement> {
    let mut index_by_session = HashMap::<&acp::SessionId, Vec<usize>>::new();
    for (index, node) in nodes.iter().enumerate() {
        if let Some(session_id) = node.session_id.as_ref() {
            index_by_session.entry(session_id).or_default().push(index);
        }
    }
    let mut parents = (0..nodes.len())
        .map(|index| resolve_parent(index, nodes, &index_by_session))
        .collect::<Vec<_>>();

    // Agents shouldn't report cycles, but one must not hide its threads.
    for start in 0..nodes.len() {
        let mut visited = HashSet::from([start]);
        let mut current = start;
        while let Some(parent) = parents[current] {
            if !visited.insert(parent) {
                parents[current] = None;
                break;
            }
            current = parent;
        }
    }

    let mut children = vec![Vec::new(); nodes.len()];
    let mut roots = Vec::new();
    for (index, parent) in parents.iter().enumerate() {
        match parent {
            Some(parent) => children[*parent].push(index),
            None => roots.push(index),
        }
    }

    let mut subtree_times = vec![None; nodes.len()];
    for index in 0..nodes.len() {
        subtree_time(index, nodes, &children, &mut subtree_times);
    }
    let by_recency = |a: &usize, b: &usize| {
        subtree_times[*b]
            .cmp(&subtree_times[*a])
            .then_with(|| a.cmp(b))
    };
    roots.sort_by(by_recency);
    for siblings in &mut children {
        siblings.sort_by(by_recency);
    }

    let mut placements = Vec::with_capacity(nodes.len());
    let mut stack = roots
        .into_iter()
        .rev()
        .map(|index| (index, 0))
        .collect::<Vec<_>>();
    while let Some((index, depth)) = stack.pop() {
        placements.push(TreePlacement {
            index,
            depth,
            child_count: children[index].len(),
            detached: nodes[index].parent_session_id.is_some() && parents[index].is_none(),
        });
        // Like a collapsed project group, a collapsed thread hides all of its
        // descendants, the active thread included.
        if !nodes[index].collapsed {
            stack.extend(
                children[index]
                    .iter()
                    .rev()
                    .map(|child| (*child, depth + 1)),
            );
        }
    }
    placements
}

fn subtree_time(
    index: usize,
    nodes: &[TreeNode],
    children: &[Vec<usize>],
    subtree_times: &mut Vec<Option<DateTime<Utc>>>,
) -> DateTime<Utc> {
    if let Some(time) = subtree_times[index] {
        return time;
    }
    let mut time = nodes[index].sort_time;
    for child in &children[index] {
        time = time.max(subtree_time(*child, nodes, children, subtree_times));
    }
    subtree_times[index] = Some(time);
    time
}

/// Nests the rows of one project group, pushed to `entries` from `group_start`
/// on, and records how each thread sits in the tree.
pub(crate) fn nest_group_rows(
    entries: &mut Vec<ListEntry>,
    group_start: usize,
    collapsed_threads: &HashSet<ThreadId>,
    tree_rows: &mut HashMap<ThreadId, ThreadTreeRow>,
    cx: &App,
) {
    let Some(parent_store) = ThreadParentStore::try_global(cx) else {
        return;
    };
    let parent_store = parent_store.read(cx);
    let parent_of = |entry: &ListEntry| match entry {
        ListEntry::Thread(thread) => thread
            .metadata
            .session_id
            .as_ref()
            .and_then(|session_id| {
                parent_store
                    .parent_session_id(&SessionSource::of_thread(&thread.metadata), session_id)
            })
            .cloned(),
        ListEntry::Terminal(_) | ListEntry::ProjectHeader { .. } => None,
    };
    if !entries[group_start..]
        .iter()
        .any(|entry| parent_of(entry).is_some())
    {
        return;
    }

    let rows = entries.split_off(group_start);
    let nodes = rows
        .iter()
        .map(|entry| match entry {
            ListEntry::Thread(thread) => TreeNode {
                agent_id: Some(thread.metadata.agent_id.clone()),
                session_id: thread.metadata.session_id.clone(),
                parent_session_id: parent_of(entry),
                sort_time: if thread.draft == Some(DraftKind::Empty) {
                    DateTime::<Utc>::MAX_UTC
                } else {
                    Sidebar::thread_display_time(&thread.metadata)
                },
                collapsed: collapsed_threads.contains(&thread.metadata.thread_id),
            },
            ListEntry::Terminal(terminal) => TreeNode {
                agent_id: None,
                session_id: None,
                parent_session_id: None,
                sort_time: terminal.metadata.created_at,
                collapsed: false,
            },
            ListEntry::ProjectHeader { .. } => TreeNode {
                agent_id: None,
                session_id: None,
                parent_session_id: None,
                sort_time: DateTime::<Utc>::MAX_UTC,
                collapsed: false,
            },
        })
        .collect::<Vec<_>>();
    let placements = arrange(&nodes);
    // A collapsed parent shows no children but still needs its disclosure.
    let in_tree_group = placements
        .iter()
        .any(|placement| placement.depth > 0 || placement.child_count > 0);

    let metadata_store = ThreadMetadataStore::global(cx).read(cx);
    let mut rows = rows.into_iter().map(Some).collect::<Vec<_>>();
    for placement in placements {
        let Some(entry) = rows[placement.index].take() else {
            continue;
        };
        if let ListEntry::Thread(thread) = &entry {
            let parent_hint = placement
                .detached
                .then(|| nodes[placement.index].parent_session_id.as_ref())
                .flatten()
                .map(|parent| {
                    parent_hint(parent_title(
                        metadata_store,
                        &thread.metadata.agent_id,
                        parent,
                    ))
                });
            if in_tree_group || parent_hint.is_some() {
                tree_rows.insert(
                    thread.metadata.thread_id,
                    ThreadTreeRow {
                        depth: placement.depth,
                        child_count: placement.child_count,
                        collapsed: nodes[placement.index].collapsed,
                        in_tree_group,
                        parent_hint,
                    },
                );
            }
        }
        entries.push(entry);
    }
}

/// Looks the parent up among all threads, by the same rule as `resolve_parent`,
/// and returns its title and whether it is archived.
fn parent_title(
    metadata_store: &ThreadMetadataStore,
    agent_id: &AgentId,
    parent_session_id: &acp::SessionId,
) -> Option<(SharedString, bool)> {
    let candidates = metadata_store
        .entries()
        .filter(|thread| thread.session_id.as_ref() == Some(parent_session_id))
        .collect::<Vec<_>>();
    candidates
        .iter()
        .find(|thread| &thread.agent_id == agent_id)
        .or(match candidates.as_slice() {
            [only] => Some(only),
            _ => None,
        })
        .map(|thread| (thread.display_title(), thread.archived))
}

/// Whether two entries, from different builds of the list, show the same row.
fn is_same_row(a: &ListEntry, b: &ListEntry) -> bool {
    match (a, b) {
        (ListEntry::ProjectHeader { key: a, .. }, ListEntry::ProjectHeader { key: b, .. }) => {
            a == b
        }
        (ListEntry::Thread(a), ListEntry::Thread(b)) => {
            a.metadata.thread_id == b.metadata.thread_id
        }
        (ListEntry::Terminal(a), ListEntry::Terminal(b)) => {
            a.metadata.terminal_id == b.metadata.terminal_id
        }
        _ => false,
    }
}

/// An archived parent isn't in the sidebar, so the hint says where it went.
fn parent_hint(parent: Option<(SharedString, bool)>) -> SharedString {
    match parent {
        Some((title, archived)) => {
            let title = util::truncate_and_trailoff(&title, MAX_PARENT_TITLE_LEN);
            if archived {
                format!("from {title} (archived)").into()
            } else {
                format!("from {title}").into()
            }
        }
        None => "from an unknown thread".into(),
    }
}

impl Sidebar {
    pub(crate) fn thread_parent_hint(&self, thread_id: &ThreadId) -> Option<SharedString> {
        self.contents
            .thread_tree
            .get(thread_id)
            .and_then(|row| row.parent_hint.clone())
    }

    pub(crate) fn toggle_thread_tree(&mut self, thread_id: ThreadId, cx: &mut Context<Self>) {
        if !self.collapsed_thread_trees.remove(&thread_id) {
            self.collapsed_thread_trees.insert(thread_id);
        }
        let selected = self
            .selection
            .and_then(|ix| self.contents.entries.get(ix))
            .cloned();
        self.update_entries(cx);
        // The rows below the toggled thread move. The selection stays on its
        // row, or moves to the toggled thread when that row is now hidden.
        if let Some(selected) = selected {
            let entries = &self.contents.entries;
            let is_toggled_thread = |entry: &ListEntry| match entry {
                ListEntry::Thread(thread) => thread.metadata.thread_id == thread_id,
                ListEntry::Terminal(_) | ListEntry::ProjectHeader { .. } => false,
            };
            self.selection = entries
                .iter()
                .position(|entry| is_same_row(entry, &selected))
                .or_else(|| entries.iter().position(is_toggled_thread));
        }
    }

    fn selected_thread_tree_row(&self, ix: usize) -> Option<(ThreadId, ThreadTreeRow)> {
        let ListEntry::Thread(thread) = self.contents.entries.get(ix)? else {
            return None;
        };
        let thread_id = thread.metadata.thread_id;
        let row = self.contents.thread_tree.get(&thread_id)?.clone();
        Some((thread_id, row))
    }

    fn select_entry(&mut self, ix: usize, cx: &mut Context<Self>) {
        self.selection = Some(ix);
        self.list_state.scroll_to_reveal_item(ix);
        cx.notify();
    }

    /// Expands a collapsed parent thread, or moves to the first child of an
    /// expanded one. Returns whether the selected row is such a parent.
    pub(crate) fn expand_selected_thread_tree(
        &mut self,
        ix: usize,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some((thread_id, row)) = self.selected_thread_tree_row(ix) else {
            return false;
        };
        if row.child_count == 0 {
            return false;
        }
        if row.collapsed {
            self.toggle_thread_tree(thread_id, cx);
        } else if ix + 1 < self.contents.entries.len() {
            self.select_entry(ix + 1, cx);
        }
        true
    }

    /// Collapses an expanded parent thread, or moves from a child thread to its
    /// parent. Returns whether either applied.
    pub(crate) fn collapse_selected_thread_tree(
        &mut self,
        ix: usize,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some((thread_id, row)) = self.selected_thread_tree_row(ix) else {
            return false;
        };
        if row.child_count > 0 && !row.collapsed {
            self.toggle_thread_tree(thread_id, cx);
            return true;
        }
        if row.depth == 0 {
            return false;
        }
        let parent_ix = (0..ix).rev().find(|candidate| {
            self.selected_thread_tree_row(*candidate)
                .is_some_and(|(_, candidate)| candidate.depth < row.depth)
        });
        match parent_ix {
            Some(parent_ix) => {
                self.select_entry(parent_ix, cx);
                true
            }
            None => false,
        }
    }

    pub(crate) fn render_thread_tree_row(
        &self,
        ix: usize,
        thread_id: ThreadId,
        row: AnyElement,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(tree) = self
            .contents
            .thread_tree
            .get(&thread_id)
            .filter(|tree| tree.in_tree_group)
        else {
            return row;
        };
        let disclosure = (tree.child_count > 0).then(|| {
            Disclosure::new(("thread-tree-disclosure", ix), !tree.collapsed).on_click(cx.listener(
                move |this, _, _window, cx| {
                    cx.stop_propagation();
                    this.toggle_thread_tree(thread_id, cx);
                },
            ))
        });
        h_flex()
            .w_full()
            .items_start()
            .child(
                // Lines the disclosure up with the row's title rather than its
                // middle, matching the row's own vertical padding and title height.
                h_flex()
                    .flex_none()
                    .justify_end()
                    .mt_1()
                    .h_6()
                    .w(DISCLOSURE_WIDTH + INDENT_PER_LEVEL * tree.depth as f32)
                    .children(disclosure),
            )
            .child(div().flex_1().min_w_0().child(row))
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(session: &str, parent: Option<&str>, minute: u32) -> TreeNode {
        agent_node("agent", session, parent, minute)
    }

    fn agent_node(agent: &str, session: &str, parent: Option<&str>, minute: u32) -> TreeNode {
        TreeNode {
            agent_id: Some(AgentId::new(agent)),
            session_id: Some(acp::SessionId::new(session)),
            parent_session_id: parent.map(acp::SessionId::new),
            sort_time: DateTime::parse_from_rfc3339(&format!("2026-11-07T10:{minute:02}:00Z"))
                .expect("timestamp")
                .with_timezone(&Utc),
            collapsed: false,
        }
    }

    fn outline(nodes: &[TreeNode]) -> Vec<String> {
        arrange(nodes)
            .into_iter()
            .map(|placement| {
                let node = &nodes[placement.index];
                let mut line = format!(
                    "{}{}",
                    "  ".repeat(placement.depth),
                    node.session_id.as_ref().map_or("-", |id| id.0.as_ref())
                );
                if placement.child_count > 0 {
                    line.push_str(&format!(" ({})", placement.child_count));
                }
                if placement.detached {
                    line.push_str(" [detached]");
                }
                line
            })
            .collect()
    }

    #[test]
    fn test_nests_children_newest_first() {
        let nodes = vec![
            node("solo", None, 50),
            node("coder-1", Some("coordinator"), 20),
            node("coordinator", None, 10),
            node("coder-2", Some("coordinator"), 30),
            node("reviewer", Some("coder-1"), 25),
        ];
        assert_eq!(
            outline(&nodes),
            [
                "solo",
                "coordinator (2)",
                "  coder-2",
                "  coder-1 (1)",
                "    reviewer",
            ]
        );
    }

    #[test]
    fn test_subtrees_are_as_recent_as_their_newest_thread() {
        let nodes = vec![
            node("recent", None, 40),
            node("coordinator", None, 10),
            node("busy-child", Some("coordinator"), 55),
        ];
        assert_eq!(
            outline(&nodes),
            ["coordinator (1)", "  busy-child", "recent"]
        );
    }

    #[test]
    fn test_threads_with_missing_parents_stay_at_the_top_level() {
        let nodes = vec![
            node("orphan", Some("elsewhere"), 30),
            node("plain", None, 20),
        ];
        assert_eq!(outline(&nodes), ["orphan [detached]", "plain"]);
    }

    #[test]
    fn test_collapsed_threads_hide_their_descendants() {
        let mut nodes = vec![
            node("coordinator", None, 10),
            node("coder", Some("coordinator"), 20),
            node("reviewer", Some("coder"), 30),
            node("other", None, 5),
        ];
        nodes[0].collapsed = true;
        assert_eq!(outline(&nodes), ["coordinator (1)", "other"]);
        nodes[0].collapsed = false;
        nodes[1].collapsed = true;
        assert_eq!(outline(&nodes), ["coordinator (1)", "  coder (1)", "other"]);
    }

    #[test]
    fn test_parents_of_other_agents_resolve_when_unambiguous() {
        // A Kimi coder started by a Claude coordinator.
        let nodes = vec![
            agent_node("claude", "coordinator", None, 10),
            agent_node("kimi", "coder", Some("coordinator"), 20),
        ];
        assert_eq!(outline(&nodes), ["coordinator (1)", "  coder"]);

        // With two candidates, the child's own agent wins.
        let nodes = vec![
            agent_node("claude", "s1", None, 10),
            agent_node("kimi", "s1", None, 11),
            agent_node("kimi", "coder", Some("s1"), 20),
        ];
        assert_eq!(outline(&nodes), ["s1 (1)", "  coder", "s1"]);
        assert_eq!(arrange(&nodes)[0].index, 1);

        // Two candidates of other agents are ambiguous.
        let nodes = vec![
            agent_node("claude", "s1", None, 10),
            agent_node("codex", "s1", None, 11),
            agent_node("kimi", "coder", Some("s1"), 20),
        ];
        assert_eq!(outline(&nodes), ["coder [detached]", "s1", "s1"]);
    }

    #[test]
    fn test_cycles_do_not_hide_threads() {
        let nodes = vec![
            node("a", Some("c"), 10),
            node("b", Some("a"), 20),
            node("c", Some("b"), 30),
            node("self", Some("self"), 5),
        ];
        let placements = arrange(&nodes);
        assert_eq!(placements.len(), nodes.len());
        assert_eq!(
            placements
                .iter()
                .filter(|placement| placement.depth == 0)
                .count(),
            2
        );
    }

    #[test]
    fn test_parent_hint() {
        assert_eq!(
            parent_hint(Some(("Coordinator".into(), false))).as_ref(),
            "from Coordinator"
        );
        assert_eq!(
            parent_hint(Some(("Coordinator".into(), true))).as_ref(),
            "from Coordinator (archived)"
        );
        assert_eq!(parent_hint(None).as_ref(), "from an unknown thread");
    }
}
