//! Marks threads whose sessions are open in another app.
//!
//! Some agents can't share a session between processes, and report the app
//! that has a session open (a desktop app, an editor, a terminal). Such a thread
//! shows a muted title, says where it is open under it, and explains on hover
//! what to do. Clicking it still opens it: the agent then answers with an error
//! that says the same.

use agent_ui::{open_elsewhere_store::OpenElsewhereStore, thread_metadata_store::ThreadMetadata};
use gpui::{AnyElement, App, Context, SharedString};
use ui::{Tooltip, prelude::*};

use crate::Sidebar;

const MAX_HOLDER_LEN: usize = 32;

/// The app that has the thread's session open, as its agent last listed it.
pub(crate) fn holder(metadata: &ThreadMetadata, cx: &App) -> Option<SharedString> {
    let session_id = metadata.session_id.as_ref()?;
    OpenElsewhereStore::try_global(cx)?
        .read(cx)
        .holder(&metadata.agent_id, session_id)
        .cloned()
}

/// The line under a thread's title: where its parent is, and where else the
/// thread is open.
pub(crate) fn caption(
    parent_hint: Option<SharedString>,
    holder: Option<&SharedString>,
) -> Option<SharedString> {
    let open_elsewhere = holder.map(|holder| {
        format!(
            "open in {}",
            util::truncate_and_trailoff(holder, MAX_HOLDER_LEN)
        )
    });
    match (parent_hint, open_elsewhere) {
        (Some(parent_hint), Some(open_elsewhere)) => {
            Some(format!("{parent_hint} • {open_elsewhere}").into())
        }
        (Some(parent_hint), None) => Some(parent_hint),
        (None, Some(open_elsewhere)) => Some(open_elsewhere.into()),
        (None, None) => None,
    }
}

impl Sidebar {
    pub(crate) fn observe_open_elsewhere(cx: &mut Context<Self>) {
        if let Some(store) = OpenElsewhereStore::try_global(cx) {
            cx.observe(&store, |_this, _store, cx| cx.notify()).detach();
        }
    }

    /// Explains a thread open in another app when the pointer rests on its row.
    pub(crate) fn render_open_elsewhere_tooltip(
        &self,
        ix: usize,
        metadata: &ThreadMetadata,
        row: AnyElement,
        cx: &App,
    ) -> AnyElement {
        let Some(holder) = holder(metadata, cx) else {
            return row;
        };
        div()
            .id(("thread-open-elsewhere", ix))
            .w_full()
            .tooltip(Tooltip::text(format!(
                "This session is open in {holder}; close it there to continue it here."
            )))
            .child(row)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_caption() {
        let desktop = SharedString::from("Codex Desktop");
        assert_eq!(caption(None, None), None);
        assert_eq!(
            caption(Some("from Coordinator".into()), None).as_deref(),
            Some("from Coordinator")
        );
        assert_eq!(
            caption(None, Some(&desktop)).as_deref(),
            Some("open in Codex Desktop")
        );
        assert_eq!(
            caption(Some("from Coordinator".into()), Some(&desktop)).as_deref(),
            Some("from Coordinator • open in Codex Desktop")
        );
        let long =
            SharedString::from("a terminal codex started from a very long folder (pid 20796)");
        let caption = caption(None, Some(&long)).expect("caption");
        assert!(caption.starts_with("open in a terminal codex"), "{caption}");
        assert!(caption.ends_with('…'), "{caption}");
    }
}
