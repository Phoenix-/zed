//! Whether the user may write in a session.
//!
//! An agent may open a session that the user can read but not write, for example a
//! thread that another app holds open. Such a thread keeps its history and stays
//! live, but takes no prompts until the agent reports that it is writable again.
//!
//! ACP has no field for this yet. Until it does, agents report it in the `_meta` of
//! the `session/load` and `session/resume` responses, of `session_info_update`
//! notifications and of `session/list` items, and [`interactivity_from_meta`] is the
//! one place that reads it. A standard field would replace that function alone.

use agent_client_protocol::schema::v2 as acp;
use gpui::SharedString;

/// Key whose value, `"full"` or `"read-only"`, is the session's interactivity. The
/// values follow the Agent Host Protocol's `ChatInteractivity`.
pub const INTERACTIVITY_META_KEY: &str = "gorynych/interactivity";
/// Key whose value tells the user why the session is read-only.
pub const INTERACTIVITY_REASON_META_KEY: &str = "gorynych/interactivityReason";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SessionInteractivity {
    /// The user can send prompts.
    #[default]
    Full,
    /// The user can read the session but not send prompts to it.
    ReadOnly {
        /// Why, in words for the user, when the agent said.
        reason: Option<SharedString>,
    },
}

impl SessionInteractivity {
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly { .. })
    }

    pub fn read_only_reason(&self) -> Option<&SharedString> {
        match self {
            Self::Full => None,
            Self::ReadOnly { reason } => reason.as_ref(),
        }
    }
}

/// The interactivity an agent reported in `meta`, if it reported one. A missing or
/// unknown value reports nothing, so that the caller decides what silence means: a
/// response that opens a session states its interactivity in full, while an update
/// leaves it as it was.
pub fn interactivity_from_meta(meta: Option<&acp::Meta>) -> Option<SessionInteractivity> {
    let meta = meta?;
    match meta.get(INTERACTIVITY_META_KEY)?.as_str()? {
        "full" => Some(SessionInteractivity::Full),
        "read-only" => {
            let reason = meta
                .get(INTERACTIVITY_REASON_META_KEY)
                .and_then(|reason| reason.as_str())
                .map(str::trim)
                .filter(|reason| !reason.is_empty())
                .map(|reason| SharedString::from(reason.to_string()));
            Some(SessionInteractivity::ReadOnly { reason })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meta(value: serde_json::Value) -> acp::Meta {
        serde_json::from_value(value).expect("meta")
    }

    #[test]
    fn test_interactivity_from_meta() {
        assert_eq!(interactivity_from_meta(None), None);
        assert_eq!(interactivity_from_meta(Some(&meta(json!({})))), None);
        assert_eq!(
            interactivity_from_meta(Some(&meta(json!({ INTERACTIVITY_META_KEY: "full" })))),
            Some(SessionInteractivity::Full)
        );
        assert_eq!(
            interactivity_from_meta(Some(&meta(json!({
                INTERACTIVITY_META_KEY: "read-only",
                INTERACTIVITY_REASON_META_KEY: " Open in Codex Desktop. ",
            })))),
            Some(SessionInteractivity::ReadOnly {
                reason: Some("Open in Codex Desktop.".into())
            })
        );
        for reason in [json!(""), json!(7)] {
            assert_eq!(
                interactivity_from_meta(Some(&meta(json!({
                    INTERACTIVITY_META_KEY: "read-only",
                    INTERACTIVITY_REASON_META_KEY: reason,
                })))),
                Some(SessionInteractivity::ReadOnly { reason: None })
            );
        }
        for value in [json!("observe"), json!(true), json!(null)] {
            assert_eq!(
                interactivity_from_meta(Some(&meta(json!({ INTERACTIVITY_META_KEY: value })))),
                None
            );
        }
    }
}
