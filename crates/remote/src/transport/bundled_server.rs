//! Remote servers that ship with dev-channel builds (Gorynych fork).
//!
//! No remote server is published for a dev-channel build's commit, so such a
//! build can't download one. A build that ships servers in a `remote-server`
//! folder next to its executable uploads the one for the remote machine's
//! platform instead. On the remote machine the server is named by the build's
//! commit: a machine without the server of the build that opens it gets it, and
//! one that has it keeps using it.

use std::path::{Path, PathBuf};

use crate::RemotePlatform;

/// The folder next to the executable that holds the servers.
const BUNDLE_DIR: &str = "remote-server";
/// How much of the commit names the server on the remote machine.
const COMMIT_LEN: usize = 12;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BundledServer {
    /// The compressed server for the remote machine's platform: a `.zip` that
    /// holds `remote_server.exe` for Windows, a gzipped binary otherwise.
    pub archive: PathBuf,
    /// The server's file name on the remote machine.
    pub binary_name: String,
}

impl BundledServer {
    /// The server shipped next to the running executable for the platform, if any.
    pub async fn next_to_current_exe(platform: RemotePlatform, commit_sha: &str) -> Option<Self> {
        let exe = std::env::current_exe().ok()?;
        Self::find(platform, exe.parent()?, commit_sha).await
    }

    pub async fn find(platform: RemotePlatform, exe_dir: &Path, commit_sha: &str) -> Option<Self> {
        let commit = commit_sha.get(..COMMIT_LEN)?;
        if !commit.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        let is_windows = platform.os.is_windows();
        let archive = exe_dir.join(BUNDLE_DIR).join(format!(
            "zed-remote-server-{}-{}.{}",
            platform.os,
            platform.arch,
            if is_windows { "zip" } else { "gz" }
        ));
        let is_file = smol::fs::metadata(&archive)
            .await
            .is_ok_and(|metadata| metadata.is_file());
        is_file.then(|| Self {
            archive,
            binary_name: format!(
                "zed-remote-server-dev-{commit}{}",
                if is_windows { ".exe" } else { "" }
            ),
        })
    }

    /// The archive's extension, which tells the remote machine how to unpack it.
    pub fn archive_extension(&self) -> &str {
        self.archive
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RemoteArch, RemoteOs};

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn test_finds_the_server_for_the_remote_platform() {
        let exe_dir = tempfile::tempdir().unwrap();
        let bundle_dir = exe_dir.path().join(BUNDLE_DIR);
        std::fs::create_dir(&bundle_dir).unwrap();
        std::fs::write(
            bundle_dir.join("zed-remote-server-windows-x86_64.zip"),
            b"zip",
        )
        .unwrap();
        let windows = RemotePlatform {
            os: RemoteOs::Windows,
            arch: RemoteArch::X86_64,
        };
        let linux = RemotePlatform {
            os: RemoteOs::Linux,
            arch: RemoteArch::X86_64,
        };

        let found = smol::block_on(BundledServer::find(windows, exe_dir.path(), SHA))
            .expect("the Windows server is shipped");
        assert_eq!(
            found,
            BundledServer {
                archive: bundle_dir.join("zed-remote-server-windows-x86_64.zip"),
                binary_name: "zed-remote-server-dev-0123456789ab.exe".to_string(),
            }
        );
        assert_eq!(found.archive_extension(), "zip");

        // No server for Linux is shipped.
        assert_eq!(
            smol::block_on(BundledServer::find(linux, exe_dir.path(), SHA)),
            None
        );
        std::fs::write(bundle_dir.join("zed-remote-server-linux-x86_64.gz"), b"gz").unwrap();
        let found = smol::block_on(BundledServer::find(linux, exe_dir.path(), SHA))
            .expect("the Linux server is shipped");
        assert_eq!(found.binary_name, "zed-remote-server-dev-0123456789ab");
        assert_eq!(found.archive_extension(), "gz");

        // Without a commit, a server can't be told apart from other builds' ones.
        for commit_sha in ["", "dev", "not-a-commit-sha-at-all"] {
            assert_eq!(
                smol::block_on(BundledServer::find(windows, exe_dir.path(), commit_sha)),
                None,
                "{commit_sha}"
            );
        }
    }
}
