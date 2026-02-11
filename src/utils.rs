use once_cell::sync::Lazy;
use regex::Regex;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

static ARXIV_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d+\.\d+(v\d+)?$").expect("valid arxiv id regex"));

pub fn strip_version(idstr: &str) -> String {
    idstr.split('v').next().unwrap_or(idstr).to_string()
}

pub fn normalize_arxiv_id(pid: &str) -> String {
    let trimmed = pid.trim();
    if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("arxiv:") {
        trimmed[6..].trim().to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn parse_arxiv_id_list(input: &str) -> Vec<String> {
    input
        .split(';')
        .map(normalize_arxiv_id)
        .filter(|id| !id.is_empty())
        .collect()
}


pub fn is_valid_id(pid: &str) -> bool {
    ARXIV_ID_RE.is_match(pid)
}

pub fn write_atomic<F>(path: &Path, write_fn: F) -> Result<(), String>
where
    F: FnOnce(&mut NamedTempFile) -> Result<(), String>,
{
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temp = NamedTempFile::new_in(parent)
        .map_err(|err| format!("Failed to create temp file in {parent:?}: {err}"))?;
    write_fn(&mut temp)?;
    temp.flush()
        .map_err(|err| format!("Failed to flush {}: {err}", path.display()))?;
    temp.persist(path)
        .map_err(|err| format!("Failed to persist {}: {err}", path.display()))?;
    Ok(())
}

pub fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), String> {
    write_atomic(path, |file| {
        file.write_all(bytes)
            .map_err(|err| format!("Failed to write {}: {err}", path.display()))
    })
}

pub fn ensure_parent_dir(path: &Path) -> Result<Option<PathBuf>, String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("Failed to create directory {parent:?}: {err}"))?;
            return Ok(Some(parent.to_path_buf()));
        }
    }
    Ok(None)
}
