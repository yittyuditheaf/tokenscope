// Loads the "user-installed" whitelists so the dashboard only counts
// MCP servers / Skills the user actually added (PRD decision).
use crate::model::UsageSource;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

pub struct UserConfig {
    pub mcp_servers: HashSet<String>,
    pub skills: HashSet<String>,
}

fn home() -> Option<PathBuf> {
    dirs::home_dir()
}

pub(crate) fn codex_home() -> Option<PathBuf> {
    std::env::var_os("CODEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| home().map(|h| h.join(".codex")))
}

/// Parse ~/.claude.json once (None if missing/unreadable/invalid).
fn read_claude_config() -> Option<serde_json::Value> {
    let path = home()?.join(".claude.json");
    let text = fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

/// mcpServers (top level) + projects[*].mcpServers from a parsed ~/.claude.json.
fn mcps_from(json: Option<&serde_json::Value>) -> HashSet<String> {
    let mut set = HashSet::new();
    let Some(json) = json else { return set };
    if let Some(obj) = json.get("mcpServers").and_then(|v| v.as_object()) {
        for k in obj.keys() {
            set.insert(k.clone());
        }
    }
    if let Some(projects) = json.get("projects").and_then(|v| v.as_object()) {
        for proj in projects.values() {
            if let Some(obj) = proj.get("mcpServers").and_then(|v| v.as_object()) {
                for k in obj.keys() {
                    set.insert(k.clone());
                }
            }
        }
    }
    set
}

/// Add each subdirectory name of `dir` to the set (skills are folders).
fn scan_skill_dir(dir: &Path, set: &mut HashSet<String>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            if e.path().is_dir() {
                if let Some(name) = e.file_name().to_str() {
                    set.insert(name.to_string());
                }
            }
        }
    }
}

/// User-installed skills = global ~/.claude/skills/ only (PRD §3.3). Project-
/// level skill dirs are intentionally not scanned: the PRD defines the skill
/// source as the global directory, and folding in every registered project's
/// dir inflated the "installed skills" metric.
fn load_claude_skills() -> HashSet<String> {
    let mut set = HashSet::new();
    if let Some(h) = home() {
        scan_skill_dir(&h.join(".claude").join("skills"), &mut set);
    }
    set
}

/// Codex MCP servers are the keys below `[mcp_servers]` in
/// `~/.codex/config.toml`. Parsing as TOML handles quoted server names and
/// nested per-server tables without relying on fragile line matching.
fn load_codex_mcps() -> HashSet<String> {
    let Some(path) = codex_home().map(|h| h.join("config.toml")) else {
        return HashSet::new();
    };
    let Ok(text) = fs::read_to_string(path) else {
        return HashSet::new();
    };
    let Ok(value) = text.parse::<toml::Value>() else {
        return HashSet::new();
    };
    value
        .get("mcp_servers")
        .and_then(|v| v.as_table())
        .map(|table| table.keys().cloned().collect())
        .unwrap_or_default()
}

/// Personal Codex skills live directly below `~/.codex/skills`. Hidden
/// directories such as `.system` are bundled/runtime content, not user skills.
fn load_codex_skills() -> HashSet<String> {
    let mut set = HashSet::new();
    let Some(dir) = codex_home().map(|h| h.join("skills")) else {
        return set;
    };
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if !entry.path().is_dir() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                if !name.starts_with('.') {
                    set.insert(name.to_string());
                }
            }
        }
    }
    set
}

impl UserConfig {
    pub fn load(source: UsageSource) -> Self {
        match source {
            UsageSource::Claude => {
                // Parse ~/.claude.json a single time and derive the MCP whitelist from it.
                let json = read_claude_config();
                UserConfig {
                    mcp_servers: mcps_from(json.as_ref()),
                    skills: load_claude_skills(),
                }
            }
            UsageSource::Codex => UserConfig {
                mcp_servers: load_codex_mcps(),
                skills: load_codex_skills(),
            },
        }
    }

    /// A tool name like "mcp__<server>__<tool>" → is server user-installed?
    pub fn is_user_mcp(&self, server: &str) -> bool {
        self.mcp_servers.contains(server)
    }

    /// A skill id (may be "plugin:skill") → strip plugin prefix, check dir.
    pub fn is_user_skill(&self, skill: &str) -> bool {
        let key = skill.rsplit(':').next().unwrap_or(skill);
        self.skills.contains(key)
    }
}
