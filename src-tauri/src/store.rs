// Incremental event store.
//
// Ingestion (this file) is the only place that touches Claude/Codex JSONL logs.
// It converts each usage/tool record into a provider/config/price-independent
// RawEvent (just the facts), reads only newly-appended bytes of changed files
// (tracked by a per-file size/mtime/offset manifest), dedupes stable event ids,
// and persists each source to its own cache. Aggregation (parser.rs) then works
// purely on these in-memory events — cheap, and recomputed per request because
// the Day/Week/Month windows are relative to "now".
use crate::model::UsageSource;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(Serialize, Deserialize, Clone)]
pub struct RawEvent {
    pub ts_ms: i64,
    pub session: String,
    pub model: String, // raw model id (price lookup), normalized later for grouping
    pub in_tok: f64,
    pub cc: f64, // cache creation
    pub cr: f64, // cache read
    pub out_tok: f64,
    pub mcp: Vec<String>,    // all mcp__<server> names called (unfiltered)
    pub skills: Vec<String>, // all Skill input.skill ids called (unfiltered)
    pub id: String,          // message id (dedup)
    // Source log file (manifest key). Lets a truncated/rewritten file purge its
    // own stale events before being re-read, so re-ingestion stays idempotent.
    #[serde(default)]
    pub source: String,
}

#[derive(Serialize, Deserialize, Default)]
struct Manifest {
    // path -> (size, mtime_ms, byte offset already ingested)
    files: HashMap<String, (u64, i64, u64)>,
    // Codex token events don't repeat the session/model on every line. Persist
    // the latest context beside each byte offset so an incremental read can
    // continue parsing without rescanning the whole file.
    #[serde(default)]
    codex_context: HashMap<String, CodexContext>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct CodexContext {
    session: String,
    model: String,
}

pub struct Store {
    pub events: Vec<RawEvent>,
    // message id -> index in `events`. A single assistant message can be split
    // across several JSONL lines (e.g. thinking on one line, tool_use on the
    // next) that all share its id; we merge their tool calls into one event and
    // count its token usage only once.
    index: HashMap<String, usize>,
    manifest: Manifest,
    source: UsageSource,
}

// Bump when the parsing/extraction logic changes in a way that requires
// re-reading logs from scratch (the incremental manifest would otherwise skip
// already-seen bytes and miss newly-extracted facts).
//   v2: count slash-command skill invocations (`/skill`), not just Skill tool_use.
//   v3: merge tool_use across lines sharing a message id (a thinking line + a
//       tool_use line were deduped, dropping the tool call).
//   v4: track a per-event source file (idempotent re-read of truncated logs).
const CLAUDE_STORE_VERSION: u32 = 4;
const CODEX_STORE_VERSION: u32 = 1;

fn store_version(source: UsageSource) -> u32 {
    match source {
        UsageSource::Claude => CLAUDE_STORE_VERSION,
        UsageSource::Codex => CODEX_STORE_VERSION,
    }
}

/// Atomically replace `path`'s contents: write a sibling temp file, then rename
/// over the target (same-volume rename is atomic on Windows and Unix). Avoids
/// the half-written/truncated JSON that a crash mid-`fs::write` would leave.
fn write_atomic(path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, data)?;
    fs::rename(&tmp, path)
}

fn logs_dir(source: UsageSource) -> Option<PathBuf> {
    match source {
        UsageSource::Claude => Some(dirs::home_dir()?.join(".claude").join("projects")),
        UsageSource::Codex => Some(crate::config::codex_home()?.join("sessions")),
    }
}

fn cache_dir(source: UsageSource) -> Option<PathBuf> {
    let root = dirs::cache_dir()?.join("tokenscope");
    // Keep Claude's established cache paths for a no-rescan upgrade. Codex has
    // its own namespace so manifests/events from the two formats never mix.
    let d = match source {
        UsageSource::Claude => root,
        UsageSource::Codex => root.join("codex"),
    };
    let _ = fs::create_dir_all(&d);
    Some(d)
}

impl Store {
    /// Load persisted events + offset manifest (empty on first run).
    pub fn load(source: UsageSource) -> Self {
        let mut events: Vec<RawEvent> = Vec::new();
        let mut manifest = Manifest::default();
        if let Some(dir) = cache_dir(source) {
            // If the cache was written by an older parser, discard it so ingest
            // does a full rescan and picks up newly-extracted facts.
            let version_ok = fs::read_to_string(dir.join("version"))
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                == Some(store_version(source));
            if version_ok {
                // events.json and offsets.json are ONE consistent unit: the
                // manifest's per-file byte offsets are only meaningful relative
                // to the events we actually loaded. If either is missing or fails
                // to parse (e.g. a crash left events.json half-written), discard
                // BOTH and fall back to a full rescan — otherwise a good manifest
                // paired with empty/corrupt events would make ingest() skip every
                // already-recorded file and silently lose all history.
                let loaded_events = fs::read_to_string(dir.join("events.json"))
                    .ok()
                    .and_then(|t| serde_json::from_str::<Vec<RawEvent>>(&t).ok());
                let loaded_manifest = fs::read_to_string(dir.join("offsets.json"))
                    .ok()
                    .and_then(|t| serde_json::from_str::<Manifest>(&t).ok());
                if let (Some(e), Some(m)) = (loaded_events, loaded_manifest) {
                    events = e;
                    manifest = m;
                }
            }
        }
        let index = events
            .iter()
            .enumerate()
            .filter(|(_, e)| !e.id.is_empty())
            .map(|(i, e)| (e.id.clone(), i))
            .collect();
        Store {
            events,
            index,
            manifest,
            source,
        }
    }

    pub fn save(&self) {
        if let Some(dir) = cache_dir(self.source) {
            // Atomic writes so a crash/kill mid-save can't leave a half-written
            // events.json (load() would then discard the pair and lose history).
            // Write events before offsets: if we crash between them, the manifest
            // is merely stale (points at fewer bytes → re-reads a little) rather
            // than ahead of the events on disk.
            if let Ok(t) = serde_json::to_string(&self.events) {
                let _ = write_atomic(&dir.join("events.json"), t.as_bytes());
            }
            if let Ok(t) = serde_json::to_string(&self.manifest) {
                let _ = write_atomic(&dir.join("offsets.json"), t.as_bytes());
            }
            let _ = write_atomic(
                &dir.join("version"),
                store_version(self.source).to_string().as_bytes(),
            );
        }
    }

    /// Rebuild the id→index map after the `events` vector is mutated wholesale
    /// (purge/prune shift positions, so partial updates aren't enough).
    fn rebuild_index(&mut self) {
        self.index = self
            .events
            .iter()
            .enumerate()
            .filter(|(_, e)| !e.id.is_empty())
            .map(|(i, e)| (e.id.clone(), i))
            .collect();
    }

    /// Drop every event that came from `key`, then rebuild the index. Used before
    /// re-reading a truncated/rewritten file so re-ingestion is idempotent
    /// (otherwise the cross-line tool_use merge re-appends calls and id-less
    /// events get pushed twice, inflating MCP/Skill counts and token totals).
    fn purge_source(&mut self, key: &str) {
        self.events.retain(|e| e.source != key);
        self.rebuild_index();
    }

    /// Drop events older than `cutoff_ms`. The reports/heatmap only span the last
    /// ~26 weeks, so anything older is dead weight that grows events.json without
    /// bound. Returns whether anything was removed. Old logs already at EOF are
    /// never re-read, so their pruned events don't reappear.
    pub fn prune_before(&mut self, cutoff_ms: i64) -> bool {
        let before = self.events.len();
        self.events.retain(|e| e.ts_ms >= cutoff_ms);
        let removed = self.events.len() != before;
        if removed {
            self.rebuild_index();
        }
        removed
    }

    /// Incrementally read only the new bytes of new/changed JSONL files.
    /// Returns whether anything changed (new events or an updated file offset),
    /// so the caller can skip a full cache rewrite when nothing moved.
    pub fn ingest(&mut self) -> bool {
        let mut dirty = false;
        let Some(root) = logs_dir(self.source) else {
            return false;
        };
        for entry in WalkDir::new(&root)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "jsonl").unwrap_or(false))
        {
            let path = entry.path();
            let key = path.to_string_lossy().to_string();
            let Ok(meta) = fs::metadata(path) else { continue };
            let size = meta.len();
            let mtime_ms = meta
                .modified()
                .ok()
                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);

            let mut offset = match self.manifest.files.get(&key).copied() {
                Some((psize, pmtime, poff)) => {
                    if psize == size && pmtime == mtime_ms {
                        continue; // unchanged → skip
                    }
                    if size < poff {
                        // truncated / rewritten (e.g. log compaction): the bytes
                        // we already ingested are gone, so purge this file's
                        // events and re-read from the start, idempotently.
                        self.purge_source(&key);
                        self.manifest.codex_context.remove(&key);
                        0
                    } else {
                        poff
                    }
                }
                None => 0,
            };

            let mut codex_context = if offset > 0 {
                self.manifest
                    .codex_context
                    .get(&key)
                    .cloned()
                    .unwrap_or_default()
            } else {
                CodexContext::default()
            };

            let Ok(mut f) = fs::File::open(path) else { continue };
            if f.seek(SeekFrom::Start(offset)).is_err() {
                continue;
            }
            let mut buf = Vec::new();
            if f.read_to_end(&mut buf).is_err() {
                continue;
            }
            // only process up to the last newline; leave a partial trailing line
            // (file still being written) for the next pass
            let process_until = match buf.iter().rposition(|&b| b == b'\n') {
                Some(i) => i + 1,
                None => 0,
            };
            for line in buf[..process_until].split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let Ok(s) = std::str::from_utf8(line) else { continue };
                let event = match self.source {
                    UsageSource::Claude => parse_claude_line(s),
                    UsageSource::Codex => parse_codex_line(s, &mut codex_context, &key),
                };
                if let Some(mut ev) = event {
                    ev.source = key.clone();
                    if !ev.id.is_empty() {
                        if let Some(&i) = self.index.get(&ev.id) {
                            // Claude can split one assistant message across
                            // multiple lines, so merge its tool calls. Codex ids
                            // are already complete event ids; a repeat is a
                            // duplicate status line and must be ignored.
                            if self.source == UsageSource::Claude {
                                let prev = &mut self.events[i];
                                prev.mcp.extend(ev.mcp);
                                prev.skills.extend(ev.skills);
                            }
                            continue;
                        }
                        self.index.insert(ev.id.clone(), self.events.len());
                    }
                    self.events.push(ev);
                }
            }
            offset += process_until as u64;
            self.manifest.files.insert(key, (size, mtime_ms, offset));
            if self.source == UsageSource::Codex {
                self.manifest
                    .codex_context
                    .insert(path.to_string_lossy().to_string(), codex_context);
            }
            dirty = true;
        }
        dirty
    }
}

/// Parse one JSONL line into a RawEvent (assistant messages only).
fn parse_claude_line(line: &str) -> Option<RawEvent> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    match v.get("type")?.as_str()? {
        "assistant" => parse_assistant(&v),
        // Skills invoked via slash command (e.g. `/find-skills`) are logged as a
        // user message with a <command-name> tag, NOT as a Skill tool_use, so
        // they need a separate path or they'd never be counted.
        "user" => parse_user_command(&v),
        _ => None,
    }
}

/// Parse one Codex rollout line. Session/model context is carried by earlier
/// records in the same file, while billable usage is emitted as
/// `event_msg/token_count`. `last_token_usage` is the single model response;
/// `total_token_usage` is used only to form a stable dedupe id because Codex can
/// append the same status event more than once.
fn parse_codex_line(
    line: &str,
    context: &mut CodexContext,
    source_path: &str,
) -> Option<RawEvent> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    let top_type = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
    let payload = v.get("payload")?;
    let payload_type = payload.get("type").and_then(|x| x.as_str()).unwrap_or("");

    match top_type {
        "session_meta" => {
            if let Some(session) = payload
                .get("id")
                .or_else(|| payload.get("session_id"))
                .and_then(|x| x.as_str())
            {
                context.session = session.to_string();
            }
            return None;
        }
        "turn_context" => {
            if let Some(model) = payload.get("model").and_then(|x| x.as_str()) {
                context.model = model.to_string();
            }
            return None;
        }
        _ => {}
    }

    let ts = v.get("timestamp")?.as_str()?;
    let ts_ms = DateTime::parse_from_rfc3339(ts).ok()?.timestamp_millis();
    let session_key = if context.session.is_empty() {
        source_path
    } else {
        &context.session
    };

    if top_type == "event_msg" && payload_type == "token_count" {
        let info = payload.get("info")?;
        let last = info.get("last_token_usage")?;
        let total = info.get("total_token_usage")?;
        let n = |value: &serde_json::Value, key: &str| -> f64 {
            value.get(key).and_then(|x| x.as_f64()).unwrap_or(0.0)
        };
        let sig = |key: &str| -> u64 {
            total.get(key).and_then(|x| x.as_u64()).unwrap_or(0)
        };
        let input_total = n(last, "input_tokens");
        let cached = n(last, "cached_input_tokens");
        let cache_write = n(last, "cache_write_input_tokens");
        let output = n(last, "output_tokens");
        // Codex writes zero-usage markers around compaction and for some child
        // session lifecycle events. They are not model requests and must not
        // create an "unknown" model row or inflate request counts.
        if input_total <= 0.0 && cached <= 0.0 && cache_write <= 0.0 && output <= 0.0 {
            return None;
        }
        // `input_tokens` includes the cached portion in Codex/OpenAI usage.
        // Keep the dashboard's buckets mutually exclusive, matching Claude's
        // uncached-input + cache + output representation.
        let uncached = (input_total - cached - cache_write).max(0.0);
        let id = format!(
            "codex-token:{session_key}:{}:{}:{}:{}:{}",
            sig("input_tokens"),
            sig("cached_input_tokens"),
            sig("cache_write_input_tokens"),
            sig("output_tokens"),
            sig("total_tokens")
        );
        return Some(RawEvent {
            ts_ms,
            session: context.session.clone(),
            model: if context.model.is_empty() {
                "unknown".to_string()
            } else {
                context.model.clone()
            },
            in_tok: uncached,
            cc: cache_write,
            cr: cached,
            out_tok: output,
            mcp: Vec::new(),
            skills: Vec::new(),
            id,
            source: String::new(),
        });
    }

    if top_type == "event_msg" && payload_type == "mcp_tool_call_end" {
        let invocation = payload.get("invocation")?;
        let server = invocation.get("server")?.as_str()?.to_string();
        if server.is_empty() {
            return None;
        }
        let call_id = payload.get("call_id").and_then(|x| x.as_str()).unwrap_or(ts);
        return Some(RawEvent {
            ts_ms,
            session: context.session.clone(),
            model: String::new(),
            in_tok: 0.0,
            cc: 0.0,
            cr: 0.0,
            out_tok: 0.0,
            mcp: vec![server],
            skills: Vec::new(),
            id: format!("codex-mcp:{session_key}:{call_id}"),
            source: String::new(),
        });
    }

    // Some Codex builds/providers expose Skill as a custom call. Current
    // built-in skills are often instruction-only and therefore leave no call
    // event; in that case we intentionally report no inferred invocation.
    if top_type == "response_item"
        && (payload_type == "custom_tool_call" || payload_type == "function_call")
        && payload.get("name").and_then(|x| x.as_str()) == Some("Skill")
    {
        let input = payload.get("input").or_else(|| payload.get("arguments"));
        let skill = input.and_then(extract_codex_skill)?;
        let call_id = payload.get("call_id").and_then(|x| x.as_str()).unwrap_or(ts);
        return Some(RawEvent {
            ts_ms,
            session: context.session.clone(),
            model: String::new(),
            in_tok: 0.0,
            cc: 0.0,
            cr: 0.0,
            out_tok: 0.0,
            mcp: Vec::new(),
            skills: vec![skill],
            id: format!("codex-skill:{session_key}:{call_id}"),
            source: String::new(),
        });
    }

    None
}

fn extract_codex_skill(input: &serde_json::Value) -> Option<String> {
    if let Some(obj) = input.as_object() {
        return obj
            .get("skill")
            .or_else(|| obj.get("name"))
            .and_then(|x| x.as_str())
            .filter(|x| !x.is_empty())
            .map(str::to_string);
    }
    let text = input.as_str()?;
    let parsed: serde_json::Value = serde_json::from_str(text).ok()?;
    extract_codex_skill(&parsed)
}

/// Extract the inner text of `<tag>...</tag>` from `s`, if present.
fn extract_tag(s: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = s.find(&open)? + open.len();
    let rest = &s[start..];
    let end = rest.find(&close)?;
    Some(rest[..end].to_string())
}

/// A user message that is a slash-command invocation of a skill, e.g.
/// `<command-name>/find-skills</command-name>`. The skill name is left
/// unfiltered here; compute_event drops non-user skills via the whitelist.
fn parse_user_command(v: &serde_json::Value) -> Option<RawEvent> {
    let text = v.get("message")?.get("content")?.as_str()?;
    let raw = extract_tag(text, "command-name")?;
    let skill = raw.trim().trim_start_matches('/').trim().to_string();
    if skill.is_empty() {
        return None;
    }
    let ts = v.get("timestamp")?.as_str()?;
    let ts_ms = DateTime::parse_from_rfc3339(ts).ok()?.timestamp_millis();
    let session = v
        .get("sessionId")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    // dedup key: the line's own uuid (command messages have no message.id)
    let id = v.get("uuid").and_then(|i| i.as_str())?.to_string();
    if id.is_empty() {
        return None;
    }
    Some(RawEvent {
        ts_ms,
        session,
        model: String::new(), // not an LLM request → no model/tokens/cost
        in_tok: 0.0,
        cc: 0.0,
        cr: 0.0,
        out_tok: 0.0,
        mcp: Vec::new(),
        skills: vec![skill],
        id,
        source: String::new(),
    })
}

fn parse_assistant(v: &serde_json::Value) -> Option<RawEvent> {
    let msg = v.get("message")?;
    let model = msg.get("model").and_then(|m| m.as_str()).unwrap_or("unknown");
    if model == "<synthetic>" {
        return None;
    }
    let ts = v.get("timestamp")?.as_str()?;
    let ts_ms = DateTime::parse_from_rfc3339(ts).ok()?.timestamp_millis();
    let session = v
        .get("sessionId")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let id = msg
        .get("id")
        .and_then(|i| i.as_str())
        .unwrap_or("")
        .to_string();

    let usage = msg.get("usage");
    let g = |k: &str| -> f64 {
        usage
            .and_then(|u| u.get(k))
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0)
    };

    let mut mcp = Vec::new();
    let mut skills = Vec::new();
    if let Some(content) = msg.get("content").and_then(|c| c.as_array()) {
        for block in content {
            if block.get("type").and_then(|t| t.as_str()) != Some("tool_use") {
                continue;
            }
            let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
            if let Some(rest) = name.strip_prefix("mcp__") {
                mcp.push(rest.split("__").next().unwrap_or("").to_string());
            } else if name == "Skill" {
                if let Some(sk) = block
                    .get("input")
                    .and_then(|i| i.get("skill"))
                    .and_then(|s| s.as_str())
                {
                    if !sk.is_empty() {
                        skills.push(sk.to_string());
                    }
                }
            }
        }
    }

    Some(RawEvent {
        ts_ms,
        session,
        model: model.to_string(),
        in_tok: g("input_tokens"),
        cc: g("cache_creation_input_tokens"),
        cr: g("cache_read_input_tokens"),
        out_tok: g("output_tokens"),
        mcp,
        skills,
        id,
        source: String::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_codex_usage_with_context_and_exclusive_cache_bucket() {
        let mut context = CodexContext::default();
        assert!(parse_codex_line(
            r#"{"type":"session_meta","payload":{"id":"session-1"}}"#,
            &mut context,
            "rollout.jsonl"
        )
        .is_none());
        assert!(parse_codex_line(
            r#"{"type":"turn_context","payload":{"model":"gpt-5-codex"}}"#,
            &mut context,
            "rollout.jsonl"
        )
        .is_none());

        let line = r#"{"timestamp":"2026-07-21T01:45:12.655Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":1000,"cached_input_tokens":400,"cache_write_input_tokens":0,"output_tokens":50,"total_tokens":1050},"total_token_usage":{"input_tokens":3000,"cached_input_tokens":800,"cache_write_input_tokens":0,"output_tokens":150,"total_tokens":3150}}}}"#;
        let event = parse_codex_line(line, &mut context, "rollout.jsonl").unwrap();
        assert_eq!(event.session, "session-1");
        assert_eq!(event.model, "gpt-5-codex");
        assert_eq!(event.in_tok, 600.0);
        assert_eq!(event.cr, 400.0);
        assert_eq!(event.out_tok, 50.0);

        let duplicate = parse_codex_line(line, &mut context, "rollout.jsonl").unwrap();
        assert_eq!(event.id, duplicate.id);
    }

    #[test]
    fn parses_codex_mcp_call_without_counting_a_model_request() {
        let mut context = CodexContext {
            session: "session-1".into(),
            model: "gpt-5-codex".into(),
        };
        let line = r#"{"timestamp":"2026-07-21T01:46:00Z","type":"event_msg","payload":{"type":"mcp_tool_call_end","call_id":"call-1","invocation":{"server":"github","tool":"search"}}}"#;
        let event = parse_codex_line(line, &mut context, "rollout.jsonl").unwrap();
        assert!(event.model.is_empty());
        assert_eq!(event.mcp, vec!["github"]);
        assert_eq!(event.id, "codex-mcp:session-1:call-1");
    }

    #[test]
    fn ignores_codex_zero_usage_lifecycle_marker() {
        let mut context = CodexContext::default();
        let line = r#"{"timestamp":"2026-07-21T01:46:00Z","type":"event_msg","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":0,"cached_input_tokens":0,"output_tokens":0,"total_tokens":1234},"total_token_usage":{"input_tokens":1000,"cached_input_tokens":400,"output_tokens":234,"total_tokens":1234}}}}"#;
        assert!(parse_codex_line(line, &mut context, "rollout.jsonl").is_none());
    }
}
