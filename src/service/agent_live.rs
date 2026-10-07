//! HTTP-owned Elasticsearch PIT/scroll cursors and bounded bulk execution.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use reqwest::{Client, Method};
use serde_json::{Value, json};

use super::http;
use crate::config::EsConfig;

const MAX_SOURCE_BYTES: usize = 64 * 1024;
const MAX_CONTINUATION_BYTES: usize = 1024;
const MAX_BULK_BODY_BYTES: usize = 1024 * 1024;
const MAX_SHARD_FAILURES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EsSearchMode {
    Pit,
    Scroll,
}

impl EsSearchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pit => "pit",
            Self::Scroll => "scroll",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EsSearchHit {
    pub index: String,
    pub id: String,
    pub source: Value,
    pub source_omitted: bool,
    pub continuation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EsShardFailure {
    pub index: Option<String>,
    pub shard: Option<String>,
    pub status: Option<String>,
    pub error_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EsSearchPage {
    pub hits: Vec<EsSearchHit>,
    pub shard_failures: Vec<EsShardFailure>,
    pub failed_shards: u64,
    pub exhausted: bool,
}

pub struct EsSearchSource {
    client: Client,
    config: EsConfig,
    index: String,
    mode: EsSearchMode,
    query: Value,
    sort: Value,
    batch_size: usize,
    keep_alive_ms: u64,
    handle: String,
    search_after: Option<Value>,
    initial_scroll: bool,
    exhausted: bool,
}

impl EsSearchSource {
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: &EsConfig,
        index: String,
        mode: EsSearchMode,
        query: Value,
        sort: Value,
        batch_size: usize,
        keep_alive_ms: u64,
        resume_from: Option<&str>,
    ) -> Result<Self, EsLiveError> {
        http::sanitize_path_component(&index).map_err(|_| EsLiveError::fatal("index_invalid"))?;
        if mode == EsSearchMode::Scroll && resume_from.is_some() {
            return Err(EsLiveError::fatal("scroll_resume_unsupported"));
        }
        let client =
            http::create_client(config).map_err(|_| EsLiveError::fatal("http_client_invalid"))?;
        let search_after = resume_from
            .map(|value| decode_continuation(value, mode))
            .transpose()?;
        let keep_alive = keep_alive(keep_alive_ms);
        let (handle, initial_scroll) = match mode {
            EsSearchMode::Pit => {
                let path = format!("/{index}/_pit?keep_alive={keep_alive}");
                let response = request_json(&client, config, Method::POST, &path, None).await?;
                let handle = response
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| EsLiveError::fatal("pit_open_response_invalid"))?
                    .to_string();
                (handle, false)
            }
            EsSearchMode::Scroll => (String::new(), true),
        };
        Ok(Self {
            client,
            config: config.clone(),
            index,
            mode,
            query,
            sort,
            batch_size,
            keep_alive_ms,
            handle,
            search_after,
            initial_scroll,
            exhausted: false,
        })
    }

    pub fn mode(&self) -> EsSearchMode {
        self.mode
    }

    pub fn exhausted(&self) -> bool {
        self.exhausted
    }

    pub async fn reconnect(&mut self, resume_from: Option<&str>) -> Result<(), EsLiveError> {
        if self.mode != EsSearchMode::Pit {
            return Err(EsLiveError::fatal("scroll_reconnect_unsupported"));
        }
        let _ = self.close_handle().await;
        let replacement = Self::open(
            &self.config,
            self.index.clone(),
            self.mode,
            self.query.clone(),
            self.sort.clone(),
            self.batch_size,
            self.keep_alive_ms,
            resume_from,
        )
        .await?;
        *self = replacement;
        Ok(())
    }

    pub async fn read(&mut self, count: usize) -> Result<EsSearchPage, EsLiveError> {
        if self.exhausted {
            return Ok(EsSearchPage {
                hits: Vec::new(),
                shard_failures: Vec::new(),
                failed_shards: 0,
                exhausted: true,
            });
        }
        let size = count.min(self.batch_size);
        let keep_alive = keep_alive(self.keep_alive_ms);
        let response = match self.mode {
            EsSearchMode::Pit => {
                let mut body = json!({
                    "size": size,
                    "query": self.query,
                    "sort": self.sort,
                    "pit": { "id": self.handle, "keep_alive": keep_alive },
                    "track_total_hits": false
                });
                if let Some(search_after) = &self.search_after {
                    body["search_after"] = search_after.clone();
                }
                request_json(
                    &self.client,
                    &self.config,
                    Method::POST,
                    "/_search",
                    Some(&body),
                )
                .await?
            }
            EsSearchMode::Scroll if self.initial_scroll => {
                let path = format!("/{}/_search?scroll={keep_alive}", self.index);
                let body = json!({
                    "size": size,
                    "query": self.query,
                    "sort": self.sort
                });
                self.initial_scroll = false;
                request_json(&self.client, &self.config, Method::POST, &path, Some(&body)).await?
            }
            EsSearchMode::Scroll => {
                let body = json!({ "scroll": keep_alive, "scroll_id": self.handle });
                request_json(
                    &self.client,
                    &self.config,
                    Method::POST,
                    "/_search/scroll",
                    Some(&body),
                )
                .await?
            }
        };

        if let Some(next_handle) = response
            .get(if self.mode == EsSearchMode::Pit {
                "pit_id"
            } else {
                "_scroll_id"
            })
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            self.handle = next_handle.to_string();
        }
        let raw_hits = response
            .pointer("/hits/hits")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut hits = Vec::with_capacity(raw_hits.len());
        for hit in raw_hits {
            let sort = hit.get("sort").cloned();
            if let Some(sort) = &sort {
                self.search_after = Some(sort.clone());
            }
            let continuation = if self.mode == EsSearchMode::Pit {
                sort.as_ref()
                    .map(|sort| encode_continuation(self.mode, sort, self.keep_alive_ms))
                    .transpose()?
            } else {
                None
            };
            let source = hit.get("_source").cloned().unwrap_or(Value::Null);
            let (source, source_omitted) = bounded_source(source);
            hits.push(EsSearchHit {
                index: hit
                    .get("_index")
                    .and_then(Value::as_str)
                    .unwrap_or(&self.index)
                    .to_string(),
                id: hit
                    .get("_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source,
                source_omitted,
                continuation,
            });
        }
        self.exhausted = hits.is_empty();
        let failed_shards = response
            .pointer("/_shards/failed")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let shard_failures = response
            .pointer("/_shards/failures")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .take(MAX_SHARD_FAILURES)
            .map(|failure| EsShardFailure {
                index: failure
                    .get("index")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                shard: failure.get("shard").map(|value| value.to_string()),
                status: failure
                    .get("status")
                    .map(|value| value.to_string().trim_matches('"').to_string()),
                error_type: failure
                    .pointer("/reason/type")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
            .collect();
        Ok(EsSearchPage {
            hits,
            shard_failures,
            failed_shards,
            exhausted: self.exhausted,
        })
    }

    pub async fn close(&mut self) -> Result<(), EsLiveError> {
        self.exhausted = true;
        self.close_handle().await
    }

    async fn close_handle(&mut self) -> Result<(), EsLiveError> {
        if self.handle.is_empty() {
            return Ok(());
        }
        let (path, body) = match self.mode {
            EsSearchMode::Pit => ("/_pit", json!({ "id": self.handle })),
            EsSearchMode::Scroll => (
                "/_search/scroll",
                json!({ "scroll_id": [self.handle.clone()] }),
            ),
        };
        request_json(
            &self.client,
            &self.config,
            Method::DELETE,
            path,
            Some(&body),
        )
        .await
        .map(|_| ())
    }
}

#[derive(Debug, Clone)]
pub enum EsBulkOperation {
    Index { id: Option<String>, document: Value },
    Create { id: String, document: Value },
    Update { id: String, document: Value },
    Delete { id: String },
}

impl EsBulkOperation {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Index { .. } => "index",
            Self::Create { .. } => "create",
            Self::Update { .. } => "update",
            Self::Delete { .. } => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EsBulkItemResult {
    pub index: usize,
    pub operation: String,
    pub id: Option<String>,
    pub status: u16,
    pub ok: bool,
    pub error_type: Option<String>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EsBulkResult {
    pub items: Vec<EsBulkItemResult>,
    pub successful: usize,
    pub failed: usize,
}

pub async fn execute_bulk(
    config: &EsConfig,
    index: &str,
    operations: &[EsBulkOperation],
) -> Result<EsBulkResult, EsLiveError> {
    http::sanitize_path_component(index).map_err(|_| EsLiveError::fatal("index_invalid"))?;
    let mut ndjson = String::new();
    for operation in operations {
        match operation {
            EsBulkOperation::Index { id, document } => {
                ndjson.push_str(&bulk_action("index", index, id.as_deref())?);
                ndjson.push('\n');
                ndjson.push_str(&json_line(document)?);
                ndjson.push('\n');
            }
            EsBulkOperation::Create { id, document } => {
                ndjson.push_str(&bulk_action("create", index, Some(id))?);
                ndjson.push('\n');
                ndjson.push_str(&json_line(document)?);
                ndjson.push('\n');
            }
            EsBulkOperation::Update { id, document } => {
                ndjson.push_str(&bulk_action("update", index, Some(id))?);
                ndjson.push('\n');
                ndjson.push_str(&json_line(&json!({ "doc": document }))?);
                ndjson.push('\n');
            }
            EsBulkOperation::Delete { id } => {
                ndjson.push_str(&bulk_action("delete", index, Some(id))?);
                ndjson.push('\n');
            }
        }
    }
    if ndjson.len() > MAX_BULK_BODY_BYTES {
        return Err(EsLiveError::fatal("bulk_body_too_large"));
    }
    let client =
        http::create_client(config).map_err(|_| EsLiveError::fatal("http_client_invalid"))?;
    let path = "/_bulk?filter_path=errors,items.*._id,items.*.status,items.*.error.type";
    let url = format!("{}{}", config.primary_url().trim_end_matches('/'), path);
    let response = http::apply_auth(client.post(url), config)
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .map_err(|_| EsLiveError::retryable("bulk_request_failed"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(EsLiveError::from_status(
            status.as_u16(),
            "bulk_http_failed",
        ));
    }
    let body: Value = response
        .json()
        .await
        .map_err(|_| EsLiveError::fatal("bulk_response_invalid"))?;
    let raw_items = body
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| EsLiveError::fatal("bulk_response_invalid"))?;
    let items = raw_items
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let (operation, item) = value
                .as_object()
                .and_then(|map| map.iter().next())
                .map(|(operation, item)| (operation.clone(), item))
                .unwrap_or_else(|| ("unknown".into(), &Value::Null));
            let status = item
                .get("status")
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok())
                .unwrap_or(500);
            EsBulkItemResult {
                index,
                operation,
                id: item.get("_id").and_then(Value::as_str).map(str::to_string),
                status,
                ok: (200..300).contains(&status),
                error_type: item
                    .pointer("/error/type")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                retryable: matches!(status, 429 | 502 | 503 | 504),
            }
        })
        .collect::<Vec<_>>();
    let successful = items.iter().filter(|item| item.ok).count();
    Ok(EsBulkResult {
        failed: items.len().saturating_sub(successful),
        items,
        successful,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EsLiveError {
    pub code: &'static str,
    pub retryable: bool,
}

impl EsLiveError {
    fn fatal(code: &'static str) -> Self {
        Self {
            code,
            retryable: false,
        }
    }

    fn retryable(code: &'static str) -> Self {
        Self {
            code,
            retryable: true,
        }
    }

    fn from_status(status: u16, default_code: &'static str) -> Self {
        if matches!(status, 429 | 502 | 503 | 504) {
            Self::retryable("target_temporarily_unavailable")
        } else {
            Self::fatal(default_code)
        }
    }
}

async fn request_json(
    client: &Client,
    config: &EsConfig,
    method: Method,
    path: &str,
    body: Option<&Value>,
) -> Result<Value, EsLiveError> {
    let url = format!("{}{}", config.primary_url().trim_end_matches('/'), path);
    let mut request = http::apply_auth(client.request(method, url), config);
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request
        .send()
        .await
        .map_err(|_| EsLiveError::retryable("target_request_failed"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(EsLiveError::from_status(
            status.as_u16(),
            "target_request_rejected",
        ));
    }
    response
        .json()
        .await
        .map_err(|_| EsLiveError::fatal("target_response_invalid"))
}

fn bounded_source(source: Value) -> (Value, bool) {
    if serde_json::to_vec(&source)
        .map(|encoded| encoded.len() <= MAX_SOURCE_BYTES)
        .unwrap_or(false)
    {
        (source, false)
    } else {
        (Value::Null, true)
    }
}

fn keep_alive(milliseconds: u64) -> String {
    format!("{milliseconds}ms")
}

fn encode_continuation(
    mode: EsSearchMode,
    sort: &Value,
    keep_alive_ms: u64,
) -> Result<String, EsLiveError> {
    let expires_at_ms = now_millis().saturating_add(keep_alive_ms);
    let bytes = serde_json::to_vec(&json!({
        "v": 1,
        "mode": mode.as_str(),
        "sort": sort,
        "expires_at_ms": expires_at_ms
    }))
    .map_err(|_| EsLiveError::fatal("continuation_encode_failed"))?;
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    if encoded.len() > MAX_CONTINUATION_BYTES {
        return Err(EsLiveError::fatal("continuation_too_large"));
    }
    Ok(encoded)
}

fn decode_continuation(value: &str, mode: EsSearchMode) -> Result<Value, EsLiveError> {
    if value.is_empty() || value.len() > MAX_CONTINUATION_BYTES {
        return Err(EsLiveError::fatal("continuation_invalid"));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| EsLiveError::fatal("continuation_invalid"))?;
    let token: Value =
        serde_json::from_slice(&bytes).map_err(|_| EsLiveError::fatal("continuation_invalid"))?;
    if token.get("v").and_then(Value::as_u64) != Some(1)
        || token.get("mode").and_then(Value::as_str) != Some(mode.as_str())
        || token
            .get("expires_at_ms")
            .and_then(Value::as_u64)
            .is_none_or(|expires| expires <= now_millis())
    {
        return Err(EsLiveError::fatal("continuation_expired_or_invalid"));
    }
    token
        .get("sort")
        .cloned()
        .filter(Value::is_array)
        .ok_or_else(|| EsLiveError::fatal("continuation_invalid"))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn bulk_action(operation: &str, index: &str, id: Option<&str>) -> Result<String, EsLiveError> {
    let mut metadata = serde_json::Map::new();
    metadata.insert("_index".into(), Value::String(index.to_string()));
    if let Some(id) = id {
        if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
            return Err(EsLiveError::fatal("bulk_document_id_invalid"));
        }
        metadata.insert("_id".into(), Value::String(id.to_string()));
    }
    let mut action = serde_json::Map::new();
    action.insert(operation.to_string(), Value::Object(metadata));
    json_line(&Value::Object(action))
}

fn json_line(value: &Value) -> Result<String, EsLiveError> {
    serde_json::to_string(value).map_err(|_| EsLiveError::fatal("bulk_payload_invalid"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuation_is_opaque_bounded_and_mode_bound() {
        let encoded = encode_continuation(EsSearchMode::Pit, &json!([42, "id"]), 60_000)
            .expect("continuation");
        assert!(encoded.len() <= MAX_CONTINUATION_BYTES);
        assert_eq!(
            decode_continuation(&encoded, EsSearchMode::Pit).unwrap(),
            json!([42, "id"])
        );
        assert!(decode_continuation(&encoded, EsSearchMode::Scroll).is_err());
    }

    #[test]
    fn expired_and_malformed_continuations_fail_closed() {
        let expired = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "v": 1,
                "mode": "pit",
                "sort": [42],
                "expires_at_ms": now_millis().saturating_sub(1)
            }))
            .unwrap(),
        );
        let error = decode_continuation(&expired, EsSearchMode::Pit)
            .expect_err("expired continuation must fail");
        assert_eq!(error.code, "continuation_expired_or_invalid");
        assert!(!error.retryable);

        let oversized = "x".repeat(MAX_CONTINUATION_BYTES + 1);
        for malformed in ["", "not-base64", oversized.as_str()] {
            assert!(decode_continuation(malformed, EsSearchMode::Pit).is_err());
        }
    }

    #[test]
    fn transient_statuses_are_distinguished_from_terminal_failures() {
        for status in [429, 502, 503, 504] {
            let error = EsLiveError::from_status(status, "terminal");
            assert!(error.retryable, "status {status}");
            assert_eq!(error.code, "target_temporarily_unavailable");
        }
        let terminal = EsLiveError::from_status(400, "terminal");
        assert!(!terminal.retryable);
        assert_eq!(terminal.code, "terminal");
    }

    #[test]
    fn bulk_ndjson_actions_do_not_interpolate_ids() {
        let line = bulk_action("delete", "events", Some("a\"b")).expect("action");
        let action: Value = serde_json::from_str(&line).expect("valid action JSON");
        assert_eq!(action["delete"]["_index"], "events");
        assert_eq!(action["delete"]["_id"], "a\"b");
        assert!(!line.contains("a\"b"));
    }
}
