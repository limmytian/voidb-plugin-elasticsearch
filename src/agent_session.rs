//! Bounded Elasticsearch PIT and scroll search sessions.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use voidb_core::{
    AGENT_LIVE_SESSION_PROTOCOL_VERSION, AgentLiveSessionAuditIdentity,
    AgentLiveSessionBackpressureMode, AgentLiveSessionBufferOverflow, AgentLiveSessionBufferPolicy,
    AgentLiveSessionCallCancellation, AgentLiveSessionCancelBehavior, AgentLiveSessionCloseEffect,
    AgentLiveSessionContract, AgentLiveSessionControlPolicy, AgentLiveSessionCursor,
    AgentLiveSessionCursorKind, AgentLiveSessionCursorScopePolicy, AgentLiveSessionDeliveryPolicy,
    AgentLiveSessionEventInput, AgentLiveSessionEventKind, AgentLiveSessionHeartbeatPolicy,
    AgentLiveSessionKind, AgentLiveSessionOperations, AgentLiveSessionReadRequest,
    AgentLiveSessionReconnectMode, AgentLiveSessionReconnectPolicy,
    AgentLiveSessionResourceDescriptor, AgentLiveSessionResumeMode,
    AgentLiveSessionSourcePacedState, AgentLiveSessionStartRequest, AgentSessionCallRequest,
    AgentSessionCallResult, AgentSessionOpenContext, CapabilityRiskLevel, PluginAgentSession,
    PluginAgentSessionFactory, PluginSessionError, PluginSessionErrorCode, PluginSessionHealth,
    PluginSessionPurpose, RedactionStatus, RedactionTarget, agent_live_session_cursor_scope,
    collect_redaction_targets, redact_text_with_targets,
};

use crate::EsConfig;
use crate::service::agent_live::{EsSearchHit, EsSearchMode, EsSearchSource, EsShardFailure};

pub(crate) const SEARCH_STREAM_READ_CAPABILITY: &str = "elasticsearch.search_stream_read";

const DEFAULT_BATCH_SIZE: usize = 100;
const MAX_BATCH_SIZE: usize = 500;
const DEFAULT_KEEP_ALIVE_MS: u64 = 60_000;
const MIN_KEEP_ALIVE_MS: u64 = 1_000;
const MAX_KEEP_ALIVE_MS: u64 = 300_000;
const MAX_READ_WAIT_MS: u64 = 30_000;

pub struct EsAgentSessionFactory {
    config: EsConfig,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

impl EsAgentSessionFactory {
    pub fn new(config: EsConfig) -> Self {
        let redaction_targets = serde_json::to_value(&config)
            .map(|value| collect_redaction_targets(&value))
            .unwrap_or_default();
        Self {
            config,
            redaction_targets: Arc::new(redaction_targets),
        }
    }
}

#[async_trait]
impl PluginAgentSessionFactory for EsAgentSessionFactory {
    fn plugin_id(&self) -> &str {
        "elasticsearch"
    }

    async fn open(
        &self,
        context: AgentSessionOpenContext,
    ) -> Result<Arc<dyn PluginAgentSession>, PluginSessionError> {
        let actual = context
            .binding
            .allowed_capabilities
            .iter()
            .map(|capability| {
                capability
                    .strip_prefix("elasticsearch.")
                    .unwrap_or(capability)
            })
            .collect::<HashSet<_>>();
        if actual != HashSet::from(["search_stream_read"]) {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "An Elasticsearch live session requires the complete search-stream family only.",
            ));
        }
        let (purpose, contract) =
            elasticsearch_live_session_contract(SEARCH_STREAM_READ_CAPABILITY)
                .expect("Elasticsearch search-stream contract");
        if context.binding.purpose != purpose {
            return Err(error(
                PluginSessionErrorCode::BindingMismatch,
                "The Elasticsearch live-session purpose does not match its capability family.",
            ));
        }
        contract.validate(&context.binding.allowed_capabilities)?;
        contract.validate_start(&context.request.input)?;
        let start: AgentLiveSessionStartRequest =
            serde_json::from_value(context.request.input.clone()).map_err(|_| {
                error(
                    PluginSessionErrorCode::PolicyDenied,
                    "The Elasticsearch live-session start envelope is invalid.",
                )
            })?;
        let index = required_string(&start.resource, "index", 255)?;
        let mode = search_mode(&start.parameters)?;
        if mode == EsSearchMode::Scroll && start.resume_from.is_some() {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "Elasticsearch scroll IDs are never resumable across session generations.",
            ));
        }
        let query = start
            .parameters
            .get("query")
            .cloned()
            .unwrap_or_else(|| json!({ "match_all": {} }));
        if !query.is_object() {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "Elasticsearch search query must be an object.",
            ));
        }
        let sort = start
            .parameters
            .get("sort")
            .cloned()
            .unwrap_or_else(|| match mode {
                EsSearchMode::Pit => json!(["_shard_doc"]),
                EsSearchMode::Scroll => json!(["_doc"]),
            });
        if !sort.is_array() {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "Elasticsearch search sort must be an array.",
            ));
        }
        let scope = agent_live_session_cursor_scope(
            SEARCH_STREAM_READ_CAPABILITY,
            &json!({
                "resource": start.resource.clone(),
                "mode": mode.as_str(),
                "query": query.clone(),
                "sort": sort.clone()
            }),
        )?;
        if start
            .resume_from
            .as_ref()
            .and_then(|cursor| cursor.scope.as_deref())
            .is_some_and(|supplied| supplied != scope)
        {
            return Err(error(
                PluginSessionErrorCode::BindingMismatch,
                "The Elasticsearch resume cursor belongs to a different index, query, or sort.",
            ));
        }
        let batch_size = parameter_usize(
            &start.parameters,
            "batch_size",
            DEFAULT_BATCH_SIZE,
            1,
            MAX_BATCH_SIZE,
        )?;
        let keep_alive_ms = parameter_u64(
            &start.parameters,
            "keep_alive_ms",
            DEFAULT_KEEP_ALIVE_MS,
            MIN_KEEP_ALIVE_MS,
            MAX_KEEP_ALIVE_MS,
        )?;
        let resume_from = start
            .resume_from
            .as_ref()
            .map(|cursor| cursor.value.as_str());
        let source = EsSearchSource::open(
            &self.config,
            index,
            mode,
            query,
            sort,
            batch_size,
            keep_alive_ms,
            resume_from,
        )
        .await
        .map_err(|_| owner_error("Elasticsearch search cursor could not be opened."))?;

        Ok(Arc::new(EsAgentLiveSession {
            contract,
            state: Mutex::new(Some(EsSearchSession {
                source,
                pending: VecDeque::new(),
                delivery: AgentLiveSessionSourcePacedState::default(),
                scope,
                batch_size,
                redaction_targets: Arc::clone(&self.redaction_targets),
            })),
            cancellations: AgentLiveSessionCallCancellation::default(),
            closed: AtomicBool::new(false),
        }))
    }
}

enum EsPendingEvent {
    Hit(EsSearchHit),
    ShardFailure {
        failed_shards: u64,
        failures: Vec<EsShardFailure>,
    },
}

struct EsSearchSession {
    source: EsSearchSource,
    pending: VecDeque<EsPendingEvent>,
    delivery: AgentLiveSessionSourcePacedState,
    scope: String,
    batch_size: usize,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

struct EsAgentLiveSession {
    contract: AgentLiveSessionContract,
    state: Mutex<Option<EsSearchSession>>,
    cancellations: AgentLiveSessionCallCancellation,
    closed: AtomicBool,
}

#[async_trait]
impl PluginAgentSession for EsAgentLiveSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if request.capability != SEARCH_STREAM_READ_CAPABILITY {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "The Elasticsearch call is outside this live-session binding.",
            ));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(owner_error("The Elasticsearch live session is closed."));
        }
        let mut read = read_request(&request)?;
        read.max_bytes = read.max_bytes.min(request.output_limit_bytes);
        let call_id = request.call_id.clone();
        let batch = self
            .cancellations
            .run(&call_id, self.read_search(read))
            .await?;
        let output = serde_json::to_value(batch).map_err(|_| {
            error(
                PluginSessionErrorCode::RedactionFailed,
                "The Elasticsearch live-session batch could not be serialized.",
            )
        })?;
        AgentSessionCallResult::bounded(request.call_id, output, request.output_limit_bytes)
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.closed.load(Ordering::Acquire) {
            PluginSessionHealth::Closed
        } else {
            PluginSessionHealth::Ready
        })
    }

    async fn cancel(&self, call_id: &str) -> Result<(), PluginSessionError> {
        self.cancellations.cancel(call_id).await;
        self.close_source().await?;
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.cancellations.close().await;
        self.close_source().await
    }
}

impl EsAgentLiveSession {
    async fn read_search(
        &self,
        read: AgentLiveSessionReadRequest,
    ) -> Result<voidb_core::AgentLiveSessionEventBatch, PluginSessionError> {
        self.contract.validate_read(&read)?;
        let mut guard = self.state.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| owner_error("The Elasticsearch search cursor is closed."))?;
        if state.pending.is_empty() && !state.source.exhausted() {
            let count = state.batch_size.min(read.max_events);
            let page =
                match state.source.read(count).await {
                    Ok(page) => page,
                    Err(live_error)
                        if live_error.retryable && state.source.mode() == EsSearchMode::Pit =>
                    {
                        state.delivery.record_reconnect(&self.contract)?;
                        let resume = state
                            .delivery
                            .latest_cursor()
                            .map(|cursor| cursor.value.clone());
                        state
                            .source
                            .reconnect(resume.as_deref())
                            .await
                            .map_err(|_| owner_error("Elasticsearch PIT resume failed."))?;
                        state.source.read(count).await.map_err(|_| {
                            owner_error("Elasticsearch PIT read failed after resume.")
                        })?
                    }
                    Err(_) => return Err(owner_error("Elasticsearch search cursor read failed.")),
                };
            state
                .pending
                .extend(page.hits.into_iter().map(EsPendingEvent::Hit));
            if page.failed_shards > 0 {
                state.pending.push_back(EsPendingEvent::ShardFailure {
                    failed_shards: page.failed_shards,
                    failures: page.shard_failures,
                });
            }
        }
        let candidates = state
            .pending
            .iter()
            .map(|event| search_event(event, &state.scope, &state.redaction_targets))
            .collect::<Vec<_>>();
        let source_closed = state.source.exhausted();
        let (batch, consumed) = state.delivery.build_batch(
            &read,
            &self.contract,
            &candidates,
            candidates.is_empty() && !source_closed,
            source_closed,
        )?;
        state.pending.drain(..consumed.min(state.pending.len()));
        Ok(batch)
    }

    async fn close_source(&self) -> Result<(), PluginSessionError> {
        let mut guard = self.state.lock().await;
        if let Some(mut state) = guard.take() {
            state
                .source
                .close()
                .await
                .map_err(|_| owner_error("Elasticsearch cursor cleanup could not be verified."))?;
        }
        Ok(())
    }
}

pub(crate) fn elasticsearch_live_session_contract(
    capability: &str,
) -> Option<(PluginSessionPurpose, AgentLiveSessionContract)> {
    if capability != SEARCH_STREAM_READ_CAPABILITY {
        return None;
    }
    Some((
        PluginSessionPurpose::DatabaseQuery,
        AgentLiveSessionContract {
            protocol_version: AGENT_LIVE_SESSION_PROTOCOL_VERSION,
            kind: AgentLiveSessionKind::Cursor,
            resource: AgentLiveSessionResourceDescriptor {
                resource_type: "elasticsearch_index_search".into(),
                identity_schema: json!({
                    "type": "object",
                    "required": ["index"],
                    "properties": {
                        "index": { "type": "string", "minLength": 1, "maxLength": 255 }
                    },
                    "additionalProperties": false
                }),
                identity_fields: vec!["/index".into()],
                audit_identity: AgentLiveSessionAuditIdentity::Fingerprint,
            },
            start_parameters_schema: json!({
                "type": "object",
                "properties": {
                    "mode": { "type": "string", "enum": ["pit", "scroll"], "default": "pit" },
                    "query": { "type": "object", "additionalProperties": true },
                    "sort": { "type": "array", "minItems": 1, "maxItems": 16 },
                    "batch_size": { "type": "integer", "minimum": 1, "maximum": MAX_BATCH_SIZE, "default": DEFAULT_BATCH_SIZE },
                    "keep_alive_ms": { "type": "integer", "minimum": MIN_KEEP_ALIVE_MS, "maximum": MAX_KEEP_ALIVE_MS, "default": DEFAULT_KEEP_ALIVE_MS }
                },
                "additionalProperties": false
            }),
            event_schema: json!({
                "type": "object",
                "required": ["event_type"],
                "properties": {
                    "event_type": { "type": "string", "enum": ["hit", "partial_shard_failure"] },
                    "index": { "type": "string" },
                    "id": { "type": "string" },
                    "source": {},
                    "source_omitted": { "type": "boolean" },
                    "failed_shards": { "type": "integer", "minimum": 1 },
                    "failures": { "type": "array", "maxItems": 16 }
                },
                "additionalProperties": false
            }),
            operations: AgentLiveSessionOperations {
                events: SEARCH_STREAM_READ_CAPABILITY.into(),
                input: None,
                resize: None,
                signal: None,
            },
            buffer: AgentLiveSessionBufferPolicy {
                max_events: MAX_BATCH_SIZE + 1,
                max_bytes: 2 * 1024 * 1024,
                overflow: AgentLiveSessionBufferOverflow::DropOldest,
            },
            reconnect: AgentLiveSessionReconnectPolicy {
                mode: AgentLiveSessionReconnectMode::Transient,
                max_attempts: 3,
                initial_backoff_ms: 250,
                max_backoff_ms: 2_000,
                resume: AgentLiveSessionResumeMode::BestEffortCursor,
                cursor_kind: Some(AgentLiveSessionCursorKind::Opaque),
            },
            delivery: AgentLiveSessionDeliveryPolicy {
                backpressure: AgentLiveSessionBackpressureMode::SourcePaced,
                cursor_scope: AgentLiveSessionCursorScopePolicy::Required,
                heartbeat: AgentLiveSessionHeartbeatPolicy::default(),
                max_read_wait_ms: MAX_READ_WAIT_MS,
            },
            control: AgentLiveSessionControlPolicy {
                cancel: AgentLiveSessionCancelBehavior::CallAndSource,
                close: AgentLiveSessionCloseEffect::StopObservation,
            },
            start_risk: CapabilityRiskLevel::ReadOnly,
        },
    ))
}

fn search_event(
    event: &EsPendingEvent,
    scope: &str,
    targets: &[RedactionTarget],
) -> AgentLiveSessionEventInput {
    let (data, cursor, kind) = match event {
        EsPendingEvent::Hit(hit) => (
            json!({
                "event_type": "hit",
                "index": hit.index,
                "id": hit.id,
                "source": hit.source,
                "source_omitted": hit.source_omitted
            }),
            hit.continuation
                .as_ref()
                .map(|continuation| AgentLiveSessionCursor {
                    kind: AgentLiveSessionCursorKind::Opaque,
                    value: continuation.clone(),
                    scope: Some(scope.to_string()),
                }),
            AgentLiveSessionEventKind::Data,
        ),
        EsPendingEvent::ShardFailure {
            failed_shards,
            failures,
        } => (
            json!({
                "event_type": "partial_shard_failure",
                "failed_shards": failed_shards,
                "failures": failures.iter().map(|failure| json!({
                    "index": failure.index,
                    "shard": failure.shard,
                    "status": failure.status,
                    "error_type": failure.error_type
                })).collect::<Vec<_>>()
            }),
            None,
            AgentLiveSessionEventKind::Warning,
        ),
    };
    let (data, redaction) = redact_json(data, targets);
    AgentLiveSessionEventInput {
        observed_at: Utc::now(),
        kind,
        data,
        cursor,
        redaction,
        terminal: false,
    }
}

fn redact_json(value: Value, targets: &[RedactionTarget]) -> (Value, RedactionStatus) {
    let Ok(encoded) = serde_json::to_string(&value) else {
        return (Value::Null, RedactionStatus::FailedClosed);
    };
    let (redacted, status) = redact_text_with_targets(&encoded, targets);
    match serde_json::from_str(&redacted) {
        Ok(value) => (value, status),
        Err(_) => (Value::Null, RedactionStatus::FailedClosed),
    }
}

fn search_mode(parameters: &Value) -> Result<EsSearchMode, PluginSessionError> {
    match parameters
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("pit")
    {
        "pit" => Ok(EsSearchMode::Pit),
        "scroll" => Ok(EsSearchMode::Scroll),
        _ => Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Elasticsearch search mode must be pit or scroll.",
        )),
    }
}

fn read_request(
    request: &AgentSessionCallRequest,
) -> Result<AgentLiveSessionReadRequest, PluginSessionError> {
    if request.input.is_null() {
        Ok(AgentLiveSessionReadRequest::default())
    } else {
        serde_json::from_value(request.input.clone()).map_err(|_| {
            error(
                PluginSessionErrorCode::PolicyDenied,
                "The Elasticsearch live-session read request is invalid.",
            )
        })
    }
}

fn required_string(
    input: &Value,
    field: &str,
    maximum: usize,
) -> Result<String, PluginSessionError> {
    input
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
        })
        .map(str::to_string)
        .ok_or_else(|| {
            error(
                PluginSessionErrorCode::PolicyDenied,
                "Elasticsearch resource identity is invalid.",
            )
        })
}

fn parameter_usize(
    parameters: &Value,
    field: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, PluginSessionError> {
    let value = parameters
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&value) {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Elasticsearch live-session numeric parameter is outside its bound.",
        ));
    }
    Ok(value)
}

fn parameter_u64(
    parameters: &Value,
    field: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, PluginSessionError> {
    let value = parameters
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&value) {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Elasticsearch live-session numeric parameter is outside its bound.",
        ));
    }
    Ok(value)
}

fn owner_error(message: &str) -> PluginSessionError {
    error(PluginSessionErrorCode::OwnerUnavailable, message)
}

fn error(code: PluginSessionErrorCode, message: &str) -> PluginSessionError {
    PluginSessionError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_context(input: Value) -> AgentSessionOpenContext {
        AgentSessionOpenContext {
            binding: voidb_core::AgentSessionBinding {
                grant_id: "grant".into(),
                profile_id: "profile".into(),
                plugin_id: "elasticsearch".into(),
                purpose: PluginSessionPurpose::DatabaseQuery,
                allowed_capabilities: vec![SEARCH_STREAM_READ_CAPABILITY.into()],
                host_generation: 1,
            },
            request: voidb_core::AgentSessionOpenRequest {
                purpose: PluginSessionPurpose::DatabaseQuery,
                capabilities: vec![SEARCH_STREAM_READ_CAPABILITY.into()],
                lease_seconds: 60,
                concurrency: voidb_core::AgentSessionConcurrency::Serialized,
                destructive_acknowledged: false,
                input,
            },
            lease_expires_at: Utc::now() + chrono::Duration::minutes(1),
        }
    }

    fn unavailable_factory() -> EsAgentSessionFactory {
        EsAgentSessionFactory::new(EsConfig {
            urls: vec!["http://fixture-secret@127.0.0.1:1".into()],
            auth: None,
            timeout: 1,
            verify_ssl: false,
        })
    }

    #[test]
    fn search_contract_is_source_paced_and_scoped() {
        let (_, contract) =
            elasticsearch_live_session_contract(SEARCH_STREAM_READ_CAPABILITY).unwrap();
        contract
            .validate(&[SEARCH_STREAM_READ_CAPABILITY.into()])
            .unwrap();
        assert_eq!(
            contract.delivery.backpressure,
            AgentLiveSessionBackpressureMode::SourcePaced
        );
        assert_eq!(
            contract.delivery.cursor_scope,
            AgentLiveSessionCursorScopePolicy::Required
        );
    }

    #[tokio::test]
    async fn scroll_resume_is_rejected_before_target_open() {
        let error = unavailable_factory()
            .open(live_context(json!({
                "resource": { "index": "fixture-events" },
                "parameters": { "mode": "scroll", "keep_alive_ms": 60000 },
                "resume_from": {
                    "kind": "opaque",
                    "value": "opaque-scroll-handle",
                    "scope": "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                }
            })))
            .await
            .err()
            .expect("scroll handles must not resume across generations");
        assert_eq!(error.code, PluginSessionErrorCode::PolicyDenied);
    }

    #[tokio::test]
    async fn pit_scope_mismatch_is_rejected_before_target_open() {
        let error = unavailable_factory()
            .open(live_context(json!({
                "resource": { "index": "fixture-events" },
                "parameters": {
                    "mode": "pit",
                    "sort": ["_shard_doc"],
                    "keep_alive_ms": 60000
                },
                "resume_from": {
                    "kind": "opaque",
                    "value": "opaque-pit-continuation",
                    "scope": "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                }
            })))
            .await
            .err()
            .expect("scope mismatch must fail before target I/O");
        assert_eq!(error.code, PluginSessionErrorCode::BindingMismatch);
    }

    #[test]
    fn partial_shard_failures_stay_machine_readable_and_secret_free() {
        let targets = collect_redaction_targets(&json!({ "token": "fixture-secret" }));
        let event = search_event(
            &EsPendingEvent::ShardFailure {
                failed_shards: 1,
                failures: vec![EsShardFailure {
                    index: Some("fixture-secret".into()),
                    shard: Some("0".into()),
                    status: Some("500".into()),
                    error_type: Some("query_shard_exception".into()),
                }],
            },
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            &targets,
        );
        assert_eq!(event.kind, AgentLiveSessionEventKind::Warning);
        assert_eq!(event.data["event_type"], "partial_shard_failure");
        assert_eq!(event.data["failed_shards"], 1);
        assert_eq!(event.redaction, RedactionStatus::Applied);
        assert!(!event.data.to_string().contains("fixture-secret"));
    }
}
