#![allow(clippy::result_large_err)]

use serde_json::{Value, json};
use voidb_core::{
    CapabilityDefinition, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, CapabilityRiskLevel, CredentialClass, InvocationOutputPage,
    InvocationStatus, Pagination, RedactionStatus, TargetSystemFailure, audit_json_summary,
};

use crate::agent_session::{SEARCH_STREAM_READ_CAPABILITY, elasticsearch_live_session_contract};
use crate::config::{EsAuth, EsConfig};
use crate::service::EsService;
use crate::service::agent_live::{EsBulkOperation, execute_bulk};
use crate::service::types::{ClusterHealth, IndexHealth, IndexInfo, NodeInfo, SearchHit};

const PLUGIN_ID: &str = "elasticsearch";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 500;

pub fn elasticsearch_capabilities() -> Vec<CapabilityDefinition> {
    vec![
        capability(
            "diagnostics",
            "Return agent-safe Elasticsearch profile diagnostics without opening a cluster connection.",
            empty_input_schema(),
            json!({
                "type": "object",
                "required": [
                    "url_count",
                    "primary_url_scheme",
                    "auth_type",
                    "timeout_secs",
                    "verify_ssl",
                    "network_checked"
                ],
                "properties": {
                    "url_count": { "type": "integer", "minimum": 0 },
                    "primary_url_scheme": { "type": ["string", "null"] },
                    "auth_type": { "type": ["string", "null"] },
                    "timeout_secs": { "type": "integer", "minimum": 0 },
                    "verify_ssl": { "type": "boolean" },
                    "network_checked": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "elasticsearch.diagnostics"],
            false,
            false,
            false,
        ),
        capability(
            "health",
            "Read Elasticsearch cluster health.",
            empty_input_schema(),
            json!({
                "type": "object",
                "required": [
                    "cluster_name",
                    "status",
                    "node_count",
                    "active_shards",
                    "unassigned_shards"
                ],
                "properties": {
                    "cluster_name": { "type": "string" },
                    "status": { "type": "string" },
                    "node_count": { "type": "integer", "minimum": 0 },
                    "active_shards": { "type": "integer", "minimum": 0 },
                    "unassigned_shards": { "type": "integer", "minimum": 0 }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "elasticsearch.cluster.health"],
            false,
            false,
            false,
        ),
        capability(
            "nodes",
            "List Elasticsearch cluster nodes with bounded output.",
            empty_input_schema(),
            list_schema("nodes", node_schema()),
            vec!["connection.read", "elasticsearch.nodes.list"],
            false,
            false,
            false,
        ),
        capability(
            "indices",
            "List Elasticsearch indices with bounded output.",
            json!({
                "type": "object",
                "properties": {
                    "include_system": { "type": "boolean", "default": false }
                },
                "additionalProperties": false
            }),
            list_schema("indices", index_schema()),
            vec!["connection.read", "elasticsearch.indices.list"],
            false,
            false,
            false,
        ),
        capability(
            "search",
            "Search one Elasticsearch index with bounded, cursor-based output.",
            json!({
                "type": "object",
                "required": ["index"],
                "properties": {
                    "index": { "type": "string", "minLength": 1 },
                    "query": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            }),
            search_output_schema(),
            vec!["connection.read", "elasticsearch.documents.search"],
            false,
            false,
            false,
        ),
        live_capability(
            SEARCH_STREAM_READ_CAPABILITY,
            "Read Elasticsearch search hits through a bounded PIT or scroll session.",
            vec!["connection.read", "elasticsearch.documents.search"],
        ),
        capability(
            "get",
            "Get one Elasticsearch document by ID.",
            json!({
                "type": "object",
                "required": ["index", "id"],
                "properties": {
                    "index": { "type": "string", "minLength": 1 },
                    "id": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["index", "id", "source", "source_summary"],
                "properties": {
                    "index": { "type": "string" },
                    "id": { "type": "string" },
                    "source": {},
                    "source_summary": { "type": "object" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "elasticsearch.documents.get"],
            false,
            false,
            false,
        ),
        capability(
            "count",
            "Count documents in one Elasticsearch index.",
            json!({
                "type": "object",
                "required": ["index"],
                "properties": {
                    "index": { "type": "string", "minLength": 1 },
                    "query": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["index", "count", "query_summary"],
                "properties": {
                    "index": { "type": "string" },
                    "count": { "type": "integer", "minimum": 0 },
                    "query_summary": { "type": "object" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "elasticsearch.documents.count"],
            false,
            false,
            false,
        ),
        capability(
            "mapping",
            "Read index mapping metadata without returning the raw mapping body.",
            json!({
                "type": "object",
                "required": ["index"],
                "properties": {
                    "index": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["index", "properties", "property_count", "raw_omitted"],
                "properties": {
                    "index": { "type": "string" },
                    "properties": { "type": "array", "items": { "type": "string" } },
                    "property_count": { "type": "integer", "minimum": 0 },
                    "raw_omitted": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "elasticsearch.mappings.get"],
            false,
            false,
            false,
        ),
        capability(
            "bulk",
            "Execute a bounded Elasticsearch bulk request with item-level failure reporting.",
            json!({
                "type": "object",
                "required": ["index", "operations"],
                "properties": {
                    "index": { "type": "string", "minLength": 1 },
                    "operations": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 100,
                        "items": {
                            "type": "object",
                            "required": ["type"],
                            "properties": {
                                "type": { "type": "string", "enum": ["index", "create", "update", "delete"] },
                                "id": { "type": "string", "minLength": 1, "maxLength": 512 },
                                "document": {}
                            },
                            "additionalProperties": false
                        }
                    }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "elasticsearch.documents.bulk"],
            true,
            false,
            true,
        ),
        capability(
            "raw_api",
            "Run a raw Elasticsearch API call; all raw API calls are destructive-gated.",
            json!({
                "type": "object",
                "required": ["method", "path"],
                "properties": {
                    "method": {
                        "type": "string",
                        "enum": ["GET", "POST", "PUT", "DELETE", "get", "post", "put", "delete"]
                    },
                    "path": { "type": "string", "minLength": 1 },
                    "body": {}
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["ok", "operation", "dry_run", "destructive", "details"],
                "properties": {
                    "ok": { "type": "boolean" },
                    "operation": { "type": "string" },
                    "dry_run": { "type": "boolean" },
                    "would_execute": { "type": "boolean" },
                    "destructive": { "type": "boolean" },
                    "details": { "type": "object" },
                    "body": {},
                    "body_summary": { "type": ["object", "null"] }
                },
                "additionalProperties": false
            }),
            vec!["connection.write", "elasticsearch.raw_api"],
            true,
            false,
            true,
        ),
    ]
}

pub async fn invoke_elasticsearch_capability(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    if invocation.plugin_id != PLUGIN_ID {
        return Err(validation_error(
            "validation.plugin_mismatch",
            "Invocation plugin_id does not match Elasticsearch.",
            json!({ "expected": PLUGIN_ID, "actual": invocation.plugin_id }),
        ));
    }

    match invocation.capability_id.as_str() {
        "diagnostics" => Ok(diagnostics_result(config, invocation.id)),
        "health" => invoke_health(config, invocation).await,
        "nodes" => invoke_nodes(config, invocation).await,
        "indices" => invoke_indices(config, invocation).await,
        "search" => invoke_search(config, invocation).await,
        "search_stream_read" => Err(unavailable_error(
            "unavailable.session_required",
            "This Elasticsearch live workflow requires a persistent agent session.",
            json!({ "capability_id": invocation.capability_id }),
        )),
        "get" => invoke_get(config, invocation).await,
        "count" => invoke_count(config, invocation).await,
        "mapping" => invoke_mapping(config, invocation).await,
        "bulk" => invoke_bulk(config, invocation).await,
        "raw_api" => invoke_raw_api(config, invocation).await,
        other => Err(unavailable_error(
            "unavailable.capability_not_found",
            "Elasticsearch capability was not found.",
            json!({ "capability_id": other }),
        )),
    }
}

fn live_capability(
    qualified_id: &str,
    description: &str,
    permissions: Vec<&str>,
) -> CapabilityDefinition {
    let id = qualified_id
        .strip_prefix("elasticsearch.")
        .expect("Elasticsearch live capability ID");
    let (purpose, contract) = elasticsearch_live_session_contract(qualified_id)
        .expect("Elasticsearch live-session contract");
    let handoff_capabilities = contract
        .operations
        .capabilities()
        .cloned()
        .collect::<Vec<_>>();
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema: live_read_schema(),
        output_schema: live_batch_schema(),
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: elasticsearch_live_authorization(purpose.clone()),
        risk: CapabilityRiskLevel::ReadOnly,
        destructive: false,
        streaming: true,
        execution_mode: voidb_core::CapabilityExecutionMode::SessionOnly,
        session_handoff: Some(
            voidb_core::CapabilitySessionHandoff::new(purpose, handoff_capabilities)
                .with_live_session(contract),
        ),
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run: false,
        default_timeout_ms: Some(DEFAULT_TIMEOUT_MS),
    }
}

fn elasticsearch_live_authorization(
    purpose: voidb_core::PluginSessionPurpose,
) -> voidb_core::CapabilityAuthorizationMetadata {
    voidb_core::CapabilityAuthorizationMetadata::declared()
        .with_session_purposes(vec![purpose])
        .with_note(
            "PIT and scroll handles stay plugin-owned; only scoped, expiring PIT search-after tokens are agent-visible.",
        )
        .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
            voidb_core::CapabilityApprovalField::new(
                "/resource/index",
                "Index",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required()
            .with_constraint(voidb_core::CapabilityConstraintKind::Prefix),
            voidb_core::CapabilityApprovalField::new(
                "/parameters/query",
                "Search query",
                voidb_core::CapabilityApprovalValueType::Json,
            )
            .with_constraint(voidb_core::CapabilityConstraintKind::Subset),
        ]))
}

fn live_read_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "after_sequence": { "type": "integer", "minimum": 0 },
            "max_events": { "type": "integer", "minimum": 1, "maximum": 1000 },
            "max_bytes": { "type": "integer", "minimum": 1, "maximum": 1048576 },
            "wait_timeout_ms": { "type": "integer", "minimum": 0, "maximum": 30000 }
        },
        "additionalProperties": false
    })
}

fn live_batch_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "protocol_version", "events", "next_sequence", "timed_out", "source_closed",
            "dropped_events", "dropped_bytes", "coalesced_events", "reconnect_attempts"
        ],
        "properties": {
            "protocol_version": { "type": "integer", "const": 1 },
            "events": { "type": "array", "maxItems": 1000 },
            "next_sequence": { "type": "integer", "minimum": 1 },
            "resume_cursor": { "type": "object" },
            "checkpoint": { "type": "object" },
            "oldest_available_sequence": { "type": "integer", "minimum": 1 },
            "truncated": { "type": "boolean" },
            "timed_out": { "type": "boolean" },
            "source_closed": { "type": "boolean" },
            "dropped_events": { "type": "integer", "minimum": 0 },
            "dropped_bytes": { "type": "integer", "minimum": 0 },
            "coalesced_events": { "type": "integer", "minimum": 0 },
            "reconnect_attempts": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

#[allow(clippy::too_many_arguments)]
fn capability(
    id: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
    permissions: Vec<&str>,
    destructive: bool,
    streaming: bool,
    supports_dry_run: bool,
) -> CapabilityDefinition {
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: elasticsearch_authorization_metadata(id),
        risk: CapabilityRiskLevel::from_destructive(destructive),
        destructive,
        streaming,
        execution_mode: voidb_core::CapabilityExecutionMode::Stateless,
        session_handoff: None,
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run,
        default_timeout_ms: Some(DEFAULT_TIMEOUT_MS),
    }
}

fn elasticsearch_authorization_metadata(id: &str) -> voidb_core::CapabilityAuthorizationMetadata {
    let fields = match id {
        "search" | "count" | "mapping" => vec![
            voidb_core::CapabilityApprovalField::new(
                "/index",
                "Index",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required()
            .with_constraint(voidb_core::CapabilityConstraintKind::Prefix),
        ],
        "get" => vec![
            voidb_core::CapabilityApprovalField::new(
                "/index",
                "Index",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required()
            .with_constraint(voidb_core::CapabilityConstraintKind::Prefix),
            voidb_core::CapabilityApprovalField::new(
                "/id",
                "Document ID",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required(),
        ],
        "bulk" => vec![
            voidb_core::CapabilityApprovalField::new(
                "/index",
                "Index",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required()
            .with_constraint(voidb_core::CapabilityConstraintKind::Prefix)
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
            voidb_core::CapabilityApprovalField::new(
                "/operations",
                "Bulk operations",
                voidb_core::CapabilityApprovalValueType::Json,
            )
            .required()
            .with_constraint(voidb_core::CapabilityConstraintKind::Subset)
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
        ],
        "raw_api" => vec![
            voidb_core::CapabilityApprovalField::new(
                "/method",
                "HTTP method",
                voidb_core::CapabilityApprovalValueType::String,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
            voidb_core::CapabilityApprovalField::new(
                "/path",
                "API path prefix",
                voidb_core::CapabilityApprovalValueType::Path,
            )
            .required()
            .with_constraint(voidb_core::CapabilityConstraintKind::Prefix)
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
        ],
        _ => Vec::new(),
    };
    let metadata = voidb_core::CapabilityAuthorizationMetadata::declared();
    if fields.is_empty() {
        metadata
    } else {
        metadata
            .with_note(if id == "raw_api" {
                "Method and path prefix are enforced; raw calls remain separate from managed PIT, scroll, and bulk workflows."
            } else if id == "bulk" {
                "Index scope and the exact bounded operation list require destructive approval."
            } else {
                "Index and document constraints are revalidated before execution."
            })
            .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields))
    }
}

fn diagnostics_result(config: &EsConfig, invocation_id: String) -> CapabilityInvocationResult {
    let output = json!({
        "url_count": config.urls.len(),
        "primary_url_scheme": url_scheme(config.primary_url()),
        "auth_type": es_auth_type(config),
        "timeout_secs": config.timeout,
        "verify_ssl": config.verify_ssl,
        "network_checked": false,
    });
    result(invocation_id, output.clone(), output, None)
}

async fn invoke_health(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let service = service(config)?;
    let health = service
        .cluster_health_direct()
        .await
        .map_err(|error| target_error(config, "elasticsearch.health_failed", error.to_string()))?;
    let output = health_json(&health);
    let summary = output.clone();
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_nodes(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config)?;
    let mut nodes = service
        .list_nodes_direct()
        .await
        .map_err(|error| target_error(config, "elasticsearch.nodes_failed", error.to_string()))?;
    nodes.sort_by(|left, right| left.name.cmp(&right.name));
    let items = nodes.iter().map(node_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "nodes",
        items,
        page,
        Value::Null,
    ))
}

async fn invoke_indices(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let include_system = optional_bool(&invocation.input, "include_system")?.unwrap_or(false);
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config)?;
    let mut indices = if include_system {
        list_all_indices(&service, config).await?
    } else {
        service.list_indices_direct().await.map_err(|error| {
            target_error(config, "elasticsearch.indices_failed", error.to_string())
        })?
    };
    indices.sort_by(|left, right| left.name.cmp(&right.name));
    let items = indices.iter().map(index_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "indices",
        items,
        page,
        json!({ "include_system": include_system }),
    ))
}

async fn invoke_search(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let index = required_string(&invocation.input, "index")?;
    let query = invocation
        .input
        .get("query")
        .cloned()
        .unwrap_or_else(|| json!({ "query": { "match_all": {} } }));
    if !query.is_object() {
        return Err(validation_error(
            "validation.input_field_invalid",
            "Elasticsearch query must be a JSON object.",
            json!({ "field": "query" }),
        ));
    }
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config)?;
    let search_result = service
        .search_direct(&index, &query, (page.limit + 1) as u32, page.offset as u32)
        .await
        .map_err(|error| target_error(config, "elasticsearch.search_failed", error.to_string()))?;
    let hits = search_result
        .hits
        .iter()
        .take(page.limit)
        .map(hit_json)
        .collect::<Vec<_>>();
    let next_offset = page.offset.saturating_add(hits.len());
    let next_cursor = (next_offset < search_result.total as usize).then(|| next_offset.to_string());
    let hit_summaries = hits
        .iter()
        .map(|hit| {
            json!({
                "id": hit["id"],
                "index": hit["index"],
                "source_summary": audit_json_summary(&hit["source"]),
            })
        })
        .collect::<Vec<_>>();
    let item_count = hits.len();
    let truncated = next_cursor.is_some();
    let output = json!({
        "index": index,
        "hits": hits,
        "hit_summaries": hit_summaries,
        "total": search_result.total,
        "item_count": item_count,
        "limit": page.limit,
        "cursor": page.cursor,
        "next_cursor": next_cursor,
        "truncated": truncated,
        "query_summary": audit_json_summary(&query),
        "columns": search_result.columns,
    });
    let summary = json!({
        "index": output["index"],
        "item_count": item_count,
        "total": output["total"],
        "truncated": truncated,
        "next_cursor": output["next_cursor"],
        "hit_summaries": output["hit_summaries"],
        "query_summary": output["query_summary"],
    });
    let output_page = truncated.then(|| InvocationOutputPage {
        next_cursor: output["next_cursor"].as_str().map(str::to_string),
    });
    Ok(result(invocation.id, output, summary, output_page))
}

async fn invoke_get(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let index = required_string(&invocation.input, "index")?;
    let id = required_string(&invocation.input, "id")?;
    let service = service(config)?;
    let source = service
        .get_document_direct(&index, &id)
        .await
        .map_err(|error| target_error(config, "elasticsearch.get_failed", error.to_string()))?;
    let output = json!({
        "index": index,
        "id": id,
        "source": source,
        "source_summary": audit_json_summary(&source),
    });
    let summary = json!({
        "index": output["index"],
        "id": output["id"],
        "source_summary": output["source_summary"],
    });
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_count(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let index = required_string(&invocation.input, "index")?;
    let query = invocation.input.get("query").cloned();
    if let Some(query) = &query
        && !query.is_object()
    {
        return Err(validation_error(
            "validation.input_field_invalid",
            "Elasticsearch query must be a JSON object.",
            json!({ "field": "query" }),
        ));
    }
    let service = service(config)?;
    let count = service
        .count_documents_direct(&index, query.as_ref())
        .await
        .map_err(|error| target_error(config, "elasticsearch.count_failed", error.to_string()))?;
    let query_summary = query
        .as_ref()
        .map(audit_json_summary)
        .unwrap_or_else(|| json!({ "kind": "null" }));
    let output = json!({
        "index": index,
        "count": count,
        "query_summary": query_summary,
    });
    let summary = output.clone();
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_mapping(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let index = required_string(&invocation.input, "index")?;
    let service = service(config)?;
    let mapping = service
        .get_mapping_direct(&index)
        .await
        .map_err(|error| target_error(config, "elasticsearch.mapping_failed", error.to_string()))?;
    let output = json!({
        "index": mapping.index,
        "properties": mapping.properties,
        "property_count": mapping.properties.len(),
        "raw_omitted": true,
    });
    let summary = output.clone();
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_bulk(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let index = required_string(&invocation.input, "index")?;
    let operations = bulk_operations(&invocation.input)?;
    let operation_types = operations
        .iter()
        .map(EsBulkOperation::name)
        .collect::<Vec<_>>();
    let preview = json!({
        "index": index,
        "operation_count": operations.len(),
        "operation_types": operation_types,
        "payloads_omitted": true
    });
    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "bulk", preview));
    }
    let bulk = execute_bulk(config, &index, &operations)
        .await
        .map_err(|error| {
            target_error(config, "elasticsearch.bulk_failed", error.code.to_string())
        })?;
    let items = bulk
        .items
        .iter()
        .map(|item| {
            json!({
                "index": item.index,
                "operation": item.operation,
                "id": item.id,
                "status": item.status,
                "ok": item.ok,
                "error_type": item.error_type,
                "retryable": item.retryable
            })
        })
        .collect::<Vec<_>>();
    let details = json!({
        "index": index,
        "successful": bulk.successful,
        "failed": bulk.failed,
        "items": items
    });
    let output = json!({
        "ok": bulk.failed == 0,
        "operation": "bulk",
        "dry_run": false,
        "destructive": true,
        "details": details
    });
    Ok(result(
        invocation.id,
        output,
        json!({
            "operation": "bulk",
            "successful": bulk.successful,
            "failed": bulk.failed
        }),
        None,
    ))
}

async fn invoke_raw_api(
    config: &EsConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let method = required_string(&invocation.input, "method")?.to_uppercase();
    validate_method(&method)?;
    let path = required_string(&invocation.input, "path")?;
    let body = invocation.input.get("body").cloned();
    let details = json!({
        "method": method,
        "path": path,
        "body_summary": body.as_ref().map(audit_json_summary),
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "raw_api", details));
    }

    let service = service(config)?;
    let result_body = service
        .raw_api_direct(&method, &path, body.as_ref())
        .await
        .map_err(|error| target_error(config, "elasticsearch.raw_api_failed", error.to_string()))?
        .body;
    let output = json!({
        "ok": true,
        "operation": "raw_api",
        "dry_run": false,
        "destructive": true,
        "details": details,
        "body": result_body,
        "body_summary": audit_json_summary(&result_body),
    });
    let summary = json!({
        "operation": "raw_api",
        "dry_run": false,
        "body_summary": output["body_summary"],
    });
    Ok(result(invocation.id, output, summary, None))
}

fn service(config: &EsConfig) -> Result<EsService, CapabilityError> {
    EsService::new_direct(config.clone())
        .map_err(|error| target_error(config, "elasticsearch.connect_failed", error.to_string()))
}

async fn list_all_indices(
    service: &EsService,
    config: &EsConfig,
) -> Result<Vec<IndexInfo>, CapabilityError> {
    let raw = service
        .list_all_indices_raw_direct()
        .await
        .map_err(|error| target_error(config, "elasticsearch.indices_failed", error.to_string()))?;
    let items = raw.as_array().ok_or_else(|| {
        target_error(
            config,
            "elasticsearch.indices_parse_failed",
            "Expected array from /_cat/indices.".to_string(),
        )
    })?;
    Ok(items
        .iter()
        .map(|item| IndexInfo {
            name: item["index"].as_str().unwrap_or("").to_string(),
            health: IndexHealth::from_str(item["health"].as_str().unwrap_or("red")),
            status: item["status"].as_str().unwrap_or("").to_string(),
            doc_count: item["docs.count"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0),
            store_size: item["store.size"].as_str().unwrap_or("0b").to_string(),
            pri_shards: item["pri"].as_str().unwrap_or("0").parse().unwrap_or(0),
            rep_shards: item["rep"].as_str().unwrap_or("0").parse().unwrap_or(0),
        })
        .collect())
}

fn paged_result(
    invocation_id: String,
    item_key: &str,
    items: Vec<Value>,
    page: PageRequest,
    metadata: Value,
) -> CapabilityInvocationResult {
    let source_count = items.len();
    let end = page.offset.saturating_add(page.limit).min(source_count);
    let page_items = if page.offset >= source_count {
        Vec::new()
    } else {
        items[page.offset..end].to_vec()
    };
    let next_cursor = (end < source_count).then(|| end.to_string());
    let item_count = page_items.len();
    let truncated = next_cursor.is_some();
    let output = json!({
        item_key: page_items,
        "item_count": item_count,
        "source_item_count": source_count,
        "limit": page.limit,
        "cursor": page.cursor,
        "next_cursor": next_cursor,
        "truncated": truncated,
        "metadata": metadata,
    });
    let summary = json!({
        "item_key": item_key,
        "item_count": item_count,
        "source_item_count": source_count,
        "truncated": truncated,
        "next_cursor": output["next_cursor"],
    });
    let output_page = truncated.then(|| InvocationOutputPage {
        next_cursor: output["next_cursor"].as_str().map(str::to_string),
    });
    result(invocation_id, output, summary, output_page)
}

fn dry_run_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    let output = json!({
        "ok": true,
        "operation": operation,
        "dry_run": true,
        "would_execute": true,
        "destructive": true,
        "details": details,
    });
    result(
        invocation_id,
        output,
        json!({ "operation": operation, "dry_run": true }),
        None,
    )
}

fn result(
    invocation_id: String,
    output: Value,
    output_summary: Value,
    page: Option<InvocationOutputPage>,
) -> CapabilityInvocationResult {
    CapabilityInvocationResult {
        invocation_id,
        status: InvocationStatus::Succeeded,
        output,
        output_summary,
        page,
    }
}

fn health_json(health: &ClusterHealth) -> Value {
    json!({
        "cluster_name": health.cluster_name,
        "status": health_status(&health.status),
        "node_count": health.node_count,
        "active_shards": health.active_shards,
        "unassigned_shards": health.unassigned_shards,
    })
}

fn node_json(node: &NodeInfo) -> Value {
    json!({
        "name": node.name,
        "roles": node.roles,
        "heap_percent": node.heap_percent,
        "disk_used": node.disk_used,
        "cpu_percent": node.cpu_percent,
    })
}

fn index_json(index: &IndexInfo) -> Value {
    json!({
        "name": index.name,
        "health": health_status(&index.health),
        "status": index.status,
        "doc_count": index.doc_count,
        "store_size": index.store_size,
        "pri_shards": index.pri_shards,
        "rep_shards": index.rep_shards,
    })
}

fn hit_json(hit: &SearchHit) -> Value {
    json!({
        "id": hit.id,
        "index": hit.index,
        "source": hit.source,
        "score": hit.score,
        "source_summary": audit_json_summary(&hit.source),
    })
}

fn health_status(status: &IndexHealth) -> &'static str {
    match status {
        IndexHealth::Green => "green",
        IndexHealth::Yellow => "yellow",
        IndexHealth::Red => "red",
    }
}

fn empty_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false
    })
}

fn list_schema(item_key: &str, item_schema: Value) -> Value {
    json!({
        "type": "object",
        "required": [
            item_key,
            "item_count",
            "source_item_count",
            "limit",
            "cursor",
            "next_cursor",
            "truncated",
            "metadata"
        ],
        "properties": {
            item_key: { "type": "array", "items": item_schema },
            "item_count": { "type": "integer", "minimum": 0 },
            "source_item_count": { "type": "integer", "minimum": 0 },
            "limit": { "type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT },
            "cursor": { "type": ["string", "null"] },
            "next_cursor": { "type": ["string", "null"] },
            "truncated": { "type": "boolean" },
            "metadata": { "type": ["object", "null"] }
        },
        "additionalProperties": false
    })
}

fn node_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name", "roles", "heap_percent", "disk_used", "cpu_percent"],
        "properties": {
            "name": { "type": "string" },
            "roles": { "type": "array", "items": { "type": "string" } },
            "heap_percent": { "type": "integer", "minimum": 0 },
            "disk_used": { "type": "string" },
            "cpu_percent": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

fn index_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "name",
            "health",
            "status",
            "doc_count",
            "store_size",
            "pri_shards",
            "rep_shards"
        ],
        "properties": {
            "name": { "type": "string" },
            "health": { "type": "string" },
            "status": { "type": "string" },
            "doc_count": { "type": "integer", "minimum": 0 },
            "store_size": { "type": "string" },
            "pri_shards": { "type": "integer", "minimum": 0 },
            "rep_shards": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

fn search_output_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "index",
            "hits",
            "hit_summaries",
            "total",
            "item_count",
            "limit",
            "cursor",
            "next_cursor",
            "truncated",
            "query_summary",
            "columns"
        ],
        "properties": {
            "index": { "type": "string" },
            "hits": { "type": "array", "items": { "type": "object" } },
            "hit_summaries": { "type": "array", "items": { "type": "object" } },
            "total": { "type": "integer", "minimum": 0 },
            "item_count": { "type": "integer", "minimum": 0 },
            "limit": { "type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT },
            "cursor": { "type": ["string", "null"] },
            "next_cursor": { "type": ["string", "null"] },
            "truncated": { "type": "boolean" },
            "query_summary": { "type": "object" },
            "columns": { "type": "array", "items": { "type": "string" } }
        },
        "additionalProperties": false
    })
}

fn mutation_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ok", "operation", "dry_run", "destructive", "details"],
        "properties": {
            "ok": { "type": "boolean" },
            "operation": { "type": "string" },
            "dry_run": { "type": "boolean" },
            "would_execute": { "type": "boolean" },
            "destructive": { "type": "boolean" },
            "details": { "type": "object" }
        },
        "additionalProperties": false
    })
}

fn bulk_operations(input: &Value) -> Result<Vec<EsBulkOperation>, CapabilityError> {
    let values = input
        .get("operations")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            validation_error(
                "validation.input_field_required",
                "Elasticsearch bulk operations must be an array.",
                json!({ "field": "operations" }),
            )
        })?;
    if values.is_empty() || values.len() > 100 {
        return Err(validation_error(
            "validation.bulk_operation_count_invalid",
            "Elasticsearch bulk requests require 1 to 100 operations.",
            json!({ "minimum": 1, "maximum": 100, "actual": values.len() }),
        ));
    }
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let operation = value.as_object().ok_or_else(|| {
                validation_error(
                    "validation.bulk_operation_invalid",
                    "Elasticsearch bulk operation must be an object.",
                    json!({ "index": index }),
                )
            })?;
            let operation_type =
                operation
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        validation_error(
                            "validation.bulk_operation_invalid",
                            "Elasticsearch bulk operation type is required.",
                            json!({ "index": index }),
                        )
                    })?;
            let id = operation
                .get("id")
                .map(|value| {
                    value
                        .as_str()
                        .filter(|value| {
                            !value.is_empty()
                                && value.len() <= 512
                                && !value.chars().any(char::is_control)
                        })
                        .map(str::to_string)
                        .ok_or_else(|| {
                            validation_error(
                                "validation.bulk_operation_invalid",
                                "Elasticsearch bulk document ID is invalid.",
                                json!({ "index": index }),
                            )
                        })
                })
                .transpose()?;
            let document = || {
                operation.get("document").cloned().ok_or_else(|| {
                    validation_error(
                        "validation.bulk_operation_invalid",
                        "Elasticsearch bulk operation document is required.",
                        json!({ "index": index }),
                    )
                })
            };
            match operation_type {
                "index" => Ok(EsBulkOperation::Index {
                    id,
                    document: document()?,
                }),
                "create" => Ok(EsBulkOperation::Create {
                    id: id.ok_or_else(|| {
                        validation_error(
                            "validation.bulk_operation_invalid",
                            "Elasticsearch bulk create requires an ID.",
                            json!({ "index": index }),
                        )
                    })?,
                    document: document()?,
                }),
                "update" => Ok(EsBulkOperation::Update {
                    id: id.ok_or_else(|| {
                        validation_error(
                            "validation.bulk_operation_invalid",
                            "Elasticsearch bulk update requires an ID.",
                            json!({ "index": index }),
                        )
                    })?,
                    document: document()?,
                }),
                "delete" => Ok(EsBulkOperation::Delete {
                    id: id.ok_or_else(|| {
                        validation_error(
                            "validation.bulk_operation_invalid",
                            "Elasticsearch bulk delete requires an ID.",
                            json!({ "index": index }),
                        )
                    })?,
                }),
                _ => Err(validation_error(
                    "validation.bulk_operation_invalid",
                    "Elasticsearch bulk operation type is unsupported.",
                    json!({ "index": index, "type": operation_type }),
                )),
            }
        })
        .collect()
}

fn required_string(input: &Value, field: &str) -> Result<String, CapabilityError> {
    optional_string(input, field)?.ok_or_else(|| {
        validation_error(
            "validation.input_field_required",
            "Required string input field is missing.",
            json!({ "field": field }),
        )
    })
}

fn optional_string(input: &Value, field: &str) -> Result<Option<String>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                validation_error(
                    "validation.input_field_required",
                    "String input field cannot be empty.",
                    json!({ "field": field }),
                )
            }),
        None => Ok(None),
    }
}

fn optional_bool(input: &Value, field: &str) -> Result<Option<bool>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_boolean() => Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be a boolean.",
            json!({ "field": field }),
        )),
        Some(value) => Ok(value.as_bool()),
        None => Ok(None),
    }
}

fn validate_method(method: &str) -> Result<(), CapabilityError> {
    match method {
        "GET" | "POST" | "PUT" | "DELETE" => Ok(()),
        other => Err(validation_error(
            "validation.elasticsearch_method_invalid",
            "Elasticsearch raw API method is not supported.",
            json!({ "method": other, "supported_methods": ["GET", "POST", "PUT", "DELETE"] }),
        )),
    }
}

#[derive(Debug)]
struct PageRequest {
    limit: usize,
    offset: usize,
    cursor: Option<String>,
}

fn page_request(page: Option<&Pagination>) -> Result<PageRequest, CapabilityError> {
    let Some(page) = page else {
        return Ok(PageRequest {
            limit: DEFAULT_PAGE_LIMIT,
            offset: 0,
            cursor: None,
        });
    };
    let limit = (page.limit as usize).clamp(1, MAX_PAGE_LIMIT);
    let offset = match &page.cursor {
        Some(cursor) => cursor.parse::<usize>().map_err(|_| {
            validation_error(
                "validation.cursor_invalid",
                "Elasticsearch cursor must be a numeric offset.",
                json!({ "cursor": cursor }),
            )
        })?,
        None => 0,
    };
    Ok(PageRequest {
        limit,
        offset,
        cursor: page.cursor.clone(),
    })
}

fn url_scheme(url: &str) -> Option<String> {
    url.split_once("://").map(|(scheme, _)| scheme.to_string())
}

fn es_auth_type(config: &EsConfig) -> Option<&'static str> {
    match config.auth.as_ref() {
        Some(EsAuth::Basic { .. }) => Some("basic"),
        Some(EsAuth::ApiKey { .. }) => Some("api_key"),
        Some(EsAuth::Bearer { .. }) => Some("bearer"),
        None => None,
    }
}

fn validation_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Validation,
        code,
        message,
        details,
        None,
        false,
    )
}

fn unavailable_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Unavailable,
        code,
        message,
        details,
        None,
        true,
    )
}

fn target_error(config: &EsConfig, code: &str, message: String) -> CapabilityError {
    let (message, redaction) = redact_elasticsearch_target_message(message, config);
    CapabilityError {
        category: CapabilityErrorCategory::TargetSystem,
        code: code.to_string(),
        message: "Elasticsearch target operation failed.".to_string(),
        details: json!({ "message": message }),
        target: Some(TargetSystemFailure {
            system: Some("elasticsearch".into()),
            code: Some(code.into()),
            message: Some(message),
        }),
        retryable: false,
        redaction,
    }
}

fn redact_elasticsearch_target_message(
    message: String,
    config: &EsConfig,
) -> (String, RedactionStatus) {
    let original = message.clone();
    let mut redacted = message;

    for url in &config.urls {
        redact_value(&mut redacted, url);
        redact_value(&mut redacted, url.trim_end_matches('/'));
        if let Some(authority) = url_authority(url) {
            redact_value(&mut redacted, &authority);
        }
        if let Some(path) = url_path(url) {
            redact_value(&mut redacted, &path);
        }
    }

    if let Some(auth) = &config.auth {
        match auth {
            EsAuth::Basic { username, password } => {
                redact_value(&mut redacted, username);
                redact_value(&mut redacted, password);
            }
            EsAuth::ApiKey { id, api_key } => {
                redact_value(&mut redacted, id);
                redact_value(&mut redacted, api_key);
                use base64::Engine;
                let encoded =
                    base64::engine::general_purpose::STANDARD.encode(format!("{id}:{api_key}"));
                redact_value(&mut redacted, &encoded);
            }
            EsAuth::Bearer { token } => {
                redact_value(&mut redacted, token);
            }
        }
    }

    let redaction = if redacted != original {
        RedactionStatus::Applied
    } else {
        RedactionStatus::NotRequired
    };
    (redacted, redaction)
}

fn url_authority(url: &str) -> Option<String> {
    let (_, rest) = url.split_once("://")?;
    let authority = rest
        .split(['/', '?'])
        .next()
        .filter(|authority| !authority.is_empty())?;
    (!authority.is_empty()).then(|| authority.to_string())
}

fn url_path(url: &str) -> Option<String> {
    let (_, rest) = url.split_once("://")?;
    let (_, path_and_query) = rest.split_once('/')?;
    let path = path_and_query
        .split('?')
        .next()
        .filter(|path| path.len() >= 4 && *path != "/")?;
    Some(path.to_string())
}

fn redact_value(message: &mut String, sensitive: &str) {
    if sensitive.len() >= 4 && message.contains(sensitive) {
        *message = message.replace(sensitive, "<redacted>");
    }
}

fn capability_error(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
) -> CapabilityError {
    CapabilityError {
        category,
        code: code.to_string(),
        message: message.to_string(),
        details,
        target,
        retryable,
        redaction: RedactionStatus::NotRequired,
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use chrono::Utc;
    use serde_json::json;
    use voidb_core::{
        ActorRef, ActorType, ConnectionInstancePurpose, ConnectionProfileRef, InstanceReusePolicy,
        InvocationConnectionTarget, InvocationControls,
    };

    use super::*;

    #[test]
    fn catalog_marks_raw_api_as_destructive_dry_run() {
        let capabilities = elasticsearch_capabilities();
        let raw_api = capabilities
            .iter()
            .find(|capability| capability.id == "raw_api")
            .expect("raw api capability");
        assert!(raw_api.destructive);
        assert!(raw_api.supports_dry_run);
        assert_eq!(raw_api.effective_risk(), CapabilityRiskLevel::Destructive);
        let bulk = capabilities
            .iter()
            .find(|capability| capability.id == "bulk")
            .expect("bulk capability");
        assert!(bulk.destructive);
        assert!(bulk.supports_dry_run);

        let search = capabilities
            .iter()
            .find(|capability| capability.id == "search")
            .expect("search capability");
        assert!(!search.destructive);
        assert!(!search.supports_dry_run);
    }

    #[tokio::test]
    async fn raw_api_dry_run_does_not_open_elasticsearch_connection() {
        let config = EsConfig {
            urls: vec!["http://127.0.0.1:1".into()],
            timeout: 1,
            ..Default::default()
        };
        let mut invocation = invocation(
            "raw_api",
            json!({
                "method": "POST",
                "path": "/users/_doc/1",
                "body": { "email": "ada@example.com", "role": "admin" }
            }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_elasticsearch_capability(&config, invocation)
            .await
            .expect("dry-run");
        let encoded = serde_json::to_string(&result).expect("serialize");

        assert_eq!(result.output["dry_run"], true);
        assert_eq!(result.output["operation"], "raw_api");
        assert_eq!(result.output["details"]["body_summary"]["kind"], "object");
        assert!(!encoded.contains("ada@example.com"));
    }

    #[tokio::test]
    async fn bulk_dry_run_previews_operations_without_payload_or_connection() {
        let config = EsConfig {
            urls: vec!["http://127.0.0.1:1".into()],
            timeout: 1,
            ..Default::default()
        };
        let mut invocation = invocation(
            "bulk",
            json!({
                "index": "users",
                "operations": [
                    { "type": "index", "id": "1", "document": { "secret": "omit-me" } },
                    { "type": "delete", "id": "2" }
                ]
            }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_elasticsearch_capability(&config, invocation)
            .await
            .expect("dry-run");
        let encoded = serde_json::to_string(&result).expect("serialize");
        assert_eq!(result.output["operation"], "bulk");
        assert_eq!(result.output["details"]["operation_count"], 2);
        assert_eq!(result.output["details"]["payloads_omitted"], true);
        assert!(!encoded.contains("omit-me"));
    }

    #[tokio::test]
    async fn diagnostics_do_not_open_elasticsearch_connection_or_expose_secrets() {
        let config = EsConfig {
            urls: vec!["https://search.example.invalid:9200".into()],
            auth: Some(EsAuth::Bearer {
                token: "es-token".into(),
            }),
            timeout: 7,
            verify_ssl: false,
        };

        let result = invoke_elasticsearch_capability(&config, invocation("diagnostics", json!({})))
            .await
            .expect("diagnostics");
        let encoded = serde_json::to_string(&result).expect("serialize");

        assert_eq!(result.output["url_count"], 1);
        assert_eq!(result.output["primary_url_scheme"], "https");
        assert_eq!(result.output["auth_type"], "bearer");
        assert_eq!(result.output["timeout_secs"], 7);
        assert_eq!(result.output["network_checked"], false);
        assert!(!encoded.contains("es-token"));
        assert!(!encoded.contains("search.example.invalid"));
    }

    #[test]
    fn target_errors_redact_configured_elasticsearch_target_material() {
        let config = EsConfig {
            urls: vec!["https://search.example.invalid:9243/hidden-base".into()],
            auth: Some(EsAuth::ApiKey {
                id: "fixture-key-id".into(),
                api_key: "fixture-api-key".into(),
            }),
            ..Default::default()
        };
        let encoded_header =
            base64::engine::general_purpose::STANDARD.encode("fixture-key-id:fixture-api-key");
        let error = target_error(
            &config,
            "elasticsearch.health_failed",
            format!(
                "request to https://search.example.invalid:9243/hidden-base/_cluster/health failed with ApiKey {encoded_header}"
            ),
        );
        let encoded = serde_json::to_string(&error).expect("serialize error");

        assert_eq!(error.redaction, RedactionStatus::Applied);
        assert!(!encoded.contains("search.example.invalid"));
        assert!(!encoded.contains("hidden-base"));
        assert!(!encoded.contains("fixture-key-id"));
        assert!(!encoded.contains("fixture-api-key"));
        assert!(!encoded.contains(&encoded_header));
    }

    #[tokio::test]
    async fn rejects_wrong_plugin_id() {
        let config = EsConfig::default();
        let mut invocation = invocation("diagnostics", json!({}));
        invocation.plugin_id = "mongodb".into();

        let error = invoke_elasticsearch_capability(&config, invocation)
            .await
            .expect_err("plugin mismatch");

        assert_eq!(error.category, CapabilityErrorCategory::Validation);
        assert_eq!(error.code, "validation.plugin_mismatch");
    }

    fn invocation(capability_id: &str, input: Value) -> CapabilityInvocation {
        CapabilityInvocation {
            id: format!("invoke-{}", capability_id),
            plugin_id: PLUGIN_ID.into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::FromProfile {
                profile: ConnectionProfileRef::Name("es".into()),
                purpose: ConnectionInstancePurpose::CapabilityInvocation,
                reuse: InstanceReusePolicy::Allow,
                options: Value::Null,
            },
            input,
            controls: InvocationControls::default(),
            actor: Some(ActorRef {
                id: "test-agent".into(),
                actor_type: ActorType::Agent,
            }),
            requested_at: Utc::now(),
        }
    }
}
