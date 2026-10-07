//! Elasticsearch fixture-backed capability smoke driver.
//!
//! This example is script-facing. It exercises the Elasticsearch plugin
//! capability surface against a disposable local Elasticsearch fixture using
//! only the generated index and documents from the fixture environment.

#![allow(clippy::result_large_err)]

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use serde_json::{Value, json};
use voidb_core::{
    ActorRef, ActorType, AgentSessionBinding, AgentSessionCallRequest, AgentSessionOpenContext,
    AgentSessionOpenRequest, AgentSessionRef, CapabilityError, CapabilityErrorCategory,
    CapabilityInvocation, CapabilityInvocationResult, InvocationAcknowledgement,
    InvocationConnectionTarget, InvocationControls, InvocationStatus, Pagination,
    PluginAgentSession, PluginAgentSessionFactory, PluginSessionHealth, PluginSessionPurpose,
    RedactionStatus,
};
use voidb_plugin_elasticsearch::{
    EsAgentSessionFactory, EsAuth, EsConfig, invoke_elasticsearch_capability,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = config_from_env()?;
    let index = required_env("VOIDB_ES_SMOKE_INDEX")?;
    let seed_document_id = required_env("VOIDB_ES_SMOKE_DOCUMENT_ID")?;
    ensure!(
        index.starts_with("voidb-fixture-"),
        "refusing Elasticsearch smoke outside a generated fixture index"
    );

    let run_id = std::env::var("VOIDB_FIXTURE_RUN_ID")
        .unwrap_or_else(|_| "elasticsearch-fixture-smoke".into());
    let safe_id = elasticsearch_safe_id(&run_id);
    let scratch_id = format!("fixture-{safe_id}-scratch");
    let dry_secret = "voidb-elasticsearch-fixture-dry-run-secret";

    let diagnostics = invoke_checked(
        &config,
        "diagnostics",
        json!({}),
        false,
        false,
        None,
        "elasticsearch.diagnostics",
    )
    .await?;
    ensure_succeeded(&diagnostics, "elasticsearch.diagnostics")?;
    ensure!(
        diagnostics.output["url_count"] == 1
            && diagnostics.output["primary_url_scheme"] == "http"
            && diagnostics.output["auth_type"].is_null()
            && diagnostics.output["verify_ssl"] == false
            && diagnostics.output["network_checked"] == false,
        "diagnostics should return shape-only metadata: {}",
        diagnostics.output
    );
    ensure_result_excludes(
        &diagnostics,
        secret_samples(&config),
        "elasticsearch.diagnostics",
    )?;

    let dry_raw = invoke_checked(
        &unavailable_config(),
        "raw_api",
        json!({
            "method": "POST",
            "path": format!("/{index}/_doc/fixture-{safe_id}-dry-raw"),
            "body": { "name": dry_secret, "fixture": true }
        }),
        true,
        false,
        None,
        "elasticsearch.raw_api dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&dry_raw, "elasticsearch.raw_api dry-run")?;
    ensure!(
        dry_raw.output["dry_run"] == true
            && dry_raw.output["would_execute"] == true
            && dry_raw.output["destructive"] == true,
        "raw_api dry-run output: {}",
        dry_raw.output
    );
    ensure_result_excludes(
        &dry_raw,
        vec![dry_secret.to_string()],
        "elasticsearch.raw_api dry-run",
    )?;

    let health = invoke_checked(
        &config,
        "health",
        json!({}),
        false,
        false,
        None,
        "elasticsearch.health",
    )
    .await?;
    ensure_succeeded(&health, "elasticsearch.health")?;
    ensure!(
        matches!(health.output["status"].as_str(), Some("yellow" | "green"))
            && health.output["node_count"].as_u64().unwrap_or_default() >= 1,
        "health should report a ready local node: {}",
        health.output
    );

    let nodes = invoke_checked(
        &config,
        "nodes",
        json!({}),
        false,
        false,
        Some(Pagination {
            limit: 5,
            cursor: None,
        }),
        "elasticsearch.nodes",
    )
    .await?;
    ensure_succeeded(&nodes, "elasticsearch.nodes")?;
    ensure!(
        nodes.output["item_count"].as_u64().unwrap_or_default() >= 1,
        "nodes should report the local fixture node: {}",
        nodes.output
    );

    let indices = invoke_checked(
        &config,
        "indices",
        json!({}),
        false,
        false,
        Some(Pagination {
            limit: 20,
            cursor: None,
        }),
        "elasticsearch.indices",
    )
    .await?;
    ensure_succeeded(&indices, "elasticsearch.indices")?;
    ensure_index_present(&indices.output, &index)?;

    let paged_search = invoke_checked(
        &config,
        "search",
        json!({
            "index": index,
            "query": {
                "query": { "match_all": {} },
                "sort": [{ "name.keyword": "asc" }]
            }
        }),
        false,
        false,
        Some(Pagination {
            limit: 2,
            cursor: None,
        }),
        "elasticsearch.search paged",
    )
    .await?;
    ensure_succeeded(&paged_search, "elasticsearch.search paged")?;
    ensure!(
        paged_search.output["item_count"].as_u64() == Some(2)
            && paged_search.output["total"].as_u64() == Some(3)
            && paged_search.output["truncated"] == true
            && paged_search.output["next_cursor"].as_str() == Some("2"),
        "search should honor pagination: {}",
        paged_search.output
    );
    ensure_hit_present(&paged_search.output, &seed_document_id)?;

    let next_page = invoke_checked(
        &config,
        "search",
        json!({
            "index": index,
            "query": {
                "query": { "match_all": {} },
                "sort": [{ "name.keyword": "asc" }]
            }
        }),
        false,
        false,
        Some(Pagination {
            limit: 2,
            cursor: Some("2".into()),
        }),
        "elasticsearch.search next page",
    )
    .await?;
    ensure_succeeded(&next_page, "elasticsearch.search next page")?;
    ensure!(
        next_page.output["item_count"].as_u64() == Some(1)
            && next_page.output["truncated"] == false,
        "search next page should return remaining document: {}",
        next_page.output
    );

    let get = invoke_checked(
        &config,
        "get",
        json!({ "index": index, "id": seed_document_id }),
        false,
        false,
        None,
        "elasticsearch.get seed document",
    )
    .await?;
    ensure_succeeded(&get, "elasticsearch.get")?;
    ensure!(
        get.output["id"] == seed_document_id && get.output["source_summary"]["kind"] == "object",
        "get should return the seeded document summary: {}",
        get.output
    );

    let count_active = invoke_checked(
        &config,
        "count",
        json!({
            "index": index,
            "query": { "query": { "term": { "active": true } } }
        }),
        false,
        false,
        None,
        "elasticsearch.count active documents",
    )
    .await?;
    ensure_succeeded(&count_active, "elasticsearch.count")?;
    ensure!(
        count_active.output["count"].as_u64() == Some(2),
        "count should report active seed documents: {}",
        count_active.output
    );

    let mapping = invoke_checked(
        &config,
        "mapping",
        json!({ "index": index }),
        false,
        false,
        None,
        "elasticsearch.mapping",
    )
    .await?;
    ensure_succeeded(&mapping, "elasticsearch.mapping")?;
    ensure!(
        mapping.output["raw_omitted"] == true
            && mapping.output["property_count"]
                .as_u64()
                .unwrap_or_default()
                >= 5,
        "mapping should omit raw mapping and expose property names: {}",
        mapping.output
    );
    ensure_property_present(&mapping.output, "name")?;
    ensure_property_present(&mapping.output, "active")?;

    let version = invoke_checked(
        &config,
        "raw_api",
        json!({ "method": "GET", "path": "/" }),
        false,
        true,
        None,
        "elasticsearch.raw_api root version",
    )
    .await?;
    ensure_succeeded(&version, "elasticsearch.raw_api root")?;
    ensure!(
        version.output["body"]["version"]["number"]
            .as_str()
            .is_some_and(|number| number.starts_with("8.")),
        "fixture should expose an Elasticsearch 8.x compatibility version: {}",
        version.output
    );

    cleanup_document(&config, &index, &scratch_id).await;
    let create_scratch = invoke_checked(
        &config,
        "raw_api",
        json!({
            "method": "PUT",
            "path": format!("/{index}/_doc/{scratch_id}?refresh=true"),
            "body": {
                "name": "scratch fixture document",
                "active": true,
                "fixture": true,
                "fixture_kind": "scratch"
            }
        }),
        false,
        true,
        None,
        "elasticsearch.raw_api scratch put",
    )
    .await?;
    ensure_succeeded(&create_scratch, "elasticsearch.raw_api scratch put")?;
    ensure!(
        create_scratch.output["body_summary"]["kind"] == "object",
        "scratch put should return shape-only summary: {}",
        create_scratch.output
    );

    let scratch = invoke_checked(
        &config,
        "get",
        json!({ "index": index, "id": scratch_id }),
        false,
        false,
        None,
        "elasticsearch.get scratch document",
    )
    .await?;
    ensure_succeeded(&scratch, "elasticsearch.get scratch")?;

    let delete_scratch = invoke_checked(
        &config,
        "raw_api",
        json!({ "method": "DELETE", "path": format!("/{index}/_doc/{scratch_id}?refresh=true") }),
        false,
        true,
        None,
        "elasticsearch.raw_api scratch delete",
    )
    .await?;
    ensure_succeeded(&delete_scratch, "elasticsearch.raw_api scratch delete")?;

    ensure_missing_index_error_redacts(&config, &index).await?;
    ensure_unavailable_target_redacts().await?;
    run_live_session_smoke(&config, &index, &seed_document_id, &safe_id).await?;

    println!("elasticsearch fixture capability smoke passed");
    println!(
        "capabilities: diagnostics, health, nodes, indices, search, get, count, mapping, raw_api dry-run, raw_api acknowledged put/delete, search_stream_read, bulk"
    );
    println!("fixture_index: generated");
    Ok(())
}

async fn run_live_session_smoke(
    config: &EsConfig,
    index: &str,
    seed_document_id: &str,
    safe_id: &str,
) -> Result<()> {
    run_pit_resume_and_expiry_smoke(config, index).await?;
    run_scroll_cleanup_smoke(config, index).await?;
    run_live_redaction_smoke(config, index, safe_id).await?;
    run_bulk_partial_failure_smoke(config, index, seed_document_id, safe_id).await?;
    Ok(())
}

async fn run_pit_resume_and_expiry_smoke(config: &EsConfig, index: &str) -> Result<()> {
    let factory = EsAgentSessionFactory::new(config.clone());
    let parameters = json!({
        "mode": "pit",
        "query": { "match_all": {} },
        "sort": [{ "name.keyword": "asc" }, "_shard_doc"],
        "batch_size": 1,
        "keep_alive_ms": 60000
    });
    let session = factory
        .open(es_live_context(json!({
            "resource": { "index": index },
            "parameters": parameters
        })))
        .await
        .map_err(|error| anyhow::anyhow!("open Elasticsearch PIT session: {error}"))?;
    let first = es_live_call(
        session.as_ref(),
        "elasticsearch-pit-first",
        json!({ "max_events": 1, "max_bytes": 65536, "wait_timeout_ms": 1000 }),
    )
    .await?;
    ensure!(
        first["events"]
            .as_array()
            .is_some_and(|events| events.len() == 1),
        "Elasticsearch PIT did not return the first hit: {first}"
    );
    ensure!(
        first["dropped_events"] == 0 && first["coalesced_events"] == 0,
        "Elasticsearch source-paced search reported buffered loss: {first}"
    );
    let first_id = first["events"][0]["data"]["id"]
        .as_str()
        .context("Elasticsearch PIT first hit ID")?
        .to_string();
    let resume = first["checkpoint"]["cursor"].clone();
    ensure!(
        resume["scope"]
            .as_str()
            .is_some_and(|scope| scope.starts_with("sha256:"))
    );
    session
        .close("fixture PIT resume transition".into())
        .await?;
    session.close("fixture PIT repeated cleanup".into()).await?;
    ensure!(session.health().await? == PluginSessionHealth::Closed);

    let resumed = factory
        .open(es_live_context(json!({
            "resource": { "index": index },
            "parameters": parameters,
            "resume_from": resume
        })))
        .await
        .map_err(|error| anyhow::anyhow!("resume Elasticsearch PIT session: {error}"))?;
    let second = es_live_call(
        resumed.as_ref(),
        "elasticsearch-pit-resumed",
        json!({ "max_events": 1, "max_bytes": 65536, "wait_timeout_ms": 1000 }),
    )
    .await?;
    ensure!(
        second["events"][0]["data"]["id"].as_str() != Some(first_id.as_str()),
        "Elasticsearch PIT resume repeated an accepted hit: {second}"
    );
    resumed.close("fixture PIT cleanup".into()).await?;

    let expiring_parameters = json!({
        "mode": "pit",
        "query": { "match_all": {} },
        "sort": ["_shard_doc"],
        "batch_size": 1,
        "keep_alive_ms": 1000
    });
    let expiring = factory
        .open(es_live_context(json!({
            "resource": { "index": index },
            "parameters": expiring_parameters
        })))
        .await
        .map_err(|error| anyhow::anyhow!("open expiring Elasticsearch PIT: {error}"))?;
    let expiring_batch = es_live_call(
        expiring.as_ref(),
        "elasticsearch-pit-expiring",
        json!({ "max_events": 1, "max_bytes": 65536, "wait_timeout_ms": 1000 }),
    )
    .await?;
    let expired_cursor = expiring_batch["checkpoint"]["cursor"].clone();
    expiring
        .close("fixture PIT expiry transition".into())
        .await?;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let expired = factory
        .open(es_live_context(json!({
            "resource": { "index": index },
            "parameters": expiring_parameters,
            "resume_from": expired_cursor
        })))
        .await;
    ensure!(
        expired.is_err(),
        "Elasticsearch accepted an expired continuation"
    );
    Ok(())
}

async fn run_scroll_cleanup_smoke(config: &EsConfig, index: &str) -> Result<()> {
    let baseline = search_open_contexts(config).await?;
    let factory = EsAgentSessionFactory::new(config.clone());
    let session = factory
        .open(es_live_context(json!({
            "resource": { "index": index },
            "parameters": {
                "mode": "scroll",
                "query": { "match_all": {} },
                "sort": ["_doc"],
                "batch_size": 1,
                "keep_alive_ms": 60000
            }
        })))
        .await
        .map_err(|error| anyhow::anyhow!("open Elasticsearch scroll session: {error}"))?;
    let first = es_live_call(
        session.as_ref(),
        "elasticsearch-scroll-first",
        json!({ "max_events": 1, "max_bytes": 65536, "wait_timeout_ms": 1000 }),
    )
    .await?;
    ensure!(
        first["events"]
            .as_array()
            .is_some_and(|events| events.len() == 1),
        "Elasticsearch scroll did not return a hit: {first}"
    );
    let opened = search_open_contexts(config).await?;
    ensure!(
        opened > baseline,
        "Elasticsearch scroll did not expose an owned search context"
    );
    session.close("fixture scroll cleanup".into()).await?;
    wait_for_open_contexts(config, baseline).await?;
    ensure!(session.health().await? == PluginSessionHealth::Closed);
    Ok(())
}

async fn run_live_redaction_smoke(config: &EsConfig, index: &str, safe_id: &str) -> Result<()> {
    let document_id = format!("fixture-{safe_id}-live-redaction");
    cleanup_document(config, index, &document_id).await;
    let protected_value = config.primary_url().to_string();
    let created = invoke_checked(
        config,
        "raw_api",
        json!({
            "method": "PUT",
            "path": format!("/{index}/_doc/{document_id}?refresh=true"),
            "body": {
                "name": "live redaction fixture",
                "protected_probe": protected_value,
                "fixture": true
            }
        }),
        false,
        true,
        None,
        "Elasticsearch live redaction fixture put",
    )
    .await?;
    ensure_succeeded(&created, "Elasticsearch live redaction fixture put")?;

    let factory = EsAgentSessionFactory::new(config.clone());
    let session = factory
        .open(es_live_context(json!({
            "resource": { "index": index },
            "parameters": {
                "mode": "pit",
                "query": { "ids": { "values": [document_id] } },
                "sort": ["_shard_doc"],
                "batch_size": 1,
                "keep_alive_ms": 60000
            }
        })))
        .await
        .map_err(|error| anyhow::anyhow!("open Elasticsearch redaction PIT: {error}"))?;
    let batch = es_live_call(
        session.as_ref(),
        "elasticsearch-live-redaction",
        json!({ "max_events": 1, "max_bytes": 65536, "wait_timeout_ms": 1000 }),
    )
    .await?;
    ensure!(
        !serde_json::to_string(&batch)?.contains(&protected_value),
        "Elasticsearch live hit exposed configured target material"
    );
    ensure!(
        batch["events"][0]["redaction"] == "applied",
        "Elasticsearch live hit did not disclose redaction: {batch}"
    );
    session.close("fixture redaction cleanup".into()).await?;
    cleanup_document(config, index, &document_id).await;
    Ok(())
}

async fn run_bulk_partial_failure_smoke(
    config: &EsConfig,
    index: &str,
    seed_document_id: &str,
    safe_id: &str,
) -> Result<()> {
    let successful_id = format!("fixture-{safe_id}-bulk-success");
    cleanup_document(config, index, &successful_id).await;
    let result = invoke_checked(
        config,
        "bulk",
        json!({
            "index": index,
            "operations": [
                {
                    "type": "create",
                    "id": seed_document_id,
                    "document": { "fixture_kind": "duplicate" }
                },
                {
                    "type": "create",
                    "id": successful_id,
                    "document": { "fixture_kind": "bulk-success" }
                }
            ]
        }),
        false,
        true,
        None,
        "Elasticsearch bulk partial failure",
    )
    .await?;
    ensure_succeeded(&result, "Elasticsearch bulk partial failure")?;
    ensure!(
        result.output["details"]["successful"] == 1 && result.output["details"]["failed"] == 1,
        "Elasticsearch bulk partial failure was not machine-readable: {}",
        result.output
    );
    ensure!(
        result.output["details"]["items"]
            .as_array()
            .is_some_and(|items| {
                items.len() == 2
                    && items.iter().any(|item| item["ok"] == false)
                    && items.iter().any(|item| item["ok"] == true)
            }),
        "Elasticsearch bulk item results were incomplete: {}",
        result.output
    );
    cleanup_document(config, index, &successful_id).await;
    Ok(())
}

fn es_live_context(input: Value) -> AgentSessionOpenContext {
    let purpose = PluginSessionPurpose::DatabaseQuery;
    AgentSessionOpenContext {
        binding: AgentSessionBinding {
            grant_id: "elasticsearch-fixture-live-grant".into(),
            profile_id: "elasticsearch-fixture-profile".into(),
            plugin_id: "elasticsearch".into(),
            purpose: purpose.clone(),
            allowed_capabilities: vec!["elasticsearch.search_stream_read".into()],
            host_generation: 1,
        },
        request: AgentSessionOpenRequest {
            purpose,
            capabilities: vec!["elasticsearch.search_stream_read".into()],
            lease_seconds: 60,
            concurrency: Default::default(),
            destructive_acknowledged: false,
            input,
        },
        lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
    }
}

async fn es_live_call(
    session: &dyn PluginAgentSession,
    call_id: &str,
    input: Value,
) -> Result<Value> {
    session
        .call(AgentSessionCallRequest {
            session: AgentSessionRef::new("elasticsearch-fixture-live-session", 1),
            call_id: call_id.into(),
            capability: "elasticsearch.search_stream_read".into(),
            input,
            destructive_acknowledged: false,
            timeout_ms: Some(30_000),
            output_limit_bytes: 65536,
        })
        .await
        .map(|result| result.output)
        .map_err(|error| {
            anyhow::anyhow!("Elasticsearch live-session call {call_id} failed: {error}")
        })
}

async fn search_open_contexts(config: &EsConfig) -> Result<u64> {
    let result = invoke_checked(
        config,
        "raw_api",
        json!({ "method": "GET", "path": "/_nodes/stats/indices/search" }),
        false,
        true,
        None,
        "Elasticsearch search context stats",
    )
    .await?;
    ensure_succeeded(&result, "Elasticsearch search context stats")?;
    let nodes = result.output["body"]["nodes"]
        .as_object()
        .context("Elasticsearch node search stats")?;
    Ok(nodes
        .values()
        .map(|node| {
            node.pointer("/indices/search/open_contexts")
                .and_then(Value::as_u64)
                .unwrap_or_default()
        })
        .sum())
}

async fn wait_for_open_contexts(config: &EsConfig, expected_maximum: u64) -> Result<()> {
    for _ in 0..20 {
        if search_open_contexts(config).await? <= expected_maximum {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    bail!("Elasticsearch search context cleanup did not complete")
}

fn config_from_env() -> Result<EsConfig> {
    Ok(EsConfig {
        urls: vec![required_env("VOIDB_ES_SMOKE_URL")?],
        auth: None,
        timeout: 10,
        verify_ssl: false,
    })
}

fn unavailable_config() -> EsConfig {
    EsConfig {
        urls: vec!["http://127.0.0.1:1/voidb-es-secret-path".into()],
        auth: Some(EsAuth::Bearer {
            token: "voidb-es-secret-token".into(),
        }),
        timeout: 1,
        verify_ssl: false,
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

async fn invoke(
    config: &EsConfig,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    acknowledged: bool,
    page: Option<Pagination>,
) -> std::result::Result<CapabilityInvocationResult, CapabilityError> {
    invoke_elasticsearch_capability(
        config,
        CapabilityInvocation {
            id: format!("elasticsearch-fixture-smoke-{capability_id}"),
            plugin_id: "elasticsearch".into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::Stateless,
            input,
            controls: InvocationControls {
                dry_run,
                acknowledgement: acknowledged.then(acknowledgement),
                page,
                ..InvocationControls::default()
            },
            actor: Some(actor()),
            requested_at: Utc::now(),
        },
    )
    .await
}

async fn invoke_checked(
    config: &EsConfig,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    acknowledged: bool,
    page: Option<Pagination>,
    label: &str,
) -> Result<CapabilityInvocationResult> {
    invoke(config, capability_id, input, dry_run, acknowledged, page)
        .await
        .map_err(|error| {
            let error_json = serde_json::to_string(&error).unwrap_or_else(|_| format!("{error:?}"));
            anyhow::anyhow!("{label}: {error_json}")
        })
}

fn ensure_succeeded(result: &CapabilityInvocationResult, label: &str) -> Result<()> {
    ensure!(
        result.status == InvocationStatus::Succeeded,
        "{label} returned non-success status: {:?}",
        result.status
    );
    Ok(())
}

fn ensure_result_excludes(
    result: &CapabilityInvocationResult,
    samples: Vec<String>,
    label: &str,
) -> Result<()> {
    let text = serde_json::to_string(result)?;
    for sample in samples {
        ensure!(
            !text.contains(&sample),
            "{label} exposed protected Elasticsearch sample {sample}: {text}"
        );
    }
    Ok(())
}

fn ensure_error_excludes_config(
    error: &CapabilityError,
    config: &EsConfig,
    label: &str,
) -> Result<()> {
    let text = serde_json::to_string(error)?;
    for sample in error_samples(config) {
        ensure!(
            !text.contains(&sample),
            "{label} error exposed Elasticsearch config material {sample}: {text}"
        );
    }
    ensure!(
        matches!(
            error.redaction,
            RedactionStatus::Applied | RedactionStatus::NotRequired
        ),
        "{label} redaction status should be non-failed: {:?}",
        error.redaction
    );
    Ok(())
}

fn secret_samples(config: &EsConfig) -> Vec<String> {
    let mut samples = config.urls.clone();
    if let Some(auth) = &config.auth {
        match auth {
            EsAuth::Basic { username, password } => {
                samples.push(username.clone());
                samples.push(password.clone());
            }
            EsAuth::ApiKey { id, api_key } => {
                samples.push(id.clone());
                samples.push(api_key.clone());
            }
            EsAuth::Bearer { token } => samples.push(token.clone()),
        }
    }
    samples
        .into_iter()
        .filter(|sample| sample.len() >= 4)
        .collect()
}

fn error_samples(config: &EsConfig) -> Vec<String> {
    let mut samples = secret_samples(config);
    for url in &config.urls {
        if let Some(authority) = url_authority(url) {
            samples.push(authority);
        }
        if let Some(path) = url_path(url) {
            samples.push(path);
        }
    }
    samples
        .into_iter()
        .filter(|sample| sample.len() >= 4)
        .collect()
}

fn url_authority(url: &str) -> Option<String> {
    let (_, rest) = url.split_once("://")?;
    let authority = rest.split(['/', '?']).next()?.to_string();
    (!authority.is_empty()).then_some(authority)
}

fn url_path(url: &str) -> Option<String> {
    let (_, rest) = url.split_once("://")?;
    let (_, path_and_query) = rest.split_once('/')?;
    path_and_query
        .split('?')
        .next()
        .filter(|path| path.len() >= 4)
        .map(str::to_string)
}

fn ensure_index_present(output: &Value, index: &str) -> Result<()> {
    let indices = output["indices"]
        .as_array()
        .context("elasticsearch.indices output should include indices array")?;
    ensure!(
        indices.iter().any(|item| item["name"] == index),
        "elasticsearch.indices did not include expected index {index}: {output}"
    );
    Ok(())
}

fn ensure_hit_present(output: &Value, document_id: &str) -> Result<()> {
    let hits = output["hits"]
        .as_array()
        .context("elasticsearch.search output should include hits array")?;
    ensure!(
        hits.iter().any(|item| item["id"] == document_id),
        "elasticsearch.search did not include expected document {document_id}: {output}"
    );
    Ok(())
}

fn ensure_property_present(output: &Value, property: &str) -> Result<()> {
    let properties = output["properties"]
        .as_array()
        .context("elasticsearch.mapping output should include properties array")?;
    ensure!(
        properties.iter().any(|item| item == property),
        "elasticsearch.mapping did not include expected property {property}: {output}"
    );
    Ok(())
}

async fn ensure_missing_index_error_redacts(config: &EsConfig, index: &str) -> Result<()> {
    let missing_index = format!("{index}-missing");
    match invoke(
        config,
        "search",
        json!({ "index": missing_index, "query": { "query": { "match_all": {} } } }),
        false,
        false,
        Some(Pagination {
            limit: 5,
            cursor: None,
        }),
    )
    .await
    {
        Ok(result) => bail!(
            "expected elasticsearch.search missing-index failure, got output: {}",
            result.output
        ),
        Err(error) => {
            ensure!(
                error.category == CapabilityErrorCategory::TargetSystem,
                "missing index should be a target error: {:?}",
                error.category
            );
            ensure_error_excludes_config(&error, config, "elasticsearch.search missing index")?;
        }
    }
    Ok(())
}

async fn ensure_unavailable_target_redacts() -> Result<()> {
    let unavailable = unavailable_config();
    match invoke(&unavailable, "health", json!({}), false, false, None).await {
        Ok(result) => bail!(
            "expected elasticsearch.health unavailable failure, got output: {}",
            result.output
        ),
        Err(error) => {
            ensure!(
                error.category == CapabilityErrorCategory::TargetSystem,
                "unavailable target should be a target error: {:?}",
                error.category
            );
            ensure_error_excludes_config(&error, &unavailable, "elasticsearch.health unavailable")?;
            ensure!(
                error.redaction == RedactionStatus::Applied,
                "unavailable target should redact configured URL/auth material: {:?}",
                error.redaction
            );
        }
    }
    Ok(())
}

async fn cleanup_document(config: &EsConfig, index: &str, document_id: &str) {
    let _ = invoke(
        config,
        "raw_api",
        json!({ "method": "DELETE", "path": format!("/{index}/_doc/{document_id}?refresh=true") }),
        false,
        true,
        None,
    )
    .await;
}

fn elasticsearch_safe_id(value: &str) -> String {
    let mut safe = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while safe.contains("--") {
        safe = safe.replace("--", "-");
    }
    safe.trim_matches('-').chars().take(32).collect()
}

fn actor() -> ActorRef {
    ActorRef {
        id: "agent:elasticsearch-fixture-smoke".into(),
        actor_type: ActorType::Agent,
    }
}

fn acknowledgement() -> InvocationAcknowledgement {
    InvocationAcknowledgement {
        actor: actor(),
        acknowledged_at: Utc::now(),
        reason: Some("fixture smoke mutation scoped to generated Elasticsearch index".into()),
        approval_id: Some("elasticsearch-fixture-smoke".into()),
    }
}
