//! Standalone Elasticsearch search-triage data-inspector TUI (Requirement 83
//! pilot).
//!
//! The pilot follows the retained family TUI pattern (see the Redis plugin):
//! overlay-first routing, a non-modal error banner, per-view selection memory,
//! a persistent hint footer, full-screen scrollable viewers, confirm-quit,
//! loading markers, Length(4) header/status panels, fixture launch with
//! `build_elasticsearch_tui_evidence`, a secret-leak scan, and secret-free
//! preflight JSON. All reads and the single supported mutation go through the
//! existing `EsService` channel contract; the TUI adds no backend operations.
//!
//! Scope is deliberately bounded to search triage: index browsing with doc
//! counts, paged hit browsing through `DocumentCommand::Search` with explicit
//! `size`/`from`, typed hit rendering, mapping inspection, cluster
//! health/nodes, and a single bounded delete-by-`_id`. Index management DDL,
//! document writes, `delete_by_query`, bulk deletes, and raw-API access are
//! deferred as `later candidate`; the CLI remains the supported general path.

use std::collections::VecDeque;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use voidb_core::{TabInfo, TabManager, TuiLaunchPlan, retained_tui_quality_gate};

use crate::config::EsConfig;
use crate::service::commands::{ClusterCommand, DocumentCommand, IndexCommand};
use crate::service::events::{ClusterEvent, DocumentEvent, IndexEvent};
use crate::service::types::{
    ClusterHealth, IndexHealth, IndexInfo, MappingResult, NodeInfo, SearchHit,
};
use crate::service::{EsCommand, EsEvent, EsService};

/// One bounded search page. Explicit `size`/`from` only; pages are replaced,
/// never appended.
const HITS_PAGE_SIZE: u32 = 10;
/// Cursor movement step for PgUp/PgDn in list views.
const PAGE_STEP: isize = 10;
/// Bounded viewer budgets (same contract as the family TUIs).
const VIEWER_LINE_LIMIT: usize = 2_000;
const VIEWER_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const INPUT_LIMIT: usize = 128;

const INDICES_HINTS: &str = "1 indices 2 cluster j/k / filter i hits m mapping r refresh ? q quit";
const CLUSTER_HINTS: &str = "1 indices 2 cluster j/k i viewer r refresh ? q quit";
const HITS_HINTS: &str = "n next b back j/k / filter Enter hit d delete m mapping Esc close";

/// Where the TUI launch came from. Mirrors the family `RedisTuiSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EsTuiSource {
    Profile,
    Connection,
    Fixture,
}

/// Launch description for the standalone Elasticsearch search TUI. Mirrors the
/// family launch shape.
#[derive(Debug, Clone)]
pub struct EsTuiLaunch {
    pub profile_label: String,
    pub config: Option<EsConfig>,
    pub source: EsTuiSource,
    pub fixture_path: Option<String>,
    pub purpose: String,
    pub readonly: bool,
    pub restore: bool,
    pub launch_plan: Option<TuiLaunchPlan>,
}

// ---------------------------------------------------------------------------
// Fixture model
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct EsTuiFixture {
    profile_label: Option<String>,
    server_label: String,
    connection_kind: String,
    status: Option<String>,
    target_error: Option<String>,
    #[serde(default)]
    error_docs: Vec<FixtureErrorDoc>,
    error_message: Option<String>,
    #[serde(default)]
    indices: Vec<FixtureIndex>,
    #[serde(default)]
    search_pages: Vec<FixtureSearchPage>,
    #[serde(default)]
    mappings: Vec<FixtureMapping>,
    cluster_health: Option<FixtureClusterHealth>,
    #[serde(default)]
    nodes: Vec<FixtureNode>,
}

#[derive(Debug, Deserialize)]
struct FixtureErrorDoc {
    index: String,
    id: String,
}

#[derive(Debug, Deserialize)]
struct FixtureIndex {
    name: String,
    health: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    doc_count: u64,
    #[serde(default)]
    store_size: String,
    #[serde(default)]
    pri_shards: u32,
    #[serde(default)]
    rep_shards: u32,
}

#[derive(Debug, Deserialize)]
struct FixtureHit {
    id: String,
    #[serde(default)]
    index: String,
    #[serde(default)]
    score: Option<f64>,
    #[serde(default)]
    source: Value,
}

#[derive(Debug, Deserialize)]
struct FixtureSearchPage {
    index: String,
    from: u32,
    total: u64,
    #[serde(default)]
    hits: Vec<FixtureHit>,
}

#[derive(Debug, Deserialize)]
struct FixtureMapping {
    index: String,
    #[serde(default)]
    properties: Vec<String>,
    #[serde(default)]
    raw: Value,
}

#[derive(Debug, Deserialize)]
struct FixtureClusterHealth {
    cluster_name: String,
    status: String,
    node_count: u32,
    active_shards: u32,
    #[serde(default)]
    unassigned_shards: u32,
}

#[derive(Debug, Deserialize)]
struct FixtureNode {
    name: String,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    heap_percent: u32,
    #[serde(default)]
    disk_used: String,
    #[serde(default)]
    cpu_percent: u32,
}

/// In-memory fixture state, normalized into the service-layer types so the
/// viewer code is shared with live mode.
struct EsTuiData {
    profile_label: String,
    server_label: String,
    connection_kind: String,
    status: String,
    target_error: Option<String>,
    error_docs: Vec<(String, String)>,
    error_message: String,
    indices: Vec<IndexInfo>,
    search_pages: Vec<FixtureSearchPageState>,
    mappings: Vec<MappingResult>,
    cluster_health: Option<ClusterHealth>,
    nodes: Vec<NodeInfo>,
}

struct FixtureSearchPageState {
    index: String,
    from: u32,
    total: u64,
    hits: Vec<SearchHit>,
}

impl EsTuiData {
    /// Resolve one bounded search page against the fixture index space.
    /// Unknown offsets return an empty page with the index total so the TUI
    /// always has a visible empty state to render.
    fn resolve_search(&self, index: &str, from: u32) -> (Vec<SearchHit>, u64) {
        let mut total = 0u64;
        for page in &self.search_pages {
            if page.index != index {
                continue;
            }
            total = total.max(page.total);
            if page.from == from {
                return (page.hits.clone(), page.total);
            }
        }
        (Vec::new(), total)
    }

    fn mapping(&self, index: &str) -> Option<&MappingResult> {
        self.mappings.iter().find(|m| m.index == index)
    }

    /// Apply the bounded single-document delete to the fixture index space.
    /// Documents listed in `error_docs` deliberately fail so the TUI can prove
    /// a target error keeps it responsive.
    fn apply_delete(&mut self, index: &str, id: &str) -> std::result::Result<String, String> {
        if self
            .error_docs
            .iter()
            .any(|(doc_index, doc_id)| doc_index == index && doc_id == id)
        {
            return Err(self.error_message.clone());
        }
        let mut removed = 0usize;
        for page in &mut self.search_pages {
            if page.index != index {
                continue;
            }
            let before = page.hits.len();
            page.hits.retain(|hit| hit.id != id);
            if page.hits.len() != before {
                removed += before - page.hits.len();
                page.total = page.total.saturating_sub(1);
            }
        }
        if removed == 0 {
            return Err(format!(
                "target error: document {index}/{id} not found in the fixture index"
            ));
        }
        for info in &mut self.indices {
            if info.name == index && info.doc_count > 0 {
                info.doc_count -= 1;
            }
        }
        Ok("document removed from the fixture index".to_string())
    }
}

fn load_fixture(path: &str) -> Result<EsTuiData> {
    let text = fs::read_to_string(path).with_context(|| format!("read fixture {path}"))?;
    let fixture: EsTuiFixture =
        serde_json::from_str(&text).with_context(|| format!("parse fixture {path}"))?;
    Ok(EsTuiData {
        profile_label: fixture
            .profile_label
            .unwrap_or_else(|| "fixture-elasticsearch".to_string()),
        server_label: fixture.server_label,
        connection_kind: fixture.connection_kind,
        status: fixture
            .status
            .unwrap_or_else(|| "fixture elasticsearch operations ready".to_string()),
        target_error: fixture.target_error,
        error_docs: fixture
            .error_docs
            .into_iter()
            .map(|doc| (doc.index, doc.id))
            .collect(),
        error_message: fixture
            .error_message
            .unwrap_or_else(|| "target error: fixture action rejected".to_string()),
        indices: fixture
            .indices
            .into_iter()
            .map(|index| IndexInfo {
                name: index.name,
                health: IndexHealth::from_str(&index.health),
                status: index.status,
                doc_count: index.doc_count,
                store_size: index.store_size,
                pri_shards: index.pri_shards,
                rep_shards: index.rep_shards,
            })
            .collect(),
        search_pages: fixture
            .search_pages
            .into_iter()
            .map(|page| FixtureSearchPageState {
                index: page.index,
                from: page.from,
                total: page.total,
                hits: page
                    .hits
                    .into_iter()
                    .map(|hit| SearchHit {
                        id: hit.id,
                        index: hit.index,
                        source: hit.source,
                        score: hit.score,
                    })
                    .collect(),
            })
            .collect(),
        mappings: fixture
            .mappings
            .into_iter()
            .map(|mapping| MappingResult {
                index: mapping.index,
                properties: mapping.properties,
                raw: mapping.raw,
            })
            .collect(),
        cluster_health: fixture.cluster_health.map(|health| ClusterHealth {
            cluster_name: health.cluster_name,
            status: IndexHealth::from_str(&health.status),
            node_count: health.node_count,
            active_shards: health.active_shards,
            unassigned_shards: health.unassigned_shards,
        }),
        nodes: fixture
            .nodes
            .into_iter()
            .map(|node| NodeInfo {
                name: node.name,
                roles: node.roles,
                heap_percent: node.heap_percent,
                disk_used: node.disk_used,
                cpu_percent: node.cpu_percent,
            })
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Indices,
    Cluster,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Browser,
    Filter,
}

/// Which list the client-side `/` filter applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterTarget {
    Indices,
    Hits,
}

/// Full-screen or popup surfaces stacked on top of the browser. Input is
/// routed to the active overlay first, so each surface owns its key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlay {
    None,
    Help,
    HitsViewer,
    HitViewer,
    MappingViewer,
    ClusterViewer,
    ConfirmDeleteDoc,
    ConfirmQuit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    DeleteDocument,
}

impl OperationKind {
    fn label(&self) -> &'static str {
        match self {
            OperationKind::DeleteDocument => "delete document",
        }
    }

    fn risk(&self) -> &'static str {
        match self {
            OperationKind::DeleteDocument => "destructive",
        }
    }
}

/// Staged mutation plan rendered in the details panel before execution.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationPlanView {
    kind: OperationKind,
    target_index: String,
    target_id: String,
    risk: &'static str,
    confirmations_required: u8,
    confirmations: u8,
}

/// Bounded text buffer for viewer bodies (same contract as the family TUIs).
struct BoundedLines {
    lines: VecDeque<String>,
    bytes: usize,
    dropped_lines: usize,
    max_lines: usize,
    max_bytes: usize,
}

pub fn write_elasticsearch_tui_preflight(launch: &EsTuiLaunch) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&preflight_value(launch))?
    );
    Ok(())
}

pub fn build_elasticsearch_tui_evidence(launch: &EsTuiLaunch) -> Result<Value> {
    let data = if let Some(path) = &launch.fixture_path {
        load_fixture(path)?
    } else {
        let config = launch
            .config
            .as_ref()
            .context("elasticsearch tui evidence requires a profile, connection, or fixture")?;
        config_data(launch.profile_label.clone(), config)
    };

    let mut evidence = json!({
        "schema_version": 1,
        "kind": "elasticsearch_tui_fixture_evidence",
        "quality_gate": retained_tui_quality_gate(
            "elasticsearch",
            &["fixture-elasticsearch-ops", "fixture-elasticsearch-local"],
            &["target error"],
            80,
            24,
            10_000
        ),
        "preflight": preflight_value(launch),
        "transcript": {
            "profile_label": data.profile_label,
            "server_label": data.server_label,
            "connection_kind": data.connection_kind,
            "status": data.status,
            "target_error": data.target_error,
            "indices": data.indices.iter().map(index_value).collect::<Vec<_>>(),
            "search_pages": data
                .search_pages
                .iter()
                .map(|page| {
                    json!({
                        "index": page.index,
                        "from": page.from,
                        "total": page.total,
                        "hit_count": page.hits.len(),
                        "hits": page.hits.iter().map(hit_value).collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>(),
            "mappings": data
                .mappings
                .iter()
                .map(|mapping| {
                    json!({
                        "index": mapping.index,
                        "property_count": mapping.properties.len(),
                        "properties": mapping.properties
                    })
                })
                .collect::<Vec<_>>(),
            "cluster_health": data.cluster_health.as_ref().map(health_value),
            "nodes": data.nodes.iter().map(node_value).collect::<Vec<_>>(),
            "page_bounds": {
                "hits_page_size": HITS_PAGE_SIZE,
                "viewer": { "max_lines": VIEWER_LINE_LIMIT, "max_bytes": VIEWER_BYTE_LIMIT }
            }
        },
        "plan_transcript": [
            {
                "action": "delete_document",
                "risk": "destructive",
                "scope": "single_document_by_id",
                "typed_confirmation": "exact_index_name_required",
                "id_echo": "document_id_shown_in_confirmation_and_status",
                "second_confirmation": "explicit_y",
                "readonly_behavior": "plan_visible_but_blocked",
                "post_action": "current_search_page_refetch"
            }
        ],
        "never_shipped": [
            "delete_by_query",
            "bulk_delete",
            "index_delete",
            "index_create",
            "raw_api_from_tui"
        ],
        "coverage": [
            "startup",
            "indices_cluster_tabs",
            "index_selection",
            "hits_paging_offset_history",
            "hits_viewer_typed_rendering",
            "hit_detail_viewer_pretty_source",
            "mapping_viewer",
            "cluster_health_viewer",
            "client_filter_match_count",
            "typed_delete_confirmation",
            "delete_id_echo",
            "delete_execute_refetch",
            "readonly_block",
            "erroring_document_responsive",
            "empty_index_state",
            "partial_page_state",
            "end_of_hits_state",
            "non_modal_error_banner",
            "target_error_responsive",
            "quit_confirmation",
            "resize",
            "quit_restore",
            "secret_leak_scan"
        ],
        "deferred": {
            "index_management_ddl": [
                "index_create",
                "index_delete",
                "index_stats_view",
                "aliases_view",
                "cluster_info_view"
            ],
            "document_writes": ["document_index_upsert"],
            "server_side_query_editing": "later candidate within the accepted pilot; / stays a client-side filter on the current page",
            "outcome": "later candidate within the accepted pilot",
            "supported_mutations": ["delete_document"],
            "cli_remains_supported_general_path": true
        },
        "mutation_refetch_policy": {
            "search_semantics": "one bounded search page at a time with explicit size/from (size=10); the next page is fetched with from+size",
            "after_mutation": "the current page from-offset is refetched so the deleted document disappears and totals update",
            "fixture_mutations": "applied to the in-memory fixture index so paging evidence stays deterministic"
        },
        "secret_leak_scan": null
    });

    let rendered = serde_json::to_string(&evidence)?;
    let markers = secret_leak_markers(&rendered);
    evidence["secret_leak_scan"] = json!({
        "passed": markers.is_empty(),
        "marker_count": markers.len(),
        "markers": markers
    });
    Ok(evidence)
}

pub async fn run_es_tui(launch: EsTuiLaunch) -> Result<()> {
    let mut app = EsTuiApp::new(launch)?;
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mut app);
    app.shutdown();
    ratatui::restore();
    result
}

fn preflight_value(launch: &EsTuiLaunch) -> Value {
    let fixture_data = launch
        .fixture_path
        .as_ref()
        .and_then(|path| load_fixture(path).ok());
    let connection = launch
        .config
        .as_ref()
        .map(connection_value)
        .or_else(|| {
            fixture_data.as_ref().map(|data| {
                json!({
                    "kind": data.connection_kind,
                    "server_label": data.server_label,
                    "secret_material": "redacted"
                })
            })
        })
        .unwrap_or_else(|| {
            json!({
                "kind": "fixture",
                "server_label": "fixture",
                "secret_material": "redacted"
            })
        });
    let launch_plan = launch.launch_plan.as_ref().map(|plan| {
        json!({
            "schema_version": plan.schema_version,
            "plugin_id": plan.plugin_id,
            "command": plan.command,
            "args": plan.args,
            "profile": plan.profile,
            "purpose": plan.purpose,
            "readonly": plan.readonly,
            "restore": plan.restore,
            "raw_input": plan.raw_input,
            "credential_ref_count": plan.credential_grant.credential_refs.len(),
            "credential_grant_id": plan.credential_grant.id,
            "redaction": plan.redaction
        })
    });

    json!({
        "ok": true,
        "command": "elasticsearch tui",
        "plugin_id": "elasticsearch",
        "profile_label": fixture_data
            .as_ref()
            .map(|data| data.profile_label.clone())
            .unwrap_or_else(|| launch.profile_label.clone()),
        "source": launch.source,
        "purpose": launch.purpose,
        "readonly": launch.readonly,
        "restore": launch.restore,
        "raw_input": false,
        "fixture": launch.fixture_path.is_some(),
        "connection": connection,
        "privacy": {
            "diagnostics_include_password": false,
            "diagnostics_include_api_key": false,
            "diagnostics_include_connection_url": false,
            "diagnostics_include_document_bodies": false
        },
        "stream_bounds": {
            "hits_page_size": HITS_PAGE_SIZE,
            "viewer": { "max_lines": VIEWER_LINE_LIMIT, "max_bytes": VIEWER_BYTE_LIMIT }
        },
        "service_boundary": "EsService::Channel",
        "modes": [
            "indices",
            "cluster",
            "hits_viewer",
            "hit_viewer",
            "mapping_viewer",
            "cluster_viewer",
            "filter",
            "typed_delete_confirmation",
            "confirm_quit",
            "target_error"
        ],
        "launch_plan": launch_plan
    })
}

fn run_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut EsTuiApp) -> Result<()> {
    terminal.draw(|frame| app.draw(frame))?;
    loop {
        let mut dirty = app.drain_service();
        if app.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => {
                    app.handle_key(key);
                    dirty = true;
                }
                Event::Resize(cols, rows) => {
                    app.status = format!("resized elasticsearch view to {cols}x{rows}");
                    dirty = true;
                }
                _ => {}
            }
        }
        if dirty {
            terminal.draw(|frame| app.draw(frame))?;
        }
    }
}

fn config_data(profile_label: String, config: &EsConfig) -> EsTuiData {
    EsTuiData {
        profile_label,
        server_label: url_host_label(config),
        connection_kind: "http".to_string(),
        status: "connecting through EsService channel mode".to_string(),
        target_error: None,
        error_docs: Vec::new(),
        error_message: "target error: fixture action rejected".to_string(),
        indices: Vec::new(),
        search_pages: Vec::new(),
        mappings: Vec::new(),
        cluster_health: None,
        nodes: Vec::new(),
    }
}

/// Host label derived from the primary URL with any userinfo stripped. The
/// full URL is never emitted (it can embed credentials).
fn url_host_label(config: &EsConfig) -> String {
    let raw = config.primary_url();
    let without_scheme = raw.split_once("://").map(|(_, rest)| rest).unwrap_or(raw);
    let without_userinfo = without_scheme
        .rsplit_once('@')
        .map(|(_, rest)| rest)
        .unwrap_or(without_scheme);
    let host = without_userinfo
        .split('/')
        .next()
        .unwrap_or(without_userinfo);
    if host.is_empty() {
        "elasticsearch".to_string()
    } else {
        host.to_string()
    }
}

fn connection_value(config: &EsConfig) -> Value {
    let auth = match &config.auth {
        Some(crate::config::EsAuth::Basic { username, .. }) => {
            json!({"method": "basic", "username_present": !username.is_empty()})
        }
        Some(crate::config::EsAuth::ApiKey { .. }) => {
            json!({"method": "api_key", "username_present": false})
        }
        Some(crate::config::EsAuth::Bearer { .. }) => {
            json!({"method": "bearer", "username_present": false})
        }
        None => json!({"method": "none", "username_present": false}),
    };
    json!({
        "kind": "http",
        "url_scheme": config
            .primary_url()
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .unwrap_or("http"),
        "url_host_label": url_host_label(config),
        "url_count": config.urls.len(),
        "verify_ssl": config.verify_ssl,
        "timeout_seconds": config.timeout,
        "auth": auth,
        "secret_material": "redacted"
    })
}

struct EsTuiApp {
    profile_label: String,
    server_label: String,
    connection_kind: String,
    source: EsTuiSource,
    purpose: String,
    readonly: bool,
    restore: bool,
    view: View,
    indices: Vec<IndexInfo>,
    indices_selection: usize,
    cluster_selection: usize,
    health: Option<ClusterHealth>,
    nodes: Vec<NodeInfo>,
    cluster_loaded: bool,
    // Hits state: one bounded search page for one index at a time.
    hits_index: String,
    hits_from: u32,
    hits_total: u64,
    hits: Vec<SearchHit>,
    hits_page: usize,
    hits_history: Vec<u32>,
    hits_selection: usize,
    filter: String,
    filter_target: FilterTarget,
    mode: Mode,
    overlay: Overlay,
    error_banner: Option<String>,
    indices_pending: bool,
    hits_pending: bool,
    cluster_pending: bool,
    mapping_pending: bool,
    hit_viewer_lines: BoundedLines,
    hit_viewer_title: String,
    hit_viewer_scroll: usize,
    hit_viewer_hit: Option<SearchHit>,
    mapping_lines: BoundedLines,
    mapping_title: String,
    mapping_scroll: usize,
    cluster_viewer_lines: BoundedLines,
    cluster_viewer_title: String,
    cluster_viewer_scroll: usize,
    typed_index_input: String,
    operation_plan: Option<OperationPlanView>,
    fixture: Option<EsTuiData>,
    service: Option<EsService>,
    status: String,
    should_quit: bool,
    render_quit: Arc<AtomicBool>,
}

impl EsTuiApp {
    fn new(launch: EsTuiLaunch) -> Result<Self> {
        let data = if let Some(path) = &launch.fixture_path {
            load_fixture(path)?
        } else {
            let config = launch
                .config
                .as_ref()
                .context("elasticsearch tui requires a profile, connection, or fixture")?;
            config_data(launch.profile_label.clone(), config)
        };

        let render_quit = Arc::new(AtomicBool::new(false));
        let service = if launch.fixture_path.is_none() {
            let config = launch
                .config
                .clone()
                .context("elasticsearch tui requires config outside fixture mode")?;
            let tabs = Arc::new(StandaloneEsTabManager::new(render_quit.clone()));
            let runtime = tokio::runtime::Handle::current();
            // EsService verifies cluster reachability in its background task
            // and emits Connected or a non-modal Error event.
            let service = EsService::new(config, tabs, runtime);
            Some(service)
        } else {
            None
        };

        let is_fixture = launch.fixture_path.is_some();
        let mut app = Self {
            profile_label: data.profile_label.clone(),
            server_label: data.server_label.clone(),
            connection_kind: data.connection_kind.clone(),
            source: launch.source,
            purpose: launch.purpose,
            readonly: launch.readonly,
            restore: launch.restore,
            view: View::Indices,
            indices: Vec::new(),
            indices_selection: 0,
            cluster_selection: 0,
            health: None,
            nodes: Vec::new(),
            cluster_loaded: false,
            hits_index: String::new(),
            hits_from: 0,
            hits_total: 0,
            hits: Vec::new(),
            hits_page: 1,
            hits_history: Vec::new(),
            hits_selection: 0,
            filter: String::new(),
            filter_target: FilterTarget::Indices,
            mode: Mode::Browser,
            overlay: Overlay::None,
            error_banner: data.target_error.clone(),
            indices_pending: false,
            hits_pending: false,
            cluster_pending: false,
            mapping_pending: false,
            hit_viewer_lines: BoundedLines::new(VIEWER_LINE_LIMIT, VIEWER_BYTE_LIMIT),
            hit_viewer_title: String::new(),
            hit_viewer_scroll: 0,
            hit_viewer_hit: None,
            mapping_lines: BoundedLines::new(VIEWER_LINE_LIMIT, VIEWER_BYTE_LIMIT),
            mapping_title: String::new(),
            mapping_scroll: 0,
            cluster_viewer_lines: BoundedLines::new(VIEWER_LINE_LIMIT, VIEWER_BYTE_LIMIT),
            cluster_viewer_title: String::new(),
            cluster_viewer_scroll: 0,
            typed_index_input: String::new(),
            operation_plan: None,
            fixture: if is_fixture { Some(data) } else { None },
            service,
            status: String::new(),
            should_quit: false,
            render_quit,
        };
        if is_fixture {
            // Fixture mode loads the index list eagerly so the first frame
            // already shows the search space.
            app.request_indices();
        }
        if let Some(error) = app
            .fixture
            .as_ref()
            .and_then(|fixture| fixture.target_error.clone())
        {
            app.error_banner = Some(error.clone());
            app.status = format!("{} | {}", app.status, safe_error_summary(&error));
        }
        Ok(app)
    }

    fn shutdown(&mut self) {
        if let Some(service) = &self.service {
            service.send(EsCommand::Disconnect);
        }
    }

    fn drain_service(&mut self) -> bool {
        let mut changed = false;
        if self.render_quit.load(Ordering::SeqCst) {
            changed |= !self.should_quit;
            self.should_quit = true;
        }
        let Some(mut service) = self.service.take() else {
            return changed;
        };
        while let Some(event) = service.poll_event() {
            changed = true;
            self.handle_service_event(event);
        }
        self.service = Some(service);
        changed
    }

    fn handle_service_event(&mut self, event: EsEvent) {
        match event {
            EsEvent::Connected => {
                self.status = "connected; loading indices".to_string();
                self.request_indices();
            }
            EsEvent::Index(IndexEvent::Listed(indices)) => {
                self.indices_pending = false;
                self.indices = indices;
                self.trim_indices_selection();
                self.status = format!("loaded {} indices", self.indices.len());
            }
            EsEvent::Index(IndexEvent::Mapping(mapping)) => {
                self.mapping_pending = false;
                self.open_mapping_viewer(&mapping);
            }
            EsEvent::Index(_) => {
                self.indices_pending = false;
                self.status =
                    "index management events are outside the pilot read surface".to_string();
            }
            EsEvent::Document(DocumentEvent::SearchResults(result)) => {
                self.hits_pending = false;
                self.hits = result.hits;
                self.hits_total = result.total;
                self.trim_hits_selection();
                self.status = format!(
                    "searched {} from={} ({} hits of total {})",
                    self.hits_index,
                    self.hits_from,
                    self.hits.len(),
                    self.hits_total
                );
            }
            EsEvent::Document(DocumentEvent::Deleted { index, id }) => {
                self.operation_plan = None;
                self.status = format!("deleted document: {index}/{id}");
                self.refetch_current_page();
            }
            EsEvent::Document(_) => {
                self.hits_pending = false;
                self.status = "document events outside the pilot read surface".to_string();
            }
            EsEvent::Cluster(ClusterEvent::Health(health)) => {
                self.cluster_pending = false;
                self.cluster_loaded = true;
                self.health = Some(health);
                self.status = "cluster health loaded".to_string();
            }
            EsEvent::Cluster(ClusterEvent::Nodes(nodes)) => {
                self.cluster_pending = false;
                self.cluster_loaded = true;
                self.nodes = nodes;
                self.status = format!("loaded {} cluster nodes", self.nodes.len());
            }
            EsEvent::Cluster(_) => {
                self.cluster_pending = false;
            }
            EsEvent::Error(message) => {
                // Background and request failures surface as a non-modal
                // banner so a failing search or listing never hijacks the
                // browser.
                self.indices_pending = false;
                self.hits_pending = false;
                self.cluster_pending = false;
                self.mapping_pending = false;
                self.error_banner = Some(message);
                self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
            }
        }
    }

    // === Input routing ===

    fn handle_key(&mut self, key: KeyEvent) {
        // The error banner is informational: any key acknowledges it while
        // the key itself keeps its normal meaning, so background failures
        // never trap the operator in a modal.
        self.error_banner = None;
        match self.overlay {
            Overlay::None => {}
            Overlay::Help => {
                self.overlay = Overlay::None;
                self.status = "help closed".to_string();
                return;
            }
            Overlay::HitsViewer => {
                self.handle_hits_viewer_key(key);
                return;
            }
            Overlay::HitViewer => {
                self.handle_hit_viewer_key(key);
                return;
            }
            Overlay::MappingViewer => {
                self.handle_mapping_viewer_key(key);
                return;
            }
            Overlay::ClusterViewer => {
                self.handle_cluster_viewer_key(key);
                return;
            }
            Overlay::ConfirmDeleteDoc => {
                self.handle_confirm_delete_key(key);
                return;
            }
            Overlay::ConfirmQuit => {
                self.handle_confirm_quit_key(key);
                return;
            }
        }
        if self.mode == Mode::Filter {
            self.handle_filter_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Char('1') => self.set_view(View::Indices),
            KeyCode::Char('2') => self.set_view(View::Cluster),
            KeyCode::Tab => {
                let next = match self.view {
                    View::Indices => View::Cluster,
                    View::Cluster => View::Indices,
                };
                self.set_view(next);
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_selected(0),
            KeyCode::End | KeyCode::Char('G') => {
                let len = self.active_list_len();
                self.set_selected(len.saturating_sub(1));
            }
            KeyCode::Enter | KeyCode::Char('i') => self.inspect_selected(),
            KeyCode::Char('/') => {
                self.mode = Mode::Filter;
                self.filter_target = match self.view {
                    View::Indices => FilterTarget::Indices,
                    View::Cluster => FilterTarget::Hits,
                };
                self.status = "filter: type to narrow live, Enter apply, Esc clear".to_string();
            }
            KeyCode::Char('m') => self.open_mapping_for_target(),
            KeyCode::Char('r') => self.refresh_view(),
            KeyCode::Char('d') => self.open_delete_prompt(),
            KeyCode::Char('y') => self.confirm_plan(),
            KeyCode::Char('?') => self.show_help(),
            KeyCode::Esc => self.cancel_plan(),
            _ => {
                self.status = format!(
                    "elasticsearch: {}",
                    match self.view {
                        View::Indices => INDICES_HINTS,
                        View::Cluster => CLUSTER_HINTS,
                    }
                );
            }
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = format!(
                    "filter applied: {} ({} matches)",
                    self.filter_label(),
                    self.active_list_len()
                );
            }
            KeyCode::Esc => {
                self.filter.clear();
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = "filter cleared".to_string();
            }
            KeyCode::Backspace => {
                self.filter.pop();
                self.set_selected(0);
            }
            KeyCode::Char(ch) => {
                if self.filter.len() < INPUT_LIMIT {
                    self.filter.push(ch);
                }
                self.set_selected(0);
            }
            _ => {}
        }
    }

    fn handle_hits_viewer_key(&mut self, key: KeyEvent) {
        if self.mode == Mode::Filter && self.filter_target == FilterTarget::Hits {
            self.handle_filter_key(key);
            return;
        }
        let max_scroll = self.filtered_hits().len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::None;
                self.mode = Mode::Browser;
                self.status = "hits viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_selected(0),
            KeyCode::End | KeyCode::Char('G') => self.set_selected(max_scroll),
            KeyCode::Enter | KeyCode::Char('i') => self.open_hit_viewer(),
            KeyCode::Char('n') => self.next_hits_page(),
            KeyCode::Char('b') => self.previous_hits_page(),
            KeyCode::Char('/') => {
                self.mode = Mode::Filter;
                self.filter_target = FilterTarget::Hits;
                self.status =
                    "hits filter: type to narrow the current page live, Enter apply, Esc clear"
                        .to_string();
            }
            KeyCode::Char('d') => self.open_delete_prompt(),
            KeyCode::Char('m') => self.open_mapping_for_target(),
            _ => {
                self.status = format!("elasticsearch: {HITS_HINTS}");
            }
        }
    }

    fn handle_hit_viewer_key(&mut self, key: KeyEvent) {
        let max_scroll = self.hit_viewer_lines.len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::HitsViewer;
                self.status = "hit viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.hit_viewer_scroll = self.hit_viewer_scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.hit_viewer_scroll = (self.hit_viewer_scroll + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.hit_viewer_scroll = self.hit_viewer_scroll.saturating_sub(PAGE_STEP as usize);
            }
            KeyCode::PageDown => {
                self.hit_viewer_scroll =
                    (self.hit_viewer_scroll + PAGE_STEP as usize).min(max_scroll);
            }
            KeyCode::Home | KeyCode::Char('g') => self.hit_viewer_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.hit_viewer_scroll = max_scroll,
            KeyCode::Char('d') => self.open_delete_prompt(),
            _ => {}
        }
    }

    fn handle_mapping_viewer_key(&mut self, key: KeyEvent) {
        let max_scroll = self.mapping_lines.len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::None;
                self.status = "mapping viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.mapping_scroll = self.mapping_scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.mapping_scroll = (self.mapping_scroll + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.mapping_scroll = self.mapping_scroll.saturating_sub(PAGE_STEP as usize);
            }
            KeyCode::PageDown => {
                self.mapping_scroll = (self.mapping_scroll + PAGE_STEP as usize).min(max_scroll);
            }
            KeyCode::Home | KeyCode::Char('g') => self.mapping_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.mapping_scroll = max_scroll,
            _ => {}
        }
    }

    fn handle_cluster_viewer_key(&mut self, key: KeyEvent) {
        let max_scroll = self.cluster_viewer_lines.len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::None;
                self.status = "cluster viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.cluster_viewer_scroll = self.cluster_viewer_scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.cluster_viewer_scroll = (self.cluster_viewer_scroll + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.cluster_viewer_scroll = self
                    .cluster_viewer_scroll
                    .saturating_sub(PAGE_STEP as usize);
            }
            KeyCode::PageDown => {
                self.cluster_viewer_scroll =
                    (self.cluster_viewer_scroll + PAGE_STEP as usize).min(max_scroll);
            }
            KeyCode::Home | KeyCode::Char('g') => self.cluster_viewer_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.cluster_viewer_scroll = max_scroll,
            _ => {}
        }
    }

    fn handle_confirm_delete_key(&mut self, key: KeyEvent) {
        let Some((target_index, target_id)) = self.delete_target() else {
            self.overlay = Overlay::None;
            return;
        };
        match key.code {
            KeyCode::Esc => {
                self.typed_index_input.clear();
                self.overlay = Overlay::None;
                self.status = "delete cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.typed_index_input.pop();
            }
            KeyCode::Enter => {
                if self.typed_index_input != target_index {
                    self.status =
                        format!("typed name does not match {target_index}; delete not staged");
                    self.typed_index_input.clear();
                    return;
                }
                self.typed_index_input.clear();
                // The typed index name is the first of two confirmations; `y`
                // remains the explicit non-default acknowledgement.
                self.operation_plan = Some(OperationPlanView {
                    kind: OperationKind::DeleteDocument,
                    target_index: target_index.clone(),
                    target_id: target_id.clone(),
                    risk: OperationKind::DeleteDocument.risk(),
                    confirmations_required: 2,
                    confirmations: 1,
                });
                self.overlay = Overlay::None;
                self.status = format!(
                    "delete plan staged for {target_index}/{target_id} (typed confirmation recorded); press y to execute"
                );
            }
            KeyCode::Char(ch) if self.typed_index_input.len() < INPUT_LIMIT => {
                self.typed_index_input.push(ch);
            }
            _ => {}
        }
    }

    fn handle_confirm_quit_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.should_quit = true;
            }
            _ => {
                self.overlay = Overlay::None;
                self.status = "quit cancelled".to_string();
            }
        }
    }

    // === Selection and views ===

    fn selected(&self) -> usize {
        match self.active_list() {
            ActiveList::Indices => self.indices_selection,
            ActiveList::Cluster => self.cluster_selection,
            ActiveList::Hits => self.hits_selection,
        }
    }

    fn set_selected(&mut self, value: usize) {
        let len = self.active_list_len();
        let clamped = value.min(len.saturating_sub(1));
        match self.active_list() {
            ActiveList::Indices => self.indices_selection = clamped,
            ActiveList::Cluster => self.cluster_selection = clamped,
            ActiveList::Hits => self.hits_selection = clamped,
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.active_list_len();
        if len == 0 {
            self.set_selected(0);
            return;
        }
        let last = len as isize - 1;
        let next = (self.selected() as isize + delta).clamp(0, last) as usize;
        self.set_selected(next);
    }

    fn trim_indices_selection(&mut self) {
        let len = self.filtered_indices().len();
        self.indices_selection = self.indices_selection.min(len.saturating_sub(1));
    }

    fn trim_hits_selection(&mut self) {
        let len = self.filtered_hits().len();
        self.hits_selection = self.hits_selection.min(len.saturating_sub(1));
    }

    fn set_view(&mut self, view: View) {
        if self.view == view {
            return;
        }
        self.view = view;
        self.status = format!(
            "view: {}",
            match view {
                View::Indices => "indices",
                View::Cluster => "cluster",
            }
        );
        if view == View::Cluster
            && self.fixture.is_none()
            && !self.cluster_loaded
            && !self.cluster_pending
            && let Some(service) = &self.service
        {
            self.cluster_pending = true;
            service.send(EsCommand::Cluster(ClusterCommand::Health));
            service.send(EsCommand::Cluster(ClusterCommand::ListNodes));
        }
    }

    fn active_list(&self) -> ActiveList {
        match self.overlay {
            Overlay::HitsViewer | Overlay::HitViewer => ActiveList::Hits,
            _ => match self.view {
                View::Indices => ActiveList::Indices,
                View::Cluster => ActiveList::Cluster,
            },
        }
    }

    fn active_list_len(&self) -> usize {
        match self.active_list() {
            ActiveList::Indices => self.filtered_indices().len(),
            ActiveList::Hits => self.filtered_hits().len(),
            ActiveList::Cluster => self
                .fixture
                .as_ref()
                .map(|fixture| fixture.nodes.len())
                .unwrap_or_else(|| self.nodes.len()),
        }
    }

    fn filtered_indices(&self) -> Vec<IndexInfo> {
        let needle = self.filter.to_lowercase();
        let filter_applies = self.filter_target == FilterTarget::Indices || self.filter.is_empty();
        self.indices
            .iter()
            .filter(|info| {
                filter_applies && (needle.is_empty() || info.name.to_lowercase().contains(&needle))
            })
            .cloned()
            .collect()
    }

    fn filtered_hits(&self) -> Vec<SearchHit> {
        let needle = self.filter.to_lowercase();
        let filter_applies = self.filter_target == FilterTarget::Hits || self.filter.is_empty();
        self.hits
            .iter()
            .filter(|hit| filter_applies && (needle.is_empty() || hit_matches(hit, &needle)))
            .cloned()
            .collect()
    }

    fn selected_index(&self) -> Option<IndexInfo> {
        self.filtered_indices().get(self.indices_selection).cloned()
    }

    fn selected_hit(&self) -> Option<SearchHit> {
        match self.overlay {
            Overlay::HitViewer => self.hit_viewer_hit.clone(),
            _ => self.filtered_hits().get(self.hits_selection).cloned(),
        }
    }

    fn delete_target(&self) -> Option<(String, String)> {
        self.selected_hit()
            .map(|hit| (hit.index, hit.id))
            .filter(|(index, id)| !index.is_empty() && !id.is_empty())
    }

    fn filter_label(&self) -> String {
        if self.filter.is_empty() {
            "none".to_string()
        } else {
            self.filter.clone()
        }
    }

    // === Fetches ===

    /// Load the index list. Fixture mode resolves synchronously; live mode
    /// sends `IndexCommand::List` (the service excludes system indices).
    fn request_indices(&mut self) {
        self.indices_pending = true;
        if let Some(fixture) = &self.fixture {
            self.indices = fixture.indices.clone();
            self.indices_pending = false;
            self.trim_indices_selection();
            self.status = format!("fixture loaded {} indices", self.indices.len());
        } else if let Some(service) = &self.service {
            service.send(EsCommand::Index(IndexCommand::List));
            self.status = "loading indices".to_string();
        } else {
            self.indices_pending = false;
        }
    }

    /// Open the full-screen paged hits viewer for the selected index. The
    /// first page is a bounded search (`size=10, from=0`), never a full dump.
    fn open_hits_viewer(&mut self) {
        let Some(index) = self.selected_index() else {
            self.status = "nothing selected".to_string();
            return;
        };
        self.hits_index = index.name.clone();
        self.hits_from = 0;
        self.hits_page = 1;
        self.hits_history.clear();
        self.hits_selection = 0;
        if self.filter_target == FilterTarget::Hits {
            self.filter.clear();
        }
        self.overlay = Overlay::HitsViewer;
        self.request_search(&index.name, 0);
        if self.service.is_some() && self.fixture.is_none() {
            self.status = format!(
                "loading hits for {} (full-screen viewer opens when loaded)",
                index.name
            );
        }
    }

    /// Send one bounded search page request with explicit `size`/`from`.
    fn request_search(&mut self, index: &str, from: u32) {
        self.hits_index = index.to_string();
        self.hits_from = from;
        self.hits_pending = true;
        if let Some(fixture) = &self.fixture {
            let (hits, total) = fixture.resolve_search(index, from);
            self.hits = hits;
            self.hits_total = total;
            self.hits_pending = false;
            self.trim_hits_selection();
            self.status = format!(
                "fixture searched {index} from={from} ({} hits of total {})",
                self.hits.len(),
                self.hits_total
            );
        } else if let Some(service) = &self.service {
            service.send(EsCommand::Document(DocumentCommand::Search {
                index: index.to_string(),
                query: json!({"query": {"match_all": {}}}),
                size: HITS_PAGE_SIZE,
                from,
            }));
        } else {
            self.hits_pending = false;
        }
    }

    /// `n` inside the hits viewer: replace the current page with the page at
    /// `from + HITS_PAGE_SIZE`. Pages are replaced (not appended) so the
    /// viewport stays bounded; the previous offset is kept in a history stack
    /// for `b`.
    fn next_hits_page(&mut self) {
        if self.hits_from as u64 + HITS_PAGE_SIZE as u64 >= self.hits_total.max(1)
            || self.hits_total == 0
        {
            self.status =
                "end of hits; press b to go back or / to filter the current page".to_string();
            return;
        }
        self.hits_history.push(self.hits_from);
        self.hits_page = self.hits_page.saturating_add(1);
        self.set_selected(0);
        let index = self.hits_index.clone();
        let from = self.hits_from + HITS_PAGE_SIZE;
        self.request_search(&index, from);
    }

    fn previous_hits_page(&mut self) {
        let Some(previous) = self.hits_history.pop() else {
            self.status = "no previous hits page".to_string();
            return;
        };
        self.hits_page = self.hits_page.saturating_sub(1).max(1);
        self.set_selected(0);
        let index = self.hits_index.clone();
        self.request_search(&index, previous);
    }

    fn refetch_current_page(&mut self) {
        // After a mutation the current page offset is refetched so the
        // deleted document disappears and totals update without leaving the
        // TUI.
        if self.hits_index.is_empty() {
            return;
        }
        let index = self.hits_index.clone();
        let from = self.hits_from;
        self.request_search(&index, from);
    }

    fn request_cluster(&mut self) {
        if let Some(fixture) = &self.fixture {
            self.cluster_loaded = true;
            self.status = format!(
                "fixture cluster info loaded ({} nodes, health {})",
                fixture.nodes.len(),
                fixture
                    .cluster_health
                    .as_ref()
                    .map(|health| health_label(&health.status))
                    .unwrap_or("unknown")
            );
            return;
        }
        if let Some(service) = &self.service {
            self.cluster_pending = true;
            service.send(EsCommand::Cluster(ClusterCommand::Health));
            service.send(EsCommand::Cluster(ClusterCommand::ListNodes));
            self.status = "loading cluster health and nodes".to_string();
        }
    }

    /// `m`: mapping viewer for the selected index (indices view) or for the
    /// index currently open in the hits/hit viewer.
    fn open_mapping_for_target(&mut self) {
        let target = match self.overlay {
            Overlay::HitsViewer | Overlay::HitViewer if !self.hits_index.is_empty() => {
                Some(self.hits_index.clone())
            }
            Overlay::HitsViewer | Overlay::HitViewer => None,
            _ => self.selected_index().map(|info| info.name),
        };
        let Some(index) = target else {
            self.status = "select an index to inspect its mapping".to_string();
            return;
        };
        if let Some(fixture) = &self.fixture {
            match fixture.mapping(&index).cloned() {
                Some(mapping) => {
                    self.open_mapping_viewer(&mapping);
                    self.status = format!("fixture mapping: {index}");
                }
                None => {
                    self.error_banner = Some(format!(
                        "target error: no fixture mapping for index {index}"
                    ));
                    self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                }
            }
            return;
        }
        if let Some(service) = &self.service {
            self.mapping_pending = true;
            service.send(EsCommand::Index(IndexCommand::GetMapping { index }));
            self.status = "loading mapping (full-screen viewer opens when loaded)".to_string();
        }
    }

    fn open_mapping_viewer(&mut self, mapping: &MappingResult) {
        let mut lines = vec![
            format!("# mapping {}", mapping.index),
            format!("fields: {}", mapping.properties.len()),
            String::new(),
        ];
        for property in &mapping.properties {
            lines.push(format!("- {property}"));
        }
        lines.push(String::new());
        lines.push("# raw response".to_string());
        push_pretty_json(&mut lines, &mapping.raw, VIEWER_LINE_LIMIT);
        self.mapping_title = format!("mapping · {}", mapping.index);
        self.mapping_lines.clear();
        for line in lines {
            self.mapping_lines.push_line(line);
        }
        self.mapping_scroll = 0;
        self.overlay = Overlay::MappingViewer;
        self.status = format!("mapping viewer: {}", self.mapping_title);
    }

    /// Enter on a hit opens the typed hit viewer synchronously; the hit data
    /// is already on the page, so no extra fetch is needed.
    fn open_hit_viewer(&mut self) {
        let Some(hit) = self.selected_hit() else {
            self.status = "no hit selected".to_string();
            return;
        };
        let mut lines = vec![
            format!("_id: {}", hit.id),
            format!("_index: {}", hit.index),
            format!(
                "_score: {}",
                hit.score
                    .map(|score| format!("{score:.4}"))
                    .unwrap_or_else(|| "null".to_string())
            ),
            String::new(),
            "_source:".to_string(),
        ];
        push_pretty_json(&mut lines, &hit.source, VIEWER_LINE_LIMIT);
        self.hit_viewer_title = format!("hit {} · {}", hit.id, hit.index);
        self.hit_viewer_lines.clear();
        for line in lines {
            self.hit_viewer_lines.push_line(line);
        }
        self.hit_viewer_scroll = 0;
        self.hit_viewer_hit = Some(hit);
        self.overlay = Overlay::HitViewer;
        self.status = format!("hit viewer: {}", self.hit_viewer_title);
    }

    fn open_cluster_viewer(&mut self) {
        let mut lines = Vec::new();
        let health = self
            .fixture
            .as_ref()
            .and_then(|fixture| fixture.cluster_health.clone())
            .or_else(|| self.health.clone());
        if let Some(health) = &health {
            lines.push("# cluster health".to_string());
            lines.push(format!("cluster_name: {}", health.cluster_name));
            lines.push(format!("status: {}", health_label(&health.status)));
            lines.push(format!("node_count: {}", health.node_count));
            lines.push(format!("active_shards: {}", health.active_shards));
            lines.push(format!("unassigned_shards: {}", health.unassigned_shards));
            lines.push(String::new());
        }
        let nodes = self
            .fixture
            .as_ref()
            .map(|fixture| fixture.nodes.clone())
            .unwrap_or_else(|| self.nodes.clone());
        lines.push(format!("# nodes ({})", nodes.len()));
        for node in &nodes {
            lines.push(format!(
                "{:<20} heap {:>3}%  cpu {:>3}%  disk {:<10} roles {}",
                node.name,
                node.heap_percent,
                node.cpu_percent,
                node.disk_used,
                node.roles.join("")
            ));
        }
        if lines.is_empty() {
            self.status = "no cluster info loaded; press r to fetch health and nodes".to_string();
            return;
        }
        self.cluster_viewer_title = format!(
            "cluster · {}{}",
            health
                .as_ref()
                .map(|h| h.cluster_name.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            self.fixture.as_ref().map(|_| " (fixture)").unwrap_or("")
        );
        self.cluster_viewer_lines.clear();
        for line in lines {
            self.cluster_viewer_lines.push_line(line);
        }
        self.cluster_viewer_scroll = 0;
        self.overlay = Overlay::ClusterViewer;
        self.status = format!("cluster viewer: {}", self.cluster_viewer_title);
    }

    fn inspect_selected(&mut self) {
        match self.overlay {
            Overlay::HitsViewer => self.open_hit_viewer(),
            Overlay::None | Overlay::HitViewer => match self.view {
                View::Cluster => self.open_cluster_viewer(),
                View::Indices => self.open_hits_viewer(),
            },
            _ => {}
        }
    }

    fn refresh_view(&mut self) {
        match self.view {
            View::Indices => {
                self.request_indices();
            }
            View::Cluster => {
                self.request_cluster();
            }
        }
    }

    // === Mutations ===

    fn open_delete_prompt(&mut self) {
        let Some((index, id)) = self.delete_target() else {
            self.status = "select a hit to delete".to_string();
            return;
        };
        self.typed_index_input.clear();
        self.overlay = Overlay::ConfirmDeleteDoc;
        self.status =
            format!("delete {index}/{id}: type the exact index name to confirm, Esc cancels");
    }

    fn confirm_plan(&mut self) {
        let Some(mut plan) = self.operation_plan.take() else {
            self.status = "no operation plan to confirm".to_string();
            return;
        };
        plan.confirmations = plan.confirmations.saturating_add(1);
        if plan.confirmations < plan.confirmations_required {
            self.status = format!(
                "{} on {}/{} is {}; press y again to execute",
                plan.kind.label(),
                plan.target_index,
                plan.target_id,
                plan.risk
            );
            self.operation_plan = Some(plan);
            return;
        }

        if self.readonly {
            self.status = format!(
                "readonly launch blocked {} on {}/{}",
                plan.kind.label(),
                plan.target_index,
                plan.target_id
            );
            self.operation_plan = Some(plan);
            return;
        }

        if let Some(fixture) = self.fixture.as_mut() {
            let result = fixture.apply_delete(&plan.target_index, &plan.target_id);
            match result {
                Ok(summary) => {
                    // Refetch first so the mutation summary is the final
                    // status the operator sees on the refreshed page.
                    self.refetch_current_page();
                    self.status = format!(
                        "fixture executed {} on {}/{}: {}",
                        plan.kind.label(),
                        plan.target_index,
                        plan.target_id,
                        summary
                    );
                }
                Err(message) => {
                    self.error_banner = Some(message);
                    self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                }
            }
        } else if let Some(service) = &self.service {
            service.send(EsCommand::Document(DocumentCommand::Delete {
                index: plan.target_index.clone(),
                id: plan.target_id.clone(),
            }));
            self.status = format!("{} sent to EsService", plan.kind.label());
        } else {
            self.status = "no service connection; plan dropped".to_string();
        }
    }

    fn cancel_plan(&mut self) {
        if self.operation_plan.take().is_some() {
            self.status = "operation plan cancelled".to_string();
        } else {
            self.mode = Mode::Browser;
            self.status = "browser mode".to_string();
        }
    }

    fn request_quit(&mut self) {
        let work_active = self.indices_pending
            || self.hits_pending
            || self.cluster_pending
            || self.mapping_pending
            || self.operation_plan.is_some();
        if work_active {
            self.overlay = Overlay::ConfirmQuit;
            self.status =
                "quit? a fetch or staged plan is active: y quits, any other key stays".to_string();
            return;
        }
        self.should_quit = true;
    }

    fn show_help(&mut self) {
        self.overlay = Overlay::Help;
        self.status = "help open; any key closes".to_string();
    }

    // === Drawing ===

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                // Header and status each carry two content rows (title+tabs,
                // status+hints) on top of their borders.
                Constraint::Length(4),
                Constraint::Min(12),
                Constraint::Length(4),
            ])
            .split(area);

        frame.render_widget(self.header(), vertical[0]);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(vertical[1]);
        match self.view {
            View::Indices => {
                frame.render_widget(self.indices_list(body[0]), body[0]);
                frame.render_widget(self.indices_details_panel(), body[1]);
            }
            View::Cluster => {
                frame.render_widget(self.cluster_list(body[0]), body[0]);
                frame.render_widget(self.cluster_details_panel(), body[1]);
            }
        }
        frame.render_widget(self.status_panel(), vertical[2]);

        match self.overlay {
            Overlay::None => {}
            Overlay::Help => self.draw_help(frame, area),
            Overlay::HitsViewer => self.draw_hits_viewer(frame, area),
            Overlay::HitViewer => self.draw_hit_viewer(frame, area),
            Overlay::MappingViewer => self.draw_mapping_viewer(frame, area),
            Overlay::ClusterViewer => self.draw_cluster_viewer(frame, area),
            Overlay::ConfirmDeleteDoc => self.draw_confirm_delete(frame, area),
            Overlay::ConfirmQuit => self.draw_confirm_quit(frame, area),
        }
        if let Some(error) = &self.error_banner {
            self.draw_error_banner(frame, area, error);
        }
    }

    fn header(&self) -> Paragraph<'_> {
        let source = format!("{:?}", self.source).to_lowercase();
        let indices_mark = if self.indices_pending { "\u{27f3}" } else { "" };
        let cluster_mark = if self.cluster_pending { "\u{27f3}" } else { "" };
        let indices_tab = format!("1:indices{indices_mark}");
        let cluster_tab = format!("2:cluster{cluster_mark}");
        let tabs = match self.view {
            View::Indices => format!("[{indices_tab}] {cluster_tab}"),
            View::Cluster => format!("{indices_tab} [{cluster_tab}]"),
        };
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(
                    "Elasticsearch Search TUI (pilot)",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  profile={} server={} indices={}",
                    self.profile_label,
                    self.server_label,
                    self.indices.len()
                )),
            ]),
            Line::from(format!(
                "{}  source={} kind={} purpose={} readonly={} restore={} filter={}",
                tabs,
                source,
                self.connection_kind,
                self.purpose,
                self.readonly,
                self.restore,
                self.filter_label()
            )),
        ])
        .block(Block::default().borders(Borders::ALL))
    }

    fn indices_list(&self, area: Rect) -> Paragraph<'_> {
        let items = self.filtered_indices();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.indices_selection, visible, items.len());
        let lines = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, info)| {
                let marker = if idx == self.indices_selection {
                    ">"
                } else {
                    " "
                };
                let style = if idx == self.indices_selection {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!(
                        "{marker} {:<30} {:<6} {:>9} {:>9}",
                        info.name,
                        health_label(&info.health),
                        info.doc_count,
                        info.store_size
                    ),
                    style,
                ))
            })
            .collect::<Vec<_>>();
        Paragraph::new(if !lines.is_empty() {
            lines
        } else if self.indices_pending {
            vec![Line::from("loading\u{2026}")]
        } else {
            vec![Line::from(
                "No indices loaded. Press r to refresh or / to filter.",
            )]
        })
        .block(
            Block::default()
                .title(format!(
                    " indices ({}/{}) ",
                    self.indices_selection
                        .saturating_add(1)
                        .min(items.len().max(1)),
                    items.len()
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
    }

    fn indices_details_panel(&self) -> Paragraph<'_> {
        let mut lines = match self.selected_index() {
            Some(info) => vec![
                Line::from(format!("index: {}", info.name)),
                Line::from(format!(
                    "health: {}  status: {}",
                    health_label(&info.health),
                    if info.status.is_empty() {
                        "-"
                    } else {
                        &info.status
                    }
                )),
                Line::from(format!(
                    "docs: {}  store: {}  shards: {}/{}",
                    info.doc_count, info.store_size, info.pri_shards, info.rep_shards
                )),
                Line::from("i/Enter hits viewer  m mapping  / filter"),
            ],
            None => vec![Line::from(
                "Select an index to browse its hits. Press r to refresh the index list.",
            )],
        };

        if let Some(plan) = &self.operation_plan {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "operation plan",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(format!("action: {}", plan.kind.label())));
            lines.push(Line::from(format!(
                "target: {}/{}",
                plan.target_index, plan.target_id
            )));
            lines.push(Line::from(format!("risk: {}", plan.risk)));
            lines.push(Line::from(format!(
                "confirmations: {}/{}",
                plan.confirmations, plan.confirmations_required
            )));
        }

        Paragraph::new(lines)
            .block(Block::default().title(" Details ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn cluster_list(&self, area: Rect) -> Paragraph<'_> {
        let nodes = self
            .fixture
            .as_ref()
            .map(|fixture| fixture.nodes.clone())
            .unwrap_or_else(|| self.nodes.clone());
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.cluster_selection, visible, nodes.len());
        let mut lines = Vec::new();
        for (idx, node) in nodes.iter().enumerate().skip(start).take(visible) {
            let marker = if idx == self.cluster_selection {
                ">"
            } else {
                " "
            };
            let style = if idx == self.cluster_selection {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(Span::styled(
                format!(
                    "{marker} {:<22} heap {:>3}%  cpu {:>3}%  disk {:<10}",
                    node.name, node.heap_percent, node.cpu_percent, node.disk_used
                ),
                style,
            )));
        }
        if lines.is_empty() {
            lines.push(if self.cluster_pending {
                Line::from("loading\u{2026}")
            } else {
                Line::from("No cluster nodes loaded. Press r to fetch health and nodes.")
            });
        }
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(format!(
                        " cluster nodes ({}/{}) ",
                        (self.cluster_selection + 1).min(nodes.len().max(1)),
                        nodes.len()
                    ))
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false })
    }

    fn cluster_details_panel(&self) -> Paragraph<'_> {
        let mut lines = Vec::new();
        let health = self
            .fixture
            .as_ref()
            .and_then(|fixture| fixture.cluster_health.clone())
            .or_else(|| self.health.clone());
        if let Some(health) = &health {
            lines.push(Line::from(format!(
                "cluster: {} ({})",
                health.cluster_name,
                health_label(&health.status)
            )));
            lines.push(Line::from(format!(
                "nodes: {}  shards: {} active, {} unassigned",
                health.node_count, health.active_shards, health.unassigned_shards
            )));
        } else {
            lines.push(Line::from("Cluster health loads here. Press r to refresh."));
        }
        let nodes = self
            .fixture
            .as_ref()
            .map(|fixture| fixture.nodes.clone())
            .unwrap_or_else(|| self.nodes.clone());
        if let Some(node) = nodes.get(self.cluster_selection.min(nodes.len().saturating_sub(1)))
            && !nodes.is_empty()
        {
            lines.push(Line::from(String::new()));
            lines.push(Line::from(format!("node: {}", node.name)));
            lines.push(Line::from(format!("roles: {}", node.roles.join(""))));
            lines.push(Line::from(format!(
                "heap: {}%  cpu: {}%  disk: {}",
                node.heap_percent, node.cpu_percent, node.disk_used
            )));
        }
        lines.push(Line::from(""));
        lines.push(Line::from("i/Enter cluster viewer  r refresh"));
        Paragraph::new(lines)
            .block(Block::default().title(" Cluster ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn status_panel(&self) -> Paragraph<'_> {
        let filter_active = self.mode == Mode::Filter && self.filter_target == FilterTarget::Hits;
        let hints = if self.mode == Mode::Filter {
            format!(
                "filter[{}]: {}\u{258f} Enter apply \u{b7} Esc clear \u{b7} {} match(es)",
                match self.filter_target {
                    FilterTarget::Indices => "indices",
                    FilterTarget::Hits => "hits",
                },
                self.filter,
                self.active_list_len()
            )
        } else {
            match self.overlay {
                Overlay::HitsViewer | Overlay::HitViewer => HITS_HINTS.to_string(),
                _ => match self.view {
                    View::Indices => INDICES_HINTS.to_string(),
                    View::Cluster => CLUSTER_HINTS.to_string(),
                },
            }
        };
        Paragraph::new(vec![
            Line::from(format!(
                "{} | mode={} | indices {} match={} | hits {}/{} {} from={} page {} | pending indices={} hits={} cluster={} mapping={}",
                self.status,
                mode_label(self.mode),
                self.indices.len(),
                self.filtered_indices().len(),
                self.hits.len(),
                self.hits_total,
                if filter_active { "filtered" } else { "" },
                self.hits_from,
                self.hits_page,
                self.indices_pending,
                self.hits_pending,
                self.cluster_pending,
                self.mapping_pending,
            )),
            Line::from(Span::styled(hints, Style::default().fg(Color::DarkGray))),
        ])
        .block(Block::default().title(" Status ").borders(Borders::ALL))
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(78, 84, area);
        frame.render_widget(Clear, popup);
        let help = vec![
            Line::from(Span::styled(
                "Navigation",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("1 indices browser \u{b7} 2 cluster (Tab cycles)"),
            Line::from("j/k or arrows move \u{b7} PgUp/PgDn page \u{b7} g/G home/end"),
            Line::from(
                "/ client-side filter (indices list or current hits page, live, Esc clears)",
            ),
            Line::from(Span::styled(
                "Search triage",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("i/Enter on an index: full-screen paged hits viewer (size=10, from=0)"),
            Line::from(
                "hits viewer: n next page \u{b7} b back (offset history) \u{b7} / filter current page \u{b7} Enter hit detail",
            ),
            Line::from("hit viewer: _id/_index/_score + pretty _source \u{b7} m mapping viewer"),
            Line::from("cluster view: i/Enter cluster viewer \u{b7} r refresh"),
            Line::from(Span::styled(
                "Mutations (staged plans, y confirms)",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("d delete one document by _id (type the exact index name, then y)"),
            Line::from(
                "every mutation refetches the current page \u{b7} readonly launches block at confirm",
            ),
            Line::from(Span::styled(
                "Sessions",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("q: quit (asks for confirmation while a fetch or plan is active)"),
            Line::from("errors appear in a non-modal banner; any key acknowledges"),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(Block::default().title(" Help ").borders(Borders::ALL))
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_hits_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(94, 88, area);
        frame.render_widget(Clear, popup);
        let items = self.filtered_hits();
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.hits_selection, visible, items.len());
        let mut lines: Vec<Line> = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, hit)| {
                let marker = if idx == self.hits_selection { ">" } else { " " };
                let style = if idx == self.hits_selection {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!(
                        "{marker} {:<24} {:>8}  {}",
                        hit.id,
                        hit.score
                            .map(|score| format!("{score:.4}"))
                            .unwrap_or_else(|| "null".to_string()),
                        source_summary(&hit.source, 64)
                    ),
                    style,
                ))
            })
            .collect();
        if lines.is_empty() && self.hits_pending {
            lines.push(Line::from("loading\u{2026}"));
        }
        if lines.is_empty() && !self.hits_pending {
            lines.push(Line::from(
                "No hits on this page. Press b to go back or Esc to close.",
            ));
        }
        let history_note = if self.hits_history.is_empty() {
            String::new()
        } else {
            format!(" \u{b7} {} back", self.hits_history.len())
        };
        frame.render_widget(
            Paragraph::new(lines)
                .block(
                    Block::default()
                        .title(format!(
                            " hits {} page {} ({}/{}) from={} total={} size={}{history_note} ",
                            self.hits_index,
                            self.hits_page,
                            self.hits_selection
                                .saturating_add(1)
                                .min(items.len().max(1)),
                            items.len(),
                            self.hits_from,
                            self.hits_total,
                            HITS_PAGE_SIZE,
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_hit_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let total = self.hit_viewer_lines.len();
        let body: Vec<Line> = if total == 0 {
            vec![Line::from("no hit inspected yet; press Enter on a hit")]
        } else {
            let start = self.hit_viewer_scroll.min(total.saturating_sub(1));
            self.hit_viewer_lines
                .range(start, start + visible)
                .into_iter()
                .map(Line::from)
                .collect()
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " hit viewer {} \u{b7} line {}/{} \u{b7} j/k PgUp/PgDn scroll \u{b7} d delete \u{b7} Esc close ",
                            self.hit_viewer_title,
                            (self.hit_viewer_scroll + 1).min(total.max(1)),
                            total
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_mapping_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let total = self.mapping_lines.len();
        let body: Vec<Line> = if total == 0 {
            vec![Line::from(
                "no mapping loaded; press m on an index or inside the hits viewer",
            )]
        } else {
            let start = self.mapping_scroll.min(total.saturating_sub(1));
            self.mapping_lines
                .range(start, start + visible)
                .into_iter()
                .map(Line::from)
                .collect()
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " mapping viewer {} \u{b7} line {}/{} \u{b7} j/k PgUp/PgDn scroll \u{b7} Esc close ",
                            self.mapping_title,
                            (self.mapping_scroll + 1).min(total.max(1)),
                            total
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_cluster_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let total = self.cluster_viewer_lines.len();
        let body: Vec<Line> = if total == 0 {
            vec![Line::from("no cluster data loaded")]
        } else {
            let start = self.cluster_viewer_scroll.min(total.saturating_sub(1));
            self.cluster_viewer_lines
                .range(start, start + visible)
                .into_iter()
                .map(Line::from)
                .collect()
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " {} \u{b7} line {}/{} \u{b7} j/k PgUp/PgDn scroll \u{b7} Esc close ",
                            self.cluster_viewer_title,
                            (self.cluster_viewer_scroll + 1).min(total.max(1)),
                            total
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_confirm_delete(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(64, 26, area);
        frame.render_widget(Clear, popup);
        let target = self
            .delete_target()
            .map(|(index, id)| format!("{index}/{id}"))
            .unwrap_or_else(|| "no hit selected".to_string());
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Delete document (destructive)",
                    Style::default()
                        .fg(Color::Red)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(format!("target: {target}")),
                Line::from("Deleting one document by _id cannot be undone."),
                Line::from("Type the exact index name to confirm:"),
                Line::from(Span::styled(
                    format!("{}\u{258f}", self.typed_index_input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(""),
                Line::from(
                    "Enter stages the plan (typed confirmation) \u{b7} y executes \u{b7} Esc cancels",
                ),
            ])
            .block(
                Block::default()
                    .title(" Confirm Delete ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_confirm_quit(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(56, 22, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Quit with active work?",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(self.active_work_summary()),
                Line::from("y/Enter: quit \u{b7} any other key: stay"),
            ])
            .block(
                Block::default()
                    .title(" Confirm Quit ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn active_work_summary(&self) -> String {
        let mut parts = Vec::new();
        if self.indices_pending {
            parts.push("index list fetch in flight".to_string());
        }
        if self.hits_pending {
            parts.push("search fetch in flight".to_string());
        }
        if self.cluster_pending {
            parts.push("cluster fetch in flight".to_string());
        }
        if self.mapping_pending {
            parts.push("mapping fetch in flight".to_string());
        }
        if let Some(plan) = &self.operation_plan {
            parts.push(format!(
                "{} plan on {}/{}",
                plan.kind.label(),
                plan.target_index,
                plan.target_id
            ));
        }
        if parts.is_empty() {
            "no active fetches or plans".to_string()
        } else {
            parts.join(", ")
        }
    }

    fn draw_error_banner(&self, frame: &mut Frame, area: Rect, error: &str) {
        let banner = Rect {
            x: area.x + 1,
            y: area.height.saturating_sub(6),
            width: area.width.saturating_sub(2),
            height: 3,
        };
        frame.render_widget(Clear, banner);
        frame.render_widget(
            Paragraph::new(vec![Line::from(Span::styled(
                safe_error_summary(error),
                Style::default().fg(Color::Red),
            ))])
            .block(
                Block::default()
                    .title(" Error (any key acknowledges) ")
                    .borders(Borders::ALL)
                    .style(Style::default().fg(Color::Red)),
            )
            .wrap(Wrap { trim: false }),
            banner,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveList {
    Indices,
    Cluster,
    Hits,
}

fn hit_matches(hit: &SearchHit, needle: &str) -> bool {
    if hit.id.to_lowercase().contains(needle) || hit.index.to_lowercase().contains(needle) {
        return true;
    }
    serde_json::to_string(&hit.source)
        .map(|text| text.to_lowercase().contains(needle))
        .unwrap_or(false)
}

/// Compact one-line summary of a `_source` document for the hits list.
fn source_summary(source: &Value, max_chars: usize) -> String {
    let text = serde_json::to_string(source).unwrap_or_else(|_| "(unserializable)".to_string());
    let mut summary: String = text
        .chars()
        .map(|ch| if ch.is_whitespace() { ' ' } else { ch })
        .collect();
    if summary.chars().count() > max_chars {
        summary = format!(
            "{}\u{2026}",
            summary.chars().take(max_chars).collect::<String>()
        );
    }
    summary
}

/// Pretty-print a JSON value into bounded viewer lines. Scalar and null
/// sources render as readable one-liners.
fn push_pretty_json(lines: &mut Vec<String>, value: &Value, max_lines: usize) {
    if value.is_null() {
        lines.push("(source omitted)".to_string());
        return;
    }
    let rendered = serde_json::to_string_pretty(value)
        .unwrap_or_else(|_| "(source not valid JSON)".to_string());
    for line in rendered.lines().take(max_lines) {
        lines.push(line.to_string());
    }
}

fn health_label(health: &IndexHealth) -> &'static str {
    match health {
        IndexHealth::Green => "green",
        IndexHealth::Yellow => "yellow",
        IndexHealth::Red => "red",
    }
}

impl BoundedLines {
    fn new(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            dropped_lines: 0,
            max_lines,
            max_bytes,
        }
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn clear(&mut self) {
        self.lines.clear();
        self.bytes = 0;
        self.dropped_lines = 0;
    }

    fn push_line(&mut self, mut line: String) {
        if line.len() > self.max_bytes {
            line.truncate(self.max_bytes);
        }
        self.bytes = self.bytes.saturating_add(line.len());
        self.lines.push_back(line);
        while self.lines.len() > self.max_lines || self.bytes > self.max_bytes {
            if let Some(removed) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(removed.len());
                self.dropped_lines = self.dropped_lines.saturating_add(1);
            } else {
                break;
            }
        }
    }

    fn range(&self, start: usize, end: usize) -> Vec<String> {
        self.lines
            .iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .cloned()
            .collect()
    }
}

fn safe_error_summary(message: &str) -> String {
    let mut summary = message.replace('\n', " ");
    if summary.len() > 180 {
        summary.truncate(177);
        summary.push_str("...");
    }
    summary
}

fn secret_leak_markers(text: &str) -> Vec<String> {
    [
        "es_password_value",
        "es_api_key_value",
        "es_bearer_token_value",
        "plaintext_credential_value",
        "raw_connection_url",
    ]
    .into_iter()
    .filter(|marker| text.contains(marker))
    .map(str::to_string)
    .collect()
}

fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Browser => "browser",
        Mode::Filter => "filter",
    }
}

fn window_start(selected: usize, visible: usize, len: usize) -> usize {
    if visible == 0 || len <= visible {
        0
    } else if selected >= visible {
        (selected + 1).saturating_sub(visible)
    } else {
        0
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn index_value(info: &IndexInfo) -> Value {
    json!({
        "name": info.name,
        "health": health_label(&info.health),
        "status": info.status,
        "doc_count": info.doc_count,
        "store_size": info.store_size,
        "pri_shards": info.pri_shards,
        "rep_shards": info.rep_shards
    })
}

fn hit_value(hit: &SearchHit) -> Value {
    json!({
        "id": hit.id,
        "index": hit.index,
        "score": hit.score,
        "source": hit.source
    })
}

fn health_value(health: &ClusterHealth) -> Value {
    json!({
        "cluster_name": health.cluster_name,
        "status": health_label(&health.status),
        "node_count": health.node_count,
        "active_shards": health.active_shards,
        "unassigned_shards": health.unassigned_shards
    })
}

fn node_value(node: &NodeInfo) -> Value {
    json!({
        "name": node.name,
        "roles": node.roles,
        "heap_percent": node.heap_percent,
        "disk_used": node.disk_used,
        "cpu_percent": node.cpu_percent
    })
}

struct StandaloneEsTabManager {
    render_tx: tokio::sync::mpsc::UnboundedSender<()>,
    should_quit: Arc<AtomicBool>,
}

impl StandaloneEsTabManager {
    fn new(should_quit: Arc<AtomicBool>) -> Self {
        let (render_tx, _render_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            render_tx,
            should_quit,
        }
    }

    fn unsupported_tabs_error() -> anyhow::Error {
        anyhow::anyhow!(
            "Elasticsearch TUI does not host plugin tabs; use plugin-owned CLI commands instead"
        )
    }
}

impl TabManager for StandaloneEsTabManager {
    fn open(&self, _title: String, _plugin_id: String, _context: Value) -> Result<()> {
        Err(Self::unsupported_tabs_error())
    }

    fn close_current(&self) -> Result<()> {
        self.quit()
    }

    fn set_title(&self, _title: String) -> Result<()> {
        Ok(())
    }

    fn request_render(&self) -> Result<()> {
        let _ = self.render_tx.send(());
        Ok(())
    }

    fn list_tabs(&self) -> Result<Vec<TabInfo>> {
        Ok(vec![TabInfo {
            index: 0,
            title: "Elasticsearch".to_string(),
            plugin_id: "elasticsearch".to_string(),
            context: json!({}),
            is_active: true,
        }])
    }

    fn close_tab(&self, index: usize) -> Result<()> {
        if index == 0 {
            self.quit()
        } else {
            Err(anyhow::anyhow!("Elasticsearch TUI has no tab {index}"))
        }
    }

    fn switch_to(&self, index: usize) -> Result<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Elasticsearch TUI has no tab {index}"))
        }
    }

    fn active_tab_index(&self) -> Result<usize> {
        Ok(0)
    }

    fn quit(&self) -> Result<()> {
        self.should_quit.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    fn fixture_launch(readonly: bool) -> EsTuiLaunch {
        EsTuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: EsTuiSource::Fixture,
            fixture_path: Some(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/elasticsearch_tui_operations.json")
                    .display()
                    .to_string(),
            ),
            purpose: "operations".to_string(),
            readonly,
            restore: true,
            launch_plan: None,
        }
    }

    fn app() -> EsTuiApp {
        EsTuiApp::new(fixture_launch(false)).unwrap()
    }

    fn press(app: &mut EsTuiApp, code: KeyCode) {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn type_text(app: &mut EsTuiApp, text: &str) {
        for ch in text.chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
        }
    }

    fn open_logs_hits(app: &mut EsTuiApp) {
        press(app, KeyCode::Char('i'));
        assert_eq!(app.overlay, Overlay::HitsViewer);
        assert_eq!(app.hits_index, "logs-app-2026.11");
        assert_eq!(app.hits.len(), 10);
        assert_eq!(app.hits_total, 14);
    }

    #[test]
    fn fixture_evidence_has_coverage_and_no_secret_markers() {
        let evidence = build_elasticsearch_tui_evidence(&fixture_launch(false)).unwrap();
        let rendered = serde_json::to_string(&evidence).unwrap();
        assert!(rendered.contains("elasticsearch_tui_fixture_evidence"));
        assert!(rendered.contains("typed_delete_confirmation"));
        assert!(rendered.contains("hits_paging_offset_history"));
        assert!(
            rendered.contains("delete_by_query"),
            "never-shipped list names it"
        );
        assert!(rendered.contains("later candidate"));
        assert_eq!(evidence["secret_leak_scan"]["passed"], json!(true));
        assert_eq!(
            evidence["quality_gate"]["plugin_id"],
            json!("elasticsearch")
        );
        assert_eq!(
            evidence["quality_gate"]["layout"]["minimum_viewport"]["width"],
            json!(80)
        );
    }

    #[test]
    fn preflight_redacts_auth_material() {
        let launch = EsTuiLaunch {
            profile_label: "live-es".to_string(),
            config: Some(EsConfig {
                urls: vec!["https://es.internal:9200".to_string()],
                auth: Some(crate::config::EsAuth::Basic {
                    username: "elastic".to_string(),
                    password: "es_password_value".to_string(),
                }),
                timeout: 30,
                verify_ssl: true,
            }),
            source: EsTuiSource::Profile,
            fixture_path: None,
            purpose: "triage".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let rendered = serde_json::to_string(&preflight_value(&launch)).unwrap();
        assert!(rendered.contains("redacted"));
        assert!(!rendered.contains("es_password_value"));
        assert!(
            !rendered.contains("https://"),
            "the connection URL itself never appears (host label is allowed)"
        );
        assert_eq!(preflight_value(&launch)["ok"], json!(true));
        assert_eq!(
            preflight_value(&launch)["command"],
            json!("elasticsearch tui")
        );
        assert_eq!(
            preflight_value(&launch)["connection"]["auth"]["method"],
            json!("basic")
        );
        assert_eq!(
            preflight_value(&launch)["connection"]["url_host_label"],
            json!("es.internal:9200")
        );
    }

    #[test]
    fn startup_target_error_banner_is_non_modal() {
        let mut app = app();
        assert!(app.error_banner.is_some());
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("target error")
        );
        assert_eq!(app.mode, Mode::Browser, "errors must not hijack the mode");
        assert_eq!(
            app.indices.len(),
            6,
            "fixture index list loads despite the banner"
        );
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "any key acknowledges");
        assert_eq!(app.indices_selection, 1, "the key keeps its normal meaning");
    }

    #[test]
    fn hotkeys_switch_views_and_tab_cycles() {
        let mut app = app();
        assert_eq!(app.view, View::Indices);
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Cluster);
        assert!(app.status.contains("view: cluster"));
        press(&mut app, KeyCode::Tab);
        assert_eq!(app.view, View::Indices);
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(app.view, View::Indices);
    }

    #[test]
    fn per_view_selection_memory_is_preserved() {
        let mut app = app();
        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Char('j'));
        assert_eq!(app.indices_selection, 2);
        press(&mut app, KeyCode::Char('2'));
        press(&mut app, KeyCode::Char('j'));
        assert_eq!(app.cluster_selection, 1);
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(
            app.indices_selection, 2,
            "indices selection survives view switches"
        );
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(
            app.cluster_selection, 1,
            "cluster selection survives view switches"
        );
    }

    #[test]
    fn hits_paging_tracks_offset_history() {
        let mut app = app();
        open_logs_hits(&mut app);
        assert_eq!(app.hits_from, 0);
        assert_eq!(app.hits_page, 1);
        press(&mut app, KeyCode::Char('n'));
        assert_eq!(app.hits_from, 10);
        assert_eq!(app.hits.len(), 4);
        assert_eq!(app.hits_total, 14);
        assert_eq!(app.hits_page, 2);
        assert!(app.status.contains("from=10"));
        press(&mut app, KeyCode::Char('n'));
        assert!(
            app.status.contains("end of hits"),
            "total 14 is exhausted at from=10"
        );
        press(&mut app, KeyCode::Char('b'));
        assert_eq!(app.hits_from, 0);
        assert_eq!(app.hits.len(), 10);
        assert_eq!(app.hits_page, 1);
        press(&mut app, KeyCode::Char('b'));
        assert!(app.status.contains("no previous hits page"));
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn filter_reports_match_count() {
        let mut app = app();
        press(&mut app, KeyCode::Char('/'));
        assert_eq!(app.mode, Mode::Filter);
        assert_eq!(app.filter_target, FilterTarget::Indices);
        type_text(&mut app, "logs");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.mode, Mode::Browser);
        assert_eq!(app.filtered_indices().len(), 2);
        assert!(app.status.contains("(2 matches)"));
        press(&mut app, KeyCode::Char('/'));
        press(&mut app, KeyCode::Esc);
        assert!(app.filter.is_empty());
        assert!(app.status.contains("filter cleared"));

        // hits filter inside the viewer
        open_logs_hits(&mut app);
        press(&mut app, KeyCode::Char('/'));
        assert_eq!(app.filter_target, FilterTarget::Hits);
        type_text(&mut app, "ERROR");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.filtered_hits().len(), 3);
        assert!(app.status.contains("(3 matches)"));
    }

    #[test]
    fn typed_delete_requires_exact_index_name() {
        let mut app = app();
        open_logs_hits(&mut app);
        press(&mut app, KeyCode::Char('d'));
        assert_eq!(app.overlay, Overlay::ConfirmDeleteDoc);
        assert!(
            app.status.contains("logs-app-2026.11/log-0001"),
            "the _id echo names the target"
        );
        type_text(&mut app, "orders-prod");
        press(&mut app, KeyCode::Enter);
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("typed name does not match"));
        assert_eq!(app.overlay, Overlay::ConfirmDeleteDoc);
        type_text(&mut app, "logs-app-2026.11");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::None);
        let plan = app.operation_plan.clone().expect("delete plan staged");
        assert_eq!(plan.kind, OperationKind::DeleteDocument);
        assert_eq!(plan.target_index, "logs-app-2026.11");
        assert_eq!(plan.target_id, "log-0001");
        assert_eq!(plan.confirmations_required, 2);
        assert_eq!(
            plan.confirmations, 1,
            "typed entry is the first confirmation"
        );
        assert!(app.status.contains("typed confirmation recorded"));
        assert!(
            app.status.contains("log-0001"),
            "the _id echo survives staging"
        );
    }

    #[test]
    fn delete_executes_after_y_and_refetches_page() {
        let mut app = app();
        open_logs_hits(&mut app);
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "logs-app-2026.11");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("fixture executed delete document"));
        assert!(app.status.contains("log-0001"));
        assert!(
            !app.hits.iter().any(|hit| hit.id == "log-0001"),
            "the deleted document is gone from the refetched page"
        );
        assert_eq!(app.hits.len(), 9);
        assert_eq!(
            app.hits_total, 13,
            "the fixture total updates after the delete"
        );
    }

    #[test]
    fn readonly_launch_blocks_confirmed_plan() {
        let mut app = EsTuiApp::new(fixture_launch(true)).unwrap();
        open_logs_hits(&mut app);
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "logs-app-2026.11");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.operation_plan.is_some());
        assert!(app.status.contains("readonly launch blocked"));
        assert!(
            app.hits.iter().any(|hit| hit.id == "log-0001"),
            "readonly launches must not mutate the index"
        );
    }

    #[test]
    fn erroring_document_shows_banner_and_stays_responsive() {
        let mut app = app();
        open_logs_hits(&mut app);
        app.set_selected(2); // log-locked-003 is the deliberate error doc
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "logs-app-2026.11");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.error_banner.is_some());
        assert!(app.status.contains("target error"));
        assert_eq!(app.mode, Mode::Browser);
        assert!(
            app.hits.iter().any(|hit| hit.id == "log-locked-003"),
            "a failed mutation must not remove the document"
        );
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "banner stays non-modal");
        assert!(app.operation_plan.is_none());
    }

    #[test]
    fn hit_viewer_renders_typed_fields_and_pretty_source() {
        let mut app = app();
        open_logs_hits(&mut app);
        app.set_selected(0);
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::HitViewer);
        assert!(app.hit_viewer_title.contains("log-0001"));
        let rendered = app
            .hit_viewer_lines
            .range(0, app.hit_viewer_lines.len())
            .join("\n");
        assert!(rendered.contains("_id: log-0001"));
        assert!(rendered.contains("_index: logs-app-2026.11"));
        assert!(rendered.contains("_score:"));
        assert!(
            rendered.contains("\"level\": \"ERROR\""),
            "source is pretty JSON"
        );
        assert!(
            rendered.contains("\"status_code\": 504"),
            "nested objects render"
        );
        press(&mut app, KeyCode::Esc);
        assert_eq!(
            app.overlay,
            Overlay::HitsViewer,
            "hit viewer returns to the hits list"
        );
        // score-null hit renders the literal null
        app.set_selected(8); // log-0009 carries score null
        press(&mut app, KeyCode::Enter);
        let rendered = app
            .hit_viewer_lines
            .range(0, app.hit_viewer_lines.len())
            .join("\n");
        assert!(rendered.contains("_score: null"));
        press(&mut app, KeyCode::Esc);
    }

    #[test]
    fn mapping_viewer_renders_properties_and_raw() {
        let mut app = app();
        press(&mut app, KeyCode::Char('m'));
        assert_eq!(app.overlay, Overlay::MappingViewer);
        assert!(app.mapping_title.contains("logs-app-2026.11"));
        let rendered = app
            .mapping_lines
            .range(0, app.mapping_lines.len())
            .join("\n");
        assert!(rendered.contains("# mapping logs-app-2026.11"));
        assert!(rendered.contains("- @timestamp"));
        assert!(
            rendered.contains("\"type\": \"date\""),
            "raw mapping JSON is visible"
        );
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);

        // mapping from inside the hits viewer targets the open index
        open_logs_hits(&mut app);
        press(&mut app, KeyCode::Char('n'));
        press(&mut app, KeyCode::Char('m'));
        assert_eq!(app.overlay, Overlay::MappingViewer);
        assert!(app.mapping_title.contains("logs-app-2026.11"));
        press(&mut app, KeyCode::Esc);
    }

    #[test]
    fn cluster_view_shows_health_nodes_and_viewer() {
        let mut app = app();
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Cluster);
        assert_eq!(
            app.active_list_len(),
            3,
            "fixture nodes are visible in cluster view"
        );
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::ClusterViewer);
        let rendered = app
            .cluster_viewer_lines
            .range(0, app.cluster_viewer_lines.len())
            .join("\n");
        assert!(rendered.contains("# cluster health"));
        assert!(rendered.contains("cluster_name: fixture-search-cluster"));
        assert!(rendered.contains("status: green"));
        assert!(rendered.contains("# nodes (3)"));
        assert!(rendered.contains("node-es-01"));
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn empty_index_shows_empty_state_and_refuses_mutations() {
        let mut app = app();
        app.set_selected(4); // metrics-empty has zero documents
        press(&mut app, KeyCode::Char('i'));
        assert_eq!(app.overlay, Overlay::HitsViewer);
        assert_eq!(app.hits_index, "metrics-empty");
        assert!(app.hits.is_empty());
        assert_eq!(app.hits_total, 0);
        press(&mut app, KeyCode::Char('d'));
        assert!(
            app.status.contains("select a hit to delete"),
            "mutations refuse to stage with nothing selected"
        );
        assert!(app.operation_plan.is_none());
        press(&mut app, KeyCode::Char('n'));
        assert!(app.status.contains("end of hits"));
        press(&mut app, KeyCode::Esc);
    }

    #[test]
    fn partial_page_state_is_visible() {
        let mut app = app();
        app.set_selected(2); // orders-prod has 3 hits on one page
        press(&mut app, KeyCode::Char('i'));
        assert_eq!(app.overlay, Overlay::HitsViewer);
        assert_eq!(app.hits.len(), 3);
        assert_eq!(app.hits_total, 3);
        press(&mut app, KeyCode::Char('n'));
        assert!(
            app.status.contains("end of hits"),
            "a partial page has no next"
        );
        press(&mut app, KeyCode::Esc);
    }

    #[test]
    fn quit_with_active_plan_requires_confirmation() {
        let mut app = app();
        open_logs_hits(&mut app);
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "logs-app-2026.11");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('q'));
        assert_eq!(app.overlay, Overlay::ConfirmQuit);
        assert!(!app.should_quit);
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        assert!(!app.should_quit);
        press(&mut app, KeyCode::Char('q'));
        press(&mut app, KeyCode::Char('y'));
        assert!(app.should_quit);
    }

    #[test]
    fn page_keys_move_selection() {
        let mut app = app();
        press(&mut app, KeyCode::PageDown);
        assert_eq!(
            app.indices_selection,
            5.min(app.filtered_indices().len() - 1)
        );
        press(&mut app, KeyCode::PageUp);
        assert_eq!(app.indices_selection, 0);
    }

    #[test]
    fn delete_from_hit_viewer_targets_the_open_hit() {
        let mut app = app();
        open_logs_hits(&mut app);
        app.set_selected(0);
        press(&mut app, KeyCode::Enter); // hit viewer on log-0001
        assert_eq!(app.overlay, Overlay::HitViewer);
        press(&mut app, KeyCode::Char('d'));
        assert_eq!(app.overlay, Overlay::ConfirmDeleteDoc);
        type_text(&mut app, "logs-app-2026.11");
        press(&mut app, KeyCode::Enter);
        let plan = app.operation_plan.clone().expect("delete plan staged");
        assert_eq!(plan.target_id, "log-0001");
        assert_eq!(plan.target_index, "logs-app-2026.11");
    }
}
