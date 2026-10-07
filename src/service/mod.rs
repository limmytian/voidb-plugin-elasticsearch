//! Elasticsearch service layer.
//!
//! This module provides `EsService`, the service facade for the Elasticsearch plugin.
//! It follows the S3Service / RedisService convention:
//!
//! - Background tokio task processes commands asynchronously
//! - `send()` dispatches commands via unbounded mpsc channel (non-blocking)
//! - `poll_event()` drains events via `try_recv()` (non-blocking)
//! - Render notifications fire after every event emission
//!
//! ## ServiceMode
//!
//! `EsService` supports two modes:
//!
//! - `Channel`: background tokio task used by the TUI plugin (non-blocking).
//! - `Direct`: synchronous (async) calls used by the CLI plugin (`new_direct()`).
//!   No background task is spawned; each method awaits directly.

pub mod agent_live;
pub mod commands;
pub mod events;
pub mod types;

mod cluster;
mod document;
mod http;
mod index;

pub use commands::EsCommand;
pub use events::EsEvent;

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::config::EsConfig;
use commands::{ClusterCommand, DocumentCommand, IndexCommand};
use events::{ClusterEvent, DocumentEvent, IndexEvent};
use serde_json::Value;
use types::{ClusterHealth, IndexInfo, MappingResult, NodeInfo, RawApiResult, SearchResult};
use voidb_core::TabManager;

use cluster::EsClusterService;
use document::EsDocumentService;
use index::EsIndexService;

// ── ServiceMode ────────────────────────────────────────────────────────────────

/// Operating mode for `EsService`.
///
/// - `Channel`: TUI mode — commands sent over mpsc; results returned as events.
/// - `Direct`: CLI mode — each async method awaits the operation directly.
#[allow(clippy::large_enum_variant)]
enum ServiceMode {
    /// TUI channel-based mode (non-blocking from the plugin's perspective).
    Channel {
        cmd_tx: mpsc::UnboundedSender<EsCommand>,
        event_rx: mpsc::UnboundedReceiver<EsEvent>,
        /// Keeps the background task alive as long as the service exists.
        _task: tokio::task::JoinHandle<()>,
    },
    /// Direct mode for CLI — holds the inner services and calls them synchronously.
    Direct {
        cluster: EsClusterService,
        index: EsIndexService,
        document: EsDocumentService,
    },
}

// ── EsService ─────────────────────────────────────────────────────────────────

/// Elasticsearch service facade.
///
/// In `Channel` mode (TUI) the struct owns the command sender and event receiver
/// channels and a background task processes operations asynchronously.
///
/// In `Direct` mode (CLI) the inner services are called directly with `.await`.
///
/// `EsService` is `Send` but NOT `Sync` (because `UnboundedReceiver` is `!Sync`
/// in Channel mode). Plugin structs must wrap it in `std::sync::Mutex` to
/// satisfy `Plugin: Send + Sync`.
pub struct EsService {
    mode: ServiceMode,
}

impl EsService {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Create a new EsService in **Channel mode** with a background processing task.
    ///
    /// Use this constructor for the TUI plugin.
    pub fn new(
        config: EsConfig,
        tabs: Arc<dyn TabManager>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<EsCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<EsEvent>();

        let task = runtime.spawn(Self::background_task(cmd_rx, event_tx, tabs, config));

        Self {
            mode: ServiceMode::Channel {
                cmd_tx,
                event_rx,
                _task: task,
            },
        }
    }

    /// Create a new EsService in **Direct mode** for CLI usage.
    ///
    /// No background task is spawned. Call the `*_direct()` async methods to
    /// perform operations. Returns an error if the HTTP client cannot be built.
    pub fn new_direct(config: EsConfig) -> Result<Self, String> {
        let client = http::create_client(&config)?;
        Ok(Self {
            mode: ServiceMode::Direct {
                cluster: EsClusterService::new(client.clone(), config.clone()),
                index: EsIndexService::new(client.clone(), config.clone()),
                document: EsDocumentService::new(client, config),
            },
        })
    }

    // ── Channel-mode API ──────────────────────────────────────────────────────

    /// Send a command to the background service task (Channel mode only).
    ///
    /// Non-blocking — safe to call from synchronous `Plugin::update()`.
    /// Silently ignores send errors when Direct mode is used (which is a
    /// programming error; the TUI should always use Channel mode).
    pub fn send(&self, cmd: EsCommand) {
        if let ServiceMode::Channel { cmd_tx, .. } = &self.mode {
            let _ = cmd_tx.send(cmd);
        }
    }

    /// Send a command, returning `Err` if the background task has exited.
    pub fn send_checked(&self, cmd: EsCommand) -> Result<(), String> {
        match &self.mode {
            ServiceMode::Channel { cmd_tx, .. } => cmd_tx
                .send(cmd)
                .map_err(|_| "Service task has exited".to_string()),
            ServiceMode::Direct { .. } => {
                Err("send_checked() is not available in Direct mode".to_string())
            }
        }
    }

    /// Poll for the next event from the service (Channel mode only).
    ///
    /// Returns `Some(event)` if available, `None` otherwise.
    /// Non-blocking, suitable for calling from `Plugin::update()`.
    pub fn poll_event(&mut self) -> Option<EsEvent> {
        match &mut self.mode {
            ServiceMode::Channel { event_rx, .. } => event_rx.try_recv().ok(),
            ServiceMode::Direct { .. } => None,
        }
    }

    // ── Direct-mode API ───────────────────────────────────────────────────────

    /// Get cluster health (Direct mode).
    pub async fn cluster_health_direct(&self) -> Result<ClusterHealth, String> {
        match &self.mode {
            ServiceMode::Direct { cluster, .. } => cluster.health().await,
            ServiceMode::Channel { .. } => {
                Err("cluster_health_direct() requires Direct mode".to_string())
            }
        }
    }

    /// List cluster nodes (Direct mode).
    pub async fn list_nodes_direct(&self) -> Result<Vec<NodeInfo>, String> {
        match &self.mode {
            ServiceMode::Direct { cluster, .. } => cluster.list_nodes().await,
            ServiceMode::Channel { .. } => {
                Err("list_nodes_direct() requires Direct mode".to_string())
            }
        }
    }

    /// List indices, excluding system indices (Direct mode).
    pub async fn list_indices_direct(&self) -> Result<Vec<IndexInfo>, String> {
        match &self.mode {
            ServiceMode::Direct { index, .. } => index.list().await,
            ServiceMode::Channel { .. } => {
                Err("list_indices_direct() requires Direct mode".to_string())
            }
        }
    }

    /// List all indices including system indices via raw cat API (Direct mode).
    ///
    /// Returns the raw JSON array from `/_cat/indices`, which the caller must
    /// parse into `IndexInfo` if needed.
    pub async fn list_all_indices_raw_direct(&self) -> Result<Value, String> {
        match &self.mode {
            ServiceMode::Direct { document, .. } => document
                .raw_api(
                    "GET",
                    "/_cat/indices?format=json&h=index,health,status,docs.count,store.size,pri,rep",
                    None,
                )
                .await
                .map(|r| r.body),
            ServiceMode::Channel { .. } => {
                Err("list_all_indices_raw_direct() requires Direct mode".to_string())
            }
        }
    }

    /// Search documents in an index (Direct mode).
    pub async fn search_direct(
        &self,
        index_name: &str,
        query: &Value,
        size: u32,
        from: u32,
    ) -> Result<SearchResult, String> {
        match &self.mode {
            ServiceMode::Direct { document, .. } => {
                document.search(index_name, query, size, from).await
            }
            ServiceMode::Channel { .. } => Err("search_direct() requires Direct mode".to_string()),
        }
    }

    /// Get a document by ID (Direct mode).
    pub async fn get_document_direct(&self, index_name: &str, id: &str) -> Result<Value, String> {
        match &self.mode {
            ServiceMode::Direct { document, .. } => {
                document.get(index_name, id).await.map(|hit| hit.source)
            }
            ServiceMode::Channel { .. } => {
                Err("get_document_direct() requires Direct mode".to_string())
            }
        }
    }

    /// Count documents in an index (Direct mode).
    pub async fn count_documents_direct(
        &self,
        index_name: &str,
        query: Option<&Value>,
    ) -> Result<u64, String> {
        match &self.mode {
            ServiceMode::Direct { document, .. } => document.count(index_name, query).await,
            ServiceMode::Channel { .. } => {
                Err("count_documents_direct() requires Direct mode".to_string())
            }
        }
    }

    /// Get index mapping (Direct mode).
    pub async fn get_mapping_direct(&self, index_name: &str) -> Result<MappingResult, String> {
        match &self.mode {
            ServiceMode::Direct { index, .. } => index.get_mapping(index_name).await,
            ServiceMode::Channel { .. } => {
                Err("get_mapping_direct() requires Direct mode".to_string())
            }
        }
    }

    /// Execute a raw API call (Direct mode).
    pub async fn raw_api_direct(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<RawApiResult, String> {
        match &self.mode {
            ServiceMode::Direct { document, .. } => document.raw_api(method, path, body).await,
            ServiceMode::Channel { .. } => Err("raw_api_direct() requires Direct mode".to_string()),
        }
    }

    // ── Background task ───────────────────────────────────────────────────────

    /// Background task that processes commands (Channel mode only).
    async fn background_task(
        mut cmd_rx: mpsc::UnboundedReceiver<EsCommand>,
        event_tx: mpsc::UnboundedSender<EsEvent>,
        tabs: Arc<dyn TabManager>,
        initial_config: EsConfig,
    ) {
        // Create HTTP client
        let client = match http::create_client(&initial_config) {
            Ok(c) => c,
            Err(e) => {
                let _ = event_tx.send(EsEvent::Error(e));
                let _ = tabs.request_render();
                return;
            }
        };

        // Create inner services
        let mut cluster_svc = EsClusterService::new(client.clone(), initial_config.clone());
        let mut index_svc = EsIndexService::new(client.clone(), initial_config.clone());
        let mut doc_svc = EsDocumentService::new(client.clone(), initial_config.clone());

        // Verify cluster is reachable before signaling connected
        match cluster_svc.health().await {
            Ok(_) => {
                let _ = event_tx.send(EsEvent::Connected);
            }
            Err(e) => {
                let _ = event_tx.send(EsEvent::Error(format!("Connection failed: {}", e)));
                let _ = tabs.request_render();
                return;
            }
        }
        let _ = tabs.request_render();

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                EsCommand::Connect { config } => match http::create_client(&config) {
                    Ok(new_client) => {
                        cluster_svc = EsClusterService::new(new_client.clone(), config.clone());
                        index_svc = EsIndexService::new(new_client.clone(), config.clone());
                        doc_svc = EsDocumentService::new(new_client.clone(), config);
                        let _ = event_tx.send(EsEvent::Connected);
                    }
                    Err(e) => {
                        let _ = event_tx.send(EsEvent::Error(e));
                    }
                },

                EsCommand::Disconnect => break,

                EsCommand::Cluster(sub) => match sub {
                    ClusterCommand::Health => match cluster_svc.health().await {
                        Ok(h) => {
                            let _ = event_tx.send(EsEvent::Cluster(ClusterEvent::Health(h)));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                    ClusterCommand::Info => match cluster_svc.info().await {
                        Ok(i) => {
                            let _ = event_tx.send(EsEvent::Cluster(ClusterEvent::Info(i)));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                    ClusterCommand::ListNodes => match cluster_svc.list_nodes().await {
                        Ok(n) => {
                            let _ = event_tx.send(EsEvent::Cluster(ClusterEvent::Nodes(n)));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                },

                EsCommand::Index(sub) => match sub {
                    IndexCommand::List => match index_svc.list().await {
                        Ok(indices) => {
                            let _ = event_tx.send(EsEvent::Index(IndexEvent::Listed(indices)));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                    IndexCommand::Create { name, settings } => {
                        match index_svc.create(&name, settings.as_ref()).await {
                            Ok(()) => {
                                let _ = event_tx.send(EsEvent::Index(IndexEvent::Created { name }));
                            }
                            Err(e) => {
                                let _ = event_tx.send(EsEvent::Error(e));
                            }
                        }
                    }
                    IndexCommand::Delete { name } => match index_svc.delete(&name).await {
                        Ok(()) => {
                            let _ = event_tx.send(EsEvent::Index(IndexEvent::Deleted { name }));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                    IndexCommand::GetMapping { index } => {
                        match index_svc.get_mapping(&index).await {
                            Ok(m) => {
                                let _ = event_tx.send(EsEvent::Index(IndexEvent::Mapping(m)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(EsEvent::Error(e));
                            }
                        }
                    }
                    IndexCommand::Stats { index } => match index_svc.stats(&index).await {
                        Ok(s) => {
                            let _ = event_tx.send(EsEvent::Index(IndexEvent::Stats(s)));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                    IndexCommand::ListAliases => match index_svc.list_aliases().await {
                        Ok(a) => {
                            let _ = event_tx.send(EsEvent::Index(IndexEvent::Aliases(a)));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                },

                EsCommand::Document(sub) => match sub {
                    DocumentCommand::Search {
                        index,
                        query,
                        size,
                        from,
                    } => match doc_svc.search(&index, &query, size, from).await {
                        Ok(r) => {
                            let _ =
                                event_tx.send(EsEvent::Document(DocumentEvent::SearchResults(r)));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                    DocumentCommand::Get { index, id } => match doc_svc.get(&index, &id).await {
                        Ok(h) => {
                            let _ = event_tx.send(EsEvent::Document(DocumentEvent::Document(h)));
                        }
                        Err(e) => {
                            let _ = event_tx.send(EsEvent::Error(e));
                        }
                    },
                    DocumentCommand::Index { index, doc, id } => {
                        match doc_svc.index_doc(&index, &doc, id.as_deref()).await {
                            Ok(doc_id) => {
                                let _ = event_tx
                                    .send(EsEvent::Document(DocumentEvent::Indexed { id: doc_id }));
                            }
                            Err(e) => {
                                let _ = event_tx.send(EsEvent::Error(e));
                            }
                        }
                    }
                    DocumentCommand::Delete { index, id } => {
                        match doc_svc.delete(&index, &id).await {
                            Ok(()) => {
                                let _ = event_tx
                                    .send(EsEvent::Document(DocumentEvent::Deleted { index, id }));
                            }
                            Err(e) => {
                                let _ = event_tx.send(EsEvent::Error(e));
                            }
                        }
                    }
                    DocumentCommand::Count { index, query } => {
                        match doc_svc.count(&index, query.as_ref()).await {
                            Ok(c) => {
                                let _ = event_tx.send(EsEvent::Document(DocumentEvent::Count(c)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(EsEvent::Error(e));
                            }
                        }
                    }
                    DocumentCommand::RawApi { method, path, body } => {
                        match doc_svc.raw_api(&method, &path, body.as_ref()).await {
                            Ok(r) => {
                                let _ = event_tx
                                    .send(EsEvent::Document(DocumentEvent::RawApiResult(r)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(EsEvent::Error(e));
                            }
                        }
                    }
                },
            }

            let _ = tabs.request_render();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    #[allow(dead_code)]
    fn assert_sync<T: Sync>() {}

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn service_is_send() {
        assert_send::<EsService>();
    }

    #[test]
    fn command_is_send() {
        assert_send::<EsCommand>();
    }

    #[test]
    fn event_is_send() {
        assert_send::<EsEvent>();
    }

    #[test]
    fn mutex_service_is_send_sync() {
        assert_send_sync::<std::sync::Mutex<EsService>>();
    }
}
