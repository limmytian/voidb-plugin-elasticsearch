//! Typed response wrappers for Elasticsearch service operations.
//!
//! All types in this module are UI-framework-independent (no TUI dependency).

/// Index health status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexHealth {
    Green,
    Yellow,
    Red,
}

impl IndexHealth {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "green" => Self::Green,
            "yellow" => Self::Yellow,
            _ => Self::Red,
        }
    }
}

/// Information about an Elasticsearch index.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub health: IndexHealth,
    pub status: String,
    pub doc_count: u64,
    pub store_size: String,
    pub pri_shards: u32,
    pub rep_shards: u32,
}

/// Information about a cluster node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub name: String,
    pub roles: Vec<String>,
    pub heap_percent: u32,
    pub disk_used: String,
    pub cpu_percent: u32,
}

/// Cluster health summary.
#[derive(Debug, Clone)]
pub struct ClusterHealth {
    pub cluster_name: String,
    pub status: IndexHealth,
    pub node_count: u32,
    pub active_shards: u32,
    pub unassigned_shards: u32,
}

/// Typed search result from Elasticsearch.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub total: u64,
    pub columns: Vec<String>,
}

/// A single search hit.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: String,
    pub index: String,
    pub source: serde_json::Value,
    pub score: Option<f64>,
}

/// Typed mapping response.
#[derive(Debug, Clone)]
pub struct MappingResult {
    pub index: String,
    pub properties: Vec<String>,
    pub raw: serde_json::Value,
}

/// Typed index stats response.
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub doc_count: u64,
    pub store_size_bytes: u64,
    pub raw: serde_json::Value,
}

/// Typed cluster info response.
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub cluster_name: String,
    pub version: String,
    pub raw: serde_json::Value,
}

/// Typed alias entry.
#[derive(Debug, Clone)]
pub struct AliasInfo {
    pub alias: String,
    pub index: String,
    pub filter: Option<String>,
    pub routing_index: Option<String>,
    pub routing_search: Option<String>,
}

/// Typed raw API response.
#[derive(Debug, Clone)]
pub struct RawApiResult {
    pub body: serde_json::Value,
}
