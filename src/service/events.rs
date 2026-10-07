//! Elasticsearch service events.
//!
//! Events sent from the EsService background task back to the UI layer.

use super::types::*;

/// Top-level event enum for the Elasticsearch service.
pub enum EsEvent {
    /// Connection established successfully.
    Connected,
    /// An error occurred.
    Error(String),
    /// Cluster-level events.
    Cluster(ClusterEvent),
    /// Index-level events.
    Index(IndexEvent),
    /// Document-level events.
    Document(DocumentEvent),
}

/// Cluster sub-events.
pub enum ClusterEvent {
    Health(ClusterHealth),
    Info(ClusterInfo),
    Nodes(Vec<NodeInfo>),
}

/// Index sub-events.
pub enum IndexEvent {
    Listed(Vec<IndexInfo>),
    Created { name: String },
    Deleted { name: String },
    Mapping(MappingResult),
    Stats(IndexStats),
    Aliases(Vec<AliasInfo>),
}

/// Document sub-events.
pub enum DocumentEvent {
    SearchResults(SearchResult),
    Document(SearchHit),
    Indexed { id: String },
    Deleted { index: String, id: String },
    Count(u64),
    RawApiResult(RawApiResult),
}
