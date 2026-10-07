//! Elasticsearch service commands.
//!
//! Commands sent from the UI layer to the EsService background task.

use crate::config::EsConfig;

/// Top-level command enum for the Elasticsearch service.
pub enum EsCommand {
    /// (Re)connect with the given config.
    Connect { config: EsConfig },
    /// Disconnect and stop the background task.
    Disconnect,
    /// Cluster-level operations.
    Cluster(ClusterCommand),
    /// Index-level operations.
    Index(IndexCommand),
    /// Document-level operations.
    Document(DocumentCommand),
}

/// Cluster sub-commands.
pub enum ClusterCommand {
    Health,
    Info,
    ListNodes,
}

/// Index sub-commands.
pub enum IndexCommand {
    List,
    Create {
        name: String,
        settings: Option<serde_json::Value>,
    },
    Delete {
        name: String,
    },
    GetMapping {
        index: String,
    },
    Stats {
        index: String,
    },
    ListAliases,
}

/// Document sub-commands.
pub enum DocumentCommand {
    Search {
        index: String,
        query: serde_json::Value,
        size: u32,
        from: u32,
    },
    Get {
        index: String,
        id: String,
    },
    Index {
        index: String,
        doc: serde_json::Value,
        id: Option<String>,
    },
    Delete {
        index: String,
        id: String,
    },
    Count {
        index: String,
        query: Option<serde_json::Value>,
    },
    RawApi {
        method: String,
        path: String,
        body: Option<serde_json::Value>,
    },
}
