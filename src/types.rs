// Re-export service types for external consumers.
pub use crate::service::types::{ClusterHealth, IndexHealth, IndexInfo, NodeInfo};

impl IndexHealth {
    pub fn icon(&self) -> &'static str {
        match self {
            Self::Green => "●",
            Self::Yellow => "●",
            Self::Red => "●",
        }
    }

}
