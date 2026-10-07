//! Elasticsearch cluster operations inner service.

use reqwest::Client;

use super::http;
use super::types::{ClusterHealth, ClusterInfo, IndexHealth, NodeInfo};
use crate::config::EsConfig;

/// Inner service for cluster-level operations.
pub struct EsClusterService {
    client: Client,
    config: EsConfig,
}

impl EsClusterService {
    pub fn new(client: Client, config: EsConfig) -> Self {
        Self { client, config }
    }

    /// Get cluster health.
    pub async fn health(&self) -> Result<ClusterHealth, String> {
        let v = http::get(&self.client, &self.config, "/_cluster/health").await?;
        Ok(ClusterHealth {
            cluster_name: v["cluster_name"].as_str().unwrap_or("unknown").to_string(),
            status: IndexHealth::from_str(v["status"].as_str().unwrap_or("red")),
            node_count: v["number_of_nodes"].as_u64().unwrap_or(0) as u32,
            active_shards: v["active_shards"].as_u64().unwrap_or(0) as u32,
            unassigned_shards: v["unassigned_shards"].as_u64().unwrap_or(0) as u32,
        })
    }

    /// Get cluster info (GET /).
    pub async fn info(&self) -> Result<ClusterInfo, String> {
        let v = http::get(&self.client, &self.config, "/").await?;
        Ok(ClusterInfo {
            cluster_name: v["cluster_name"].as_str().unwrap_or("unknown").to_string(),
            version: v["version"]["number"]
                .as_str()
                .unwrap_or("unknown")
                .to_string(),
            raw: v,
        })
    }

    /// List cluster nodes.
    pub async fn list_nodes(&self) -> Result<Vec<NodeInfo>, String> {
        let v = http::get(
            &self.client,
            &self.config,
            "/_cat/nodes?format=json&h=name,node.role,heap.percent,disk.used,cpu",
        )
        .await?;
        let arr = v.as_array().ok_or("Expected array")?;
        Ok(arr
            .iter()
            .map(|n| NodeInfo {
                name: n["name"].as_str().unwrap_or("").to_string(),
                roles: n["node.role"]
                    .as_str()
                    .unwrap_or("")
                    .chars()
                    .map(|c| c.to_string())
                    .collect(),
                heap_percent: n["heap.percent"]
                    .as_str()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0),
                disk_used: n["disk.used"].as_str().unwrap_or("0").to_string(),
                cpu_percent: n["cpu"].as_str().unwrap_or("0").parse().unwrap_or(0),
            })
            .collect())
    }
}
