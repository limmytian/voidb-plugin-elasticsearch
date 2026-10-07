//! Elasticsearch index operations inner service.

use reqwest::Client;
use serde_json::Value;

use super::http;
use super::types::{AliasInfo, IndexHealth, IndexInfo, IndexStats, MappingResult};
use crate::config::EsConfig;

/// Inner service for index-level operations.
pub struct EsIndexService {
    client: Client,
    config: EsConfig,
}

impl EsIndexService {
    pub fn new(client: Client, config: EsConfig) -> Self {
        Self { client, config }
    }

    /// List all indices (excluding system indices starting with '.').
    pub async fn list(&self) -> Result<Vec<IndexInfo>, String> {
        let v = http::get(
            &self.client,
            &self.config,
            "/_cat/indices?format=json&h=index,health,status,docs.count,store.size,pri,rep",
        )
        .await?;
        let arr = v.as_array().ok_or("Expected array")?;
        Ok(arr
            .iter()
            .filter(|i| {
                let name = i["index"].as_str().unwrap_or("");
                !name.starts_with('.')
            })
            .map(|i| IndexInfo {
                name: i["index"].as_str().unwrap_or("").to_string(),
                health: IndexHealth::from_str(i["health"].as_str().unwrap_or("red")),
                status: i["status"].as_str().unwrap_or("").to_string(),
                doc_count: i["docs.count"].as_str().unwrap_or("0").parse().unwrap_or(0),
                store_size: i["store.size"].as_str().unwrap_or("0b").to_string(),
                pri_shards: i["pri"].as_str().unwrap_or("0").parse().unwrap_or(0),
                rep_shards: i["rep"].as_str().unwrap_or("0").parse().unwrap_or(0),
            })
            .collect())
    }

    /// Create an index with optional settings/mappings.
    pub async fn create(&self, name: &str, settings: Option<&Value>) -> Result<(), String> {
        let name = http::sanitize_path_component(name)?;
        let path = format!("/{}", name);
        http::put(&self.client, &self.config, &path, settings).await?;
        Ok(())
    }

    /// Delete an index.
    pub async fn delete(&self, name: &str) -> Result<(), String> {
        let name = http::sanitize_path_component(name)?;
        let path = format!("/{}", name);
        http::delete(&self.client, &self.config, &path).await?;
        Ok(())
    }

    /// Get index mapping, returning typed MappingResult with extracted property names.
    pub async fn get_mapping(&self, index: &str) -> Result<MappingResult, String> {
        let index = http::sanitize_path_component(index)?;
        let path = format!("/{}/_mapping", index);
        let raw = http::get(&self.client, &self.config, &path).await?;

        // Extract column names from mapping
        let properties = extract_columns_from_mapping(&raw, index);

        Ok(MappingResult {
            index: index.to_string(),
            properties,
            raw,
        })
    }

    /// Get index stats.
    pub async fn stats(&self, index: &str) -> Result<IndexStats, String> {
        let index = http::sanitize_path_component(index)?;
        let path = format!("/{}/_stats", index);
        let raw = http::get(&self.client, &self.config, &path).await?;
        Ok(IndexStats {
            doc_count: raw["_all"]["total"]["docs"]["count"].as_u64().unwrap_or(0),
            store_size_bytes: raw["_all"]["total"]["store"]["size_in_bytes"]
                .as_u64()
                .unwrap_or(0),
            raw,
        })
    }

    /// List all aliases.
    pub async fn list_aliases(&self) -> Result<Vec<AliasInfo>, String> {
        let v = http::get(&self.client, &self.config, "/_cat/aliases?format=json").await?;
        let arr = v.as_array().ok_or("Expected array")?;
        Ok(arr
            .iter()
            .map(|a| AliasInfo {
                alias: a["alias"].as_str().unwrap_or("").to_string(),
                index: a["index"].as_str().unwrap_or("").to_string(),
                filter: a["filter"].as_str().map(|s| s.to_string()),
                routing_index: a["routing.index"].as_str().map(|s| s.to_string()),
                routing_search: a["routing.search"].as_str().map(|s| s.to_string()),
            })
            .collect())
    }
}

/// Extract column names from an ES mapping response.
fn extract_columns_from_mapping(mapping: &Value, index: &str) -> Vec<String> {
    let mut columns = vec!["_id".to_string()];

    let properties = mapping[index]["mappings"]["properties"]
        .as_object()
        .or_else(|| {
            mapping
                .as_object()
                .and_then(|m| m.values().next())
                .and_then(|v| v["mappings"]["properties"].as_object())
        });

    if let Some(props) = properties {
        let mut field_names: Vec<String> = props.keys().cloned().collect();
        field_names.sort();
        columns.extend(field_names);
    }

    columns
}
