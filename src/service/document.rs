//! Elasticsearch document operations inner service.

use reqwest::Client;
use serde_json::Value;

use super::http;
use super::types::{RawApiResult, SearchHit, SearchResult};
use crate::config::EsConfig;

/// Inner service for document-level operations.
pub struct EsDocumentService {
    client: Client,
    config: EsConfig,
}

impl EsDocumentService {
    pub fn new(client: Client, config: EsConfig) -> Self {
        Self { client, config }
    }

    /// Search documents in an index.
    pub async fn search(
        &self,
        index: &str,
        query: &Value,
        size: u32,
        from: u32,
    ) -> Result<SearchResult, String> {
        let index = http::sanitize_path_component(index)?;
        let path = format!("/{}/_search?size={}&from={}", index, size, from);
        let resp = http::post(&self.client, &self.config, &path, Some(query)).await?;

        let total = resp["hits"]["total"]["value"].as_u64().unwrap_or(0);
        let hits_arr = resp["hits"]["hits"].as_array();

        let hits = hits_arr
            .map(|arr| {
                arr.iter()
                    .map(|h| SearchHit {
                        id: h["_id"].as_str().unwrap_or("").to_string(),
                        index: h["_index"].as_str().unwrap_or("").to_string(),
                        source: h["_source"].clone(),
                        score: h["_score"].as_f64(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(SearchResult {
            hits,
            total,
            columns: Vec::new(), // Columns populated separately via mapping
        })
    }

    /// Get a single document by ID.
    pub async fn get(&self, index: &str, id: &str) -> Result<SearchHit, String> {
        let index = http::sanitize_path_component(index)?;
        let id = http::sanitize_path_component(id)?;
        let path = format!("/{}/_doc/{}", index, id);
        let v = http::get(&self.client, &self.config, &path).await?;
        Ok(SearchHit {
            id: v["_id"].as_str().unwrap_or("").to_string(),
            index: v["_index"].as_str().unwrap_or("").to_string(),
            source: v["_source"].clone(),
            score: v["_score"].as_f64(),
        })
    }

    /// Index (create/update) a document.
    pub async fn index_doc(
        &self,
        index: &str,
        doc: &Value,
        id: Option<&str>,
    ) -> Result<String, String> {
        let index = http::sanitize_path_component(index)?;
        let id = id.map(|id| http::sanitize_path_component(id)).transpose()?;
        let path = if let Some(id) = id {
            format!("/{}/_doc/{}", index, id)
        } else {
            format!("/{}/_doc", index)
        };

        let resp = if id.is_some() {
            http::put(&self.client, &self.config, &path, Some(doc)).await?
        } else {
            http::post(&self.client, &self.config, &path, Some(doc)).await?
        };

        Ok(resp["_id"].as_str().unwrap_or("").to_string())
    }

    /// Delete a document by ID.
    pub async fn delete(&self, index: &str, id: &str) -> Result<(), String> {
        let index = http::sanitize_path_component(index)?;
        let id = http::sanitize_path_component(id)?;
        let path = format!("/{}/_doc/{}", index, id);
        http::delete(&self.client, &self.config, &path).await?;
        Ok(())
    }

    /// Count documents in an index (optionally with a query).
    pub async fn count(&self, index: &str, query: Option<&Value>) -> Result<u64, String> {
        let index = http::sanitize_path_component(index)?;
        let path = format!("/{}/_count", index);
        let resp = if let Some(q) = query {
            http::post(&self.client, &self.config, &path, Some(q)).await?
        } else {
            http::get(&self.client, &self.config, &path).await?
        };
        Ok(resp["count"].as_u64().unwrap_or(0))
    }

    /// Raw API call (any method, any path).
    pub async fn raw_api(
        &self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<RawApiResult, String> {
        http::validate_raw_api_path(path)?;
        let result = match method.to_uppercase().as_str() {
            "GET" => http::get(&self.client, &self.config, path).await?,
            "POST" => http::post(&self.client, &self.config, path, body).await?,
            "PUT" => http::put(&self.client, &self.config, path, body).await?,
            "DELETE" => http::delete(&self.client, &self.config, path).await?,
            _ => return Err(format!("Unsupported method: {}", method)),
        };
        Ok(RawApiResult { body: result })
    }
}
