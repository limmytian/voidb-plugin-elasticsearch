//! Elasticsearch REST API operations via reqwest.

use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

use crate::config::{EsAuth, EsConfig};
use crate::types::{ClusterHealth, IndexHealth, IndexInfo, NodeInfo};

/// Create a reqwest Client configured with auth and TLS settings.
pub fn create_client(config: &EsConfig) -> Result<Client, String> {
    let mut builder = Client::builder()
        .timeout(Duration::from_secs(config.timeout))
        .danger_accept_invalid_certs(!config.verify_ssl);

    if let Some(EsAuth::Basic { username, password }) = &config.auth {
        // reqwest doesn't set default auth on builder; we handle it per-request
        let _ = (username, password); // auth applied per-request
        let _ = &mut builder;
    }

    builder
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {}", e))
}

/// Apply auth headers to a request builder.
fn apply_auth(req: reqwest::RequestBuilder, config: &EsConfig) -> reqwest::RequestBuilder {
    match &config.auth {
        Some(EsAuth::Basic { username, password }) => req.basic_auth(username, Some(password)),
        Some(EsAuth::ApiKey { id, api_key }) => {
            use base64::Engine;
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", id, api_key));
            req.header("Authorization", format!("ApiKey {}", encoded))
        }
        Some(EsAuth::Bearer { token }) => req.bearer_auth(token),
        None => req,
    }
}

/// GET request helper.
async fn get(client: &Client, config: &EsConfig, path: &str) -> Result<Value, String> {
    let url = format!("{}{}", config.primary_url().trim_end_matches('/'), path);
    let req = apply_auth(client.get(&url), config);
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| format!("Read body failed: {}", e))?;
    if !status.is_success() {
        return Err(format!("HTTP {}: {}", status, body));
    }
    serde_json::from_str(&body).map_err(|e| format!("JSON parse error: {}", e))
}

/// POST request helper.
async fn post(
    client: &Client,
    config: &EsConfig,
    path: &str,
    body: Option<&Value>,
) -> Result<Value, String> {
    let url = format!("{}{}", config.primary_url().trim_end_matches('/'), path);
    let mut req = apply_auth(client.post(&url), config);
    if let Some(b) = body {
        req = req.json(b);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Read body failed: {}", e))?;
    if !status.is_success() {
        return Err(format!("HTTP {}: {}", status, text));
    }
    serde_json::from_str(&text).map_err(|e| format!("JSON parse error: {}", e))
}

/// PUT request helper.
async fn put(
    client: &Client,
    config: &EsConfig,
    path: &str,
    body: Option<&Value>,
) -> Result<Value, String> {
    let url = format!("{}{}", config.primary_url().trim_end_matches('/'), path);
    let mut req = apply_auth(client.put(&url), config);
    if let Some(b) = body {
        req = req.json(b);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Read body failed: {}", e))?;
    if !status.is_success() {
        return Err(format!("HTTP {}: {}", status, text));
    }
    serde_json::from_str(&text).map_err(|e| format!("JSON parse error: {}", e))
}

/// DELETE request helper.
async fn delete(client: &Client, config: &EsConfig, path: &str) -> Result<Value, String> {
    let url = format!("{}{}", config.primary_url().trim_end_matches('/'), path);
    let req = apply_auth(client.delete(&url), config);
    let resp = req
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Read body failed: {}", e))?;
    if !status.is_success() {
        return Err(format!("HTTP {}: {}", status, text));
    }
    serde_json::from_str(&text).map_err(|e| format!("JSON parse error: {}", e))
}

/// Get cluster health.
pub async fn cluster_health(client: &Client, config: &EsConfig) -> Result<ClusterHealth, String> {
    let v = get(client, config, "/_cluster/health").await?;
    Ok(ClusterHealth {
        cluster_name: v["cluster_name"].as_str().unwrap_or("unknown").to_string(),
        status: IndexHealth::from_str(v["status"].as_str().unwrap_or("red")),
        node_count: v["number_of_nodes"].as_u64().unwrap_or(0) as u32,
        active_shards: v["active_shards"].as_u64().unwrap_or(0) as u32,
        unassigned_shards: v["unassigned_shards"].as_u64().unwrap_or(0) as u32,
    })
}

/// Get cluster info (GET /).
pub async fn cluster_info(client: &Client, config: &EsConfig) -> Result<Value, String> {
    get(client, config, "/").await
}

/// List cluster nodes.
pub async fn list_nodes(client: &Client, config: &EsConfig) -> Result<Vec<NodeInfo>, String> {
    let v = get(
        client,
        config,
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

/// List all indices.
pub async fn list_indices(client: &Client, config: &EsConfig) -> Result<Vec<IndexInfo>, String> {
    let v = get(
        client,
        config,
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
pub async fn create_index(
    client: &Client,
    config: &EsConfig,
    name: &str,
    settings: Option<&Value>,
) -> Result<(), String> {
    let path = format!("/{}", name);
    put(client, config, &path, settings).await?;
    Ok(())
}

/// Delete an index.
pub async fn delete_index(client: &Client, config: &EsConfig, name: &str) -> Result<(), String> {
    let path = format!("/{}", name);
    delete(client, config, &path).await?;
    Ok(())
}

/// Get index mapping.
pub async fn get_mapping(client: &Client, config: &EsConfig, index: &str) -> Result<Value, String> {
    let path = format!("/{}/_mapping", index);
    get(client, config, &path).await
}

/// Get index stats.
pub async fn index_stats(client: &Client, config: &EsConfig, index: &str) -> Result<Value, String> {
    let path = format!("/{}/_stats", index);
    get(client, config, &path).await
}

/// Search documents in an index.
pub async fn search(
    client: &Client,
    config: &EsConfig,
    index: &str,
    query: &Value,
    size: u32,
    from: u32,
) -> Result<Value, String> {
    let path = format!("/{}/_search?size={}&from={}", index, size, from);
    post(client, config, &path, Some(query)).await
}

/// Get a single document by ID.
pub async fn get_document(
    client: &Client,
    config: &EsConfig,
    index: &str,
    id: &str,
) -> Result<Value, String> {
    let path = format!("/{}/_doc/{}", index, id);
    get(client, config, &path).await
}

/// Index (create/update) a document.
pub async fn index_document(
    client: &Client,
    config: &EsConfig,
    index: &str,
    doc: &Value,
    id: Option<&str>,
) -> Result<String, String> {
    let path = if let Some(id) = id {
        format!("/{}/_doc/{}", index, id)
    } else {
        format!("/{}/_doc", index)
    };

    let resp = if id.is_some() {
        put(client, config, &path, Some(doc)).await?
    } else {
        post(client, config, &path, Some(doc)).await?
    };

    Ok(resp["_id"].as_str().unwrap_or("").to_string())
}

/// Delete a document by ID.
pub async fn delete_document(
    client: &Client,
    config: &EsConfig,
    index: &str,
    id: &str,
) -> Result<(), String> {
    let path = format!("/{}/_doc/{}", index, id);
    delete(client, config, &path).await?;
    Ok(())
}

/// Count documents in an index (optionally with a query).
pub async fn count_documents(
    client: &Client,
    config: &EsConfig,
    index: &str,
    query: Option<&Value>,
) -> Result<u64, String> {
    let path = format!("/{}/_count", index);
    let resp = if let Some(q) = query {
        post(client, config, &path, Some(q)).await?
    } else {
        get(client, config, &path).await?
    };
    Ok(resp["count"].as_u64().unwrap_or(0))
}

/// List all aliases.
pub async fn list_aliases(client: &Client, config: &EsConfig) -> Result<Value, String> {
    get(client, config, "/_cat/aliases?format=json").await
}

/// Raw API call (any method, any path).
pub async fn raw_api(
    client: &Client,
    config: &EsConfig,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<Value, String> {
    match method.to_uppercase().as_str() {
        "GET" => get(client, config, path).await,
        "POST" => post(client, config, path, body).await,
        "PUT" => put(client, config, path, body).await,
        "DELETE" => delete(client, config, path).await,
        _ => Err(format!("Unsupported method: {}", method)),
    }
}
