//! Private HTTP helpers for Elasticsearch REST API calls.

use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

use crate::config::{EsAuth, EsConfig};

/// Validate a single path component (index name, document ID) to prevent
/// path traversal and query/fragment injection. Percent escapes are rejected
/// so encoded separators cannot cross the component boundary.
pub(crate) fn sanitize_path_component(s: &str) -> Result<&str, String> {
    if s.is_empty() {
        return Err("Path component must not be empty".to_string());
    }
    if s.contains('/')
        || s.contains("..")
        || s.contains('\0')
        || s.contains('\\')
        || s.contains('?')
        || s.contains('#')
        || s.contains('%')
    {
        return Err(format!("Invalid path component: {}", s));
    }
    Ok(s)
}

/// Validate a raw API path to prevent path traversal. The path must start
/// with `/` and must not contain `..` segments.
pub(crate) fn validate_raw_api_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/') {
        return Err(format!("Raw API path must start with '/': {}", path));
    }
    // Split on '/' and check each segment (ignoring query string)
    let path_part = path.split('?').next().unwrap_or(path);
    for segment in path_part.split('/') {
        if segment == ".." {
            return Err(format!("Path traversal detected in: {}", path));
        }
    }
    Ok(())
}

/// Create a reqwest Client configured with timeout and TLS settings.
pub(crate) fn create_client(config: &EsConfig) -> Result<Client, String> {
    let builder = Client::builder()
        .timeout(Duration::from_secs(config.timeout))
        .danger_accept_invalid_certs(!config.verify_ssl);

    builder
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {}", e))
}

/// Apply auth headers to a request builder.
pub(crate) fn apply_auth(
    req: reqwest::RequestBuilder,
    config: &EsConfig,
) -> reqwest::RequestBuilder {
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
pub(crate) async fn get(client: &Client, config: &EsConfig, path: &str) -> Result<Value, String> {
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
pub(crate) async fn post(
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
pub(crate) async fn put(
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
pub(crate) async fn delete(
    client: &Client,
    config: &EsConfig,
    path: &str,
) -> Result<Value, String> {
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

#[cfg(test)]
mod tests {
    use super::sanitize_path_component;

    #[test]
    fn path_components_reject_query_fragment_and_encoded_separator_injection() {
        for invalid in ["logs?pretty=true", "logs#fragment", "logs%2F_hidden"] {
            assert!(sanitize_path_component(invalid).is_err(), "{invalid}");
        }
        assert_eq!(
            sanitize_path_component("tenant-events-2026").unwrap(),
            "tenant-events-2026"
        );
    }
}
