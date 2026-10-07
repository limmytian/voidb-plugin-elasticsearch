use serde::{Deserialize, Serialize};

/// Elasticsearch connection configuration, stored as JSON in ConnectionConfig.plugin_config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EsConfig {
    /// Elasticsearch URL(s) (e.g., "https://localhost:9200")
    pub urls: Vec<String>,

    /// Authentication method
    #[serde(default)]
    pub auth: Option<EsAuth>,

    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Whether to verify SSL certificates
    #[serde(default = "default_true")]
    pub verify_ssl: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EsAuth {
    /// Basic authentication (username + password)
    Basic { username: String, password: String },
    /// API key authentication (id + api_key encoded as base64)
    ApiKey { id: String, api_key: String },
    /// Bearer token authentication
    Bearer { token: String },
}

fn default_timeout() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

impl Default for EsConfig {
    fn default() -> Self {
        Self {
            urls: vec!["http://localhost:9200".to_string()],
            auth: None,
            timeout: 30,
            verify_ssl: true,
        }
    }
}

impl EsConfig {
    /// Get the primary URL (first in the list).
    pub fn primary_url(&self) -> &str {
        self.urls
            .first()
            .map(|s| s.as_str())
            .unwrap_or("http://localhost:9200")
    }
}
