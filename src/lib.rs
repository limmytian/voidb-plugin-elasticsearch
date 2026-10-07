//! VoidB Elasticsearch Plugin - Index, document, and cluster capability surface.

mod agent_session;
mod capabilities;
mod cli_plugin;
mod config;
pub mod es_ops;
pub mod service;
mod types;

pub use agent_session::EsAgentSessionFactory;
pub use capabilities::{elasticsearch_capabilities, invoke_elasticsearch_capability};
pub use cli_plugin::create_es_cli_plugin;
pub use config::{EsAuth, EsConfig};

/// Test an Elasticsearch connection by issuing GET / to the cluster.
pub async fn test_connection(
    conn: &voidb_core::connection::ConnectionConfig,
) -> anyhow::Result<String> {
    let es_config: EsConfig = conn
        .plugin_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing plugin_config"))
        .and_then(|pc| serde_json::from_value(pc.clone()).map_err(Into::into))?;

    let client = es_ops::create_client(&es_config).map_err(|e| anyhow::anyhow!("{}", e))?;

    let info = es_ops::cluster_info(&client, &es_config)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let name = info["cluster_name"].as_str().unwrap_or("unknown");
    let version = info["version"]["number"].as_str().unwrap_or("?");
    Ok(format!(
        "OK: {} (v{}) at {}",
        name,
        version,
        es_config.primary_url()
    ))
}
