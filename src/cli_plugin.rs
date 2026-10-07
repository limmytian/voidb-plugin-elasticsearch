//! Elasticsearch CLI plugin.
//!
//! All operations go through `EsService::new_direct()` so that the same
//! validated, path-sanitized code paths used by the TUI are exercised here.

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};
use voidb_core::VoidbError;
use voidb_core::plugin::cli::{CliContext, CliPlugin};

use crate::config::EsConfig;
use crate::service::EsService;
use crate::service::types::IndexHealth;

pub struct EsCliPlugin;

pub fn create_es_cli_plugin() -> Box<dyn CliPlugin> {
    Box::new(EsCliPlugin)
}

#[async_trait]
impl CliPlugin for EsCliPlugin {
    fn plugin_id(&self) -> &str {
        "elasticsearch"
    }

    fn name(&self) -> &str {
        "Elasticsearch"
    }

    fn commands(&self) -> Vec<Command> {
        let conn_arg = Arg::new("connection")
            .short('c')
            .long("connection")
            .required(true)
            .help("Connection name");

        vec![
            Command::new("health")
                .about("Show cluster health")
                .arg(conn_arg.clone()),
            Command::new("nodes")
                .about("List cluster nodes")
                .arg(conn_arg.clone()),
            Command::new("indices")
                .about("List indices")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("all")
                        .short('a')
                        .long("all")
                        .action(clap::ArgAction::SetTrue)
                        .help("Include system indices (dot-prefixed)"),
                ),
            Command::new("search")
                .about("Search an index with JSON DSL query")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("index")
                        .short('i')
                        .long("index")
                        .required(true)
                        .help("Index name"),
                )
                .arg(
                    Arg::new("query")
                        .short('q')
                        .long("query")
                        .help("JSON query body (omit for match_all)"),
                )
                .arg(
                    Arg::new("size")
                        .short('n')
                        .long("size")
                        .default_value("10")
                        .help("Number of results"),
                )
                .arg(
                    Arg::new("from")
                        .long("from")
                        .default_value("0")
                        .help("Offset for pagination"),
                ),
            Command::new("get")
                .about("Get a document by ID")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("index")
                        .short('i')
                        .long("index")
                        .required(true)
                        .help("Index name"),
                )
                .arg(Arg::new("id").required(true).help("Document ID")),
            Command::new("count")
                .about("Count documents in an index")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("index")
                        .short('i')
                        .long("index")
                        .required(true)
                        .help("Index name"),
                )
                .arg(
                    Arg::new("query")
                        .short('q')
                        .long("query")
                        .help("JSON query body (omit for total count)"),
                ),
            Command::new("mapping")
                .about("Show index mapping")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("index")
                        .short('i')
                        .long("index")
                        .required(true)
                        .help("Index name"),
                ),
            Command::new("api")
                .about("Execute a raw API call")
                .arg(conn_arg)
                .arg(
                    Arg::new("method")
                        .short('X')
                        .long("method")
                        .default_value("GET")
                        .help("HTTP method (GET, POST, PUT, DELETE)"),
                )
                .arg(
                    Arg::new("path")
                        .required(true)
                        .help("API path (e.g. /_cat/shards?format=json)"),
                )
                .arg(
                    Arg::new("body")
                        .short('d')
                        .long("data")
                        .help("JSON request body"),
                ),
        ]
    }

    async fn execute(
        &self,
        command: &str,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        match command {
            "health" => self.handle_health(matches, ctx).await,
            "nodes" => self.handle_nodes(matches, ctx).await,
            "indices" => self.handle_indices(matches, ctx).await,
            "search" => self.handle_search(matches, ctx).await,
            "get" => self.handle_get(matches, ctx).await,
            "count" => self.handle_count(matches, ctx).await,
            "mapping" => self.handle_mapping(matches, ctx).await,
            "api" => self.handle_api(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!("Unknown command: {}", command))),
        }
    }
}

impl EsCliPlugin {
    /// Parse the ES config from the named connection in the CLI context.
    fn parse_config(conn_name: &str, ctx: &CliContext) -> Result<EsConfig, VoidbError> {
        let config = ctx
            .find_connection(conn_name)
            .ok_or_else(|| VoidbError::Plugin(format!("Connection '{}' not found", conn_name)))?;

        if config.effective_plugin_id() != "elasticsearch" {
            return Err(VoidbError::Plugin(format!(
                "Connection '{}' is not an Elasticsearch connection (plugin: {})",
                conn_name,
                config.effective_plugin_id()
            )));
        }

        config
            .plugin_config
            .as_ref()
            .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
            .and_then(|pc| {
                serde_json::from_value(pc.clone())
                    .map_err(|e| VoidbError::Connection(format!("Invalid ES config: {}", e)))
            })
    }

    /// Build a Direct-mode service from the named connection.
    fn make_service(matches: &ArgMatches, ctx: &CliContext) -> Result<EsService, VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let es_config = Self::parse_config(conn_name, ctx)?;
        EsService::new_direct(es_config).map_err(VoidbError::Plugin)
    }

    async fn handle_health(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let health = svc
            .cluster_health_direct()
            .await
            .map_err(VoidbError::Plugin)?;

        println!("Cluster: {}", health.cluster_name);
        println!(
            "Status:  {}",
            match health.status {
                IndexHealth::Green => "green",
                IndexHealth::Yellow => "yellow",
                IndexHealth::Red => "red",
            }
        );
        println!("Nodes:   {}", health.node_count);
        println!(
            "Shards:  {} active, {} unassigned",
            health.active_shards, health.unassigned_shards
        );
        Ok(())
    }

    async fn handle_nodes(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let nodes = svc.list_nodes_direct().await.map_err(VoidbError::Plugin)?;

        println!(
            "{:<30} {:<10} {:<8} {:<10} {:<6}",
            "NAME", "ROLES", "HEAP%", "DISK", "CPU%"
        );
        for node in &nodes {
            println!(
                "{:<30} {:<10} {:<8} {:<10} {:<6}",
                node.name,
                node.roles.join(""),
                node.heap_percent,
                node.disk_used,
                node.cpu_percent,
            );
        }
        eprintln!("({} nodes)", nodes.len());
        Ok(())
    }

    async fn handle_indices(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let show_all = matches.get_flag("all");

        // When --all is requested we call the raw cat API (which skips the
        // service-layer filter that drops dot-prefixed system indices).
        let indices = if show_all {
            let raw = svc
                .list_all_indices_raw_direct()
                .await
                .map_err(VoidbError::Plugin)?;

            let arr = raw.as_array().ok_or_else(|| {
                VoidbError::Plugin("Expected array from /_cat/indices".to_string())
            })?;
            arr.iter()
                .map(|i| crate::service::types::IndexInfo {
                    name: i["index"].as_str().unwrap_or("").to_string(),
                    health: IndexHealth::from_str(i["health"].as_str().unwrap_or("red")),
                    status: i["status"].as_str().unwrap_or("").to_string(),
                    doc_count: i["docs.count"].as_str().unwrap_or("0").parse().unwrap_or(0),
                    store_size: i["store.size"].as_str().unwrap_or("0b").to_string(),
                    pri_shards: i["pri"].as_str().unwrap_or("0").parse().unwrap_or(0),
                    rep_shards: i["rep"].as_str().unwrap_or("0").parse().unwrap_or(0),
                })
                .collect::<Vec<_>>()
        } else {
            svc.list_indices_direct()
                .await
                .map_err(VoidbError::Plugin)?
        };

        println!(
            "{:<6} {:<40} {:<8} {:<12} {:<10} {:<5} {:<5}",
            "HEALTH", "INDEX", "STATUS", "DOCS", "SIZE", "PRI", "REP"
        );
        for idx in &indices {
            let health_str = match idx.health {
                IndexHealth::Green => "green",
                IndexHealth::Yellow => "yellow",
                IndexHealth::Red => "red",
            };
            println!(
                "{:<6} {:<40} {:<8} {:<12} {:<10} {:<5} {:<5}",
                health_str,
                idx.name,
                idx.status,
                idx.doc_count,
                idx.store_size,
                idx.pri_shards,
                idx.rep_shards,
            );
        }
        eprintln!("({} indices)", indices.len());
        Ok(())
    }

    async fn handle_search(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let index = matches.get_one::<String>("index").unwrap();
        let size: u32 = matches
            .get_one::<String>("size")
            .unwrap()
            .parse()
            .map_err(|_| VoidbError::Plugin("Invalid size".to_string()))?;
        let from: u32 = matches
            .get_one::<String>("from")
            .unwrap()
            .parse()
            .map_err(|_| VoidbError::Plugin("Invalid from".to_string()))?;

        let query: serde_json::Value = if let Some(q) = matches.get_one::<String>("query") {
            serde_json::from_str(q)
                .map_err(|e| VoidbError::Plugin(format!("Invalid JSON query: {}", e)))?
        } else {
            serde_json::json!({"query": {"match_all": {}}})
        };

        let result = svc
            .search_direct(index, &query, size, from)
            .await
            .map_err(VoidbError::Plugin)?;

        let total = result.total;

        for hit in &result.hits {
            println!("--- {} ---", hit.id);
            println!(
                "{}",
                serde_json::to_string_pretty(&hit.source).unwrap_or_default()
            );
        }

        eprintln!(
            "({} hits, showing {}-{})",
            total,
            from,
            from + result.hits.len() as u32
        );
        Ok(())
    }

    async fn handle_get(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let index = matches.get_one::<String>("index").unwrap();
        let id = matches.get_one::<String>("id").unwrap();

        let source = svc
            .get_document_direct(index, id)
            .await
            .map_err(VoidbError::Plugin)?;

        println!(
            "{}",
            serde_json::to_string_pretty(&source).unwrap_or_default()
        );
        Ok(())
    }

    async fn handle_count(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let index = matches.get_one::<String>("index").unwrap();

        let query: Option<serde_json::Value> = if let Some(q) = matches.get_one::<String>("query") {
            Some(
                serde_json::from_str(q)
                    .map_err(|e| VoidbError::Plugin(format!("Invalid JSON query: {}", e)))?,
            )
        } else {
            None
        };

        let count = svc
            .count_documents_direct(index, query.as_ref())
            .await
            .map_err(VoidbError::Plugin)?;

        println!("{}", count);
        Ok(())
    }

    async fn handle_mapping(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let index = matches.get_one::<String>("index").unwrap();

        let mapping = svc
            .get_mapping_direct(index)
            .await
            .map_err(VoidbError::Plugin)?;

        println!(
            "{}",
            serde_json::to_string_pretty(&mapping.raw).unwrap_or_default()
        );
        Ok(())
    }

    async fn handle_api(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let method = matches.get_one::<String>("method").unwrap();
        let path = matches.get_one::<String>("path").unwrap();

        let body: Option<serde_json::Value> = if let Some(b) = matches.get_one::<String>("body") {
            Some(
                serde_json::from_str(b)
                    .map_err(|e| VoidbError::Plugin(format!("Invalid JSON body: {}", e)))?,
            )
        } else {
            None
        };

        let result = svc
            .raw_api_direct(method, path, body.as_ref())
            .await
            .map_err(VoidbError::Plugin)?;

        println!(
            "{}",
            serde_json::to_string_pretty(&result.body).unwrap_or_default()
        );
        Ok(())
    }
}
