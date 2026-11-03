//! Elasticsearch CLI plugin.
//!
//! All operations go through `EsService::new_direct()` so that the same
//! validated, path-sanitized code paths used by the TUI are exercised here.

use std::fs;

use async_trait::async_trait;
use chrono::Utc;
use clap::{Arg, ArgAction, ArgMatches, Command};
use voidb_core::VoidbError;
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::{ConnectionProfileRef, TuiLaunchRequest, build_tui_launch_plan};

use crate::config::EsConfig;
use crate::service::EsService;
use crate::service::types::IndexHealth;
use crate::tui::{
    EsTuiLaunch, EsTuiSource, build_elasticsearch_tui_evidence, run_es_tui,
    write_elasticsearch_tui_preflight,
};

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
                .arg(conn_arg.clone())
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
            Command::new("test")
                .about("Test Elasticsearch connection and show cluster info")
                .arg(conn_arg.clone().required(false))
                .arg(
                    Arg::new("stdin-config")
                        .long("stdin-config")
                        .action(clap::ArgAction::SetTrue)
                        .help("Read connection configuration JSON from stdin"),
                ),
            Command::new("tui")
                .about("Launch the standalone Elasticsearch search-triage TUI (pilot)")
                .arg(
                    Arg::new("profile")
                        .long("profile")
                        .value_name("PROFILE")
                        .conflicts_with("connection")
                        .help("Profile name, id:<id>, or name:<name>"),
                )
                .arg(
                    Arg::new("connection")
                        .short('c')
                        .long("connection")
                        .value_name("CONNECTION")
                        .conflicts_with("profile")
                        .help("Legacy connection name"),
                )
                .arg(
                    Arg::new("fixture").long("fixture").value_name("PATH").help(
                        "Load deterministic Elasticsearch TUI fixture JSON instead of opening a cluster",
                    ),
                )
                .arg(
                    Arg::new("purpose")
                        .long("purpose")
                        .value_name("PURPOSE")
                        .default_value("operations")
                        .help("Launch purpose, for example operations or triage"),
                )
                .arg(
                    Arg::new("readonly")
                        .long("readonly")
                        .action(ArgAction::SetTrue)
                        .help("Stage operation plans but block mutating service commands"),
                )
                .arg(
                    Arg::new("no-restore")
                        .long("no-restore")
                        .action(ArgAction::SetTrue)
                        .help("Start without restoring plugin-owned UI state"),
                )
                .arg(
                    Arg::new("format")
                        .long("format")
                        .value_parser(["json"])
                        .help("Emit secret-free preflight JSON and exit"),
                )
                .arg(
                    Arg::new("evidence")
                        .long("evidence")
                        .value_name("PATH")
                        .help("Write fixture-backed standalone Elasticsearch TUI evidence JSON and exit"),
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
            "test" => self.handle_test(matches, ctx).await,
            "health" => self.handle_health(matches, ctx).await,
            "nodes" => self.handle_nodes(matches, ctx).await,
            "indices" => self.handle_indices(matches, ctx).await,
            "search" => self.handle_search(matches, ctx).await,
            "get" => self.handle_get(matches, ctx).await,
            "count" => self.handle_count(matches, ctx).await,
            "mapping" => self.handle_mapping(matches, ctx).await,
            "api" => self.handle_api(matches, ctx).await,
            "tui" => self.handle_tui(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!("Unknown command: {}", command))),
        }
    }
}

impl EsCliPlugin {
    fn parse_tui_launch(matches: &ArgMatches, ctx: &CliContext) -> Result<EsTuiLaunch, VoidbError> {
        let fixture_path = matches.get_one::<String>("fixture").cloned();
        let purpose = matches
            .get_one::<String>("purpose")
            .cloned()
            .unwrap_or_else(|| "operations".to_string());
        let readonly = matches.get_flag("readonly");
        let restore = !matches.get_flag("no-restore");

        if let Some(profile_ref) = matches.get_one::<String>("profile") {
            let (profile, connection) =
                ctx.resolve_profile_connection(profile_ref, Some("elasticsearch"))?;
            let config: EsConfig = connection
                .plugin_config
                .as_ref()
                .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
                .and_then(|pc| {
                    serde_json::from_value(pc.clone())
                        .map_err(|e| VoidbError::Connection(format!("Invalid ES config: {}", e)))
                })?;
            let profile_arg = profile_ref_arg(profile_ref, &profile.id);
            let request = TuiLaunchRequest::new("elasticsearch", profile_arg, purpose.clone())
                .readonly(readonly)
                .restore(restore)
                .raw_input(false);
            let launch_plan = build_tui_launch_plan(&profile, request, "voidb-cli", Utc::now())?;

            return Ok(EsTuiLaunch {
                profile_label: profile.name,
                config: Some(config),
                source: EsTuiSource::Profile,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: Some(launch_plan),
            });
        }

        if let Some(conn_name) = matches.get_one::<String>("connection") {
            let config = Self::parse_config(conn_name, ctx)?;
            return Ok(EsTuiLaunch {
                profile_label: conn_name.clone(),
                config: Some(config),
                source: EsTuiSource::Connection,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        if fixture_path.is_some() {
            return Ok(EsTuiLaunch {
                profile_label: "fixture".to_string(),
                config: None,
                source: EsTuiSource::Fixture,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        Err(VoidbError::Plugin(
            "elasticsearch tui requires --profile, --connection, or --fixture".to_string(),
        ))
    }

    async fn handle_tui(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let launch = Self::parse_tui_launch(matches, ctx)?;
        if let Some(path) = matches.get_one::<String>("evidence") {
            let evidence = build_elasticsearch_tui_evidence(&launch).map_err(|e| {
                VoidbError::Plugin(format!("Elasticsearch TUI evidence failed: {}", e))
            })?;
            let rendered = serde_json::to_string_pretty(&evidence).map_err(|e| {
                VoidbError::Plugin(format!(
                    "Elasticsearch TUI evidence serialization failed: {}",
                    e
                ))
            })?;
            if let Some(parent) = std::path::Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).map_err(|e| {
                    VoidbError::Plugin(format!(
                        "Failed to create Elasticsearch TUI evidence directory '{}': {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            fs::write(path, rendered).map_err(|e| {
                VoidbError::Plugin(format!(
                    "Failed to write Elasticsearch TUI evidence '{}': {}",
                    path, e
                ))
            })?;
            println!("Wrote Elasticsearch TUI evidence to {path}");
            return Ok(());
        }

        if matches
            .get_one::<String>("format")
            .is_some_and(|format| format == "json")
        {
            write_elasticsearch_tui_preflight(&launch).map_err(|e| {
                VoidbError::Plugin(format!("Elasticsearch TUI preflight failed: {}", e))
            })?;
            return Ok(());
        }

        run_es_tui(launch)
            .await
            .map_err(|e| VoidbError::Plugin(format!("Elasticsearch TUI failed: {}", e)))
    }

    async fn handle_test(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let conn = if matches.get_flag("stdin-config") {
            use std::io::Read;
            let mut input = String::new();
            std::io::stdin()
                .read_to_string(&mut input)
                .map_err(|e| VoidbError::Config(format!("Failed to read stdin: {}", e)))?;
            let conn: voidb_core::connection::ConnectionConfig = serde_json::from_str(&input)
                .map_err(|e| VoidbError::Config(format!("Invalid JSON on stdin: {}", e)))?;
            conn
        } else if let Some(conn_name) = matches.get_one::<String>("connection") {
            if let Some(config) = ctx.find_connection(conn_name) {
                config.clone()
            } else if let Ok((_profile, config)) =
                ctx.resolve_profile_connection(conn_name, Some("elasticsearch"))
            {
                config
            } else {
                return Err(VoidbError::Plugin(format!(
                    "Connection or profile '{}' not found",
                    conn_name
                )));
            }
        } else {
            return Err(VoidbError::Plugin(
                "Either --connection or --stdin-config is required".into(),
            ));
        };
        let msg = crate::test_connection(&conn)
            .await
            .map_err(|e| VoidbError::Connection(e.to_string()))?;
        println!("{msg}");
        Ok(())
    }

    /// Parse the ES config from the named connection in the CLI context.
    fn parse_config(conn_name: &str, ctx: &CliContext) -> Result<EsConfig, VoidbError> {
        let config = if let Some(config) = ctx.find_connection(conn_name) {
            config.clone()
        } else if let Ok((_profile, config)) =
            ctx.resolve_profile_connection(conn_name, Some("elasticsearch"))
        {
            config
        } else {
            return Err(VoidbError::Plugin(format!(
                "Connection or profile '{}' not found",
                conn_name
            )));
        };

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

fn profile_ref_arg(input: &str, profile_id: &str) -> ConnectionProfileRef {
    if let Some(id) = input.strip_prefix("id:") {
        ConnectionProfileRef::Id(id.to_string())
    } else if let Some(name) = input
        .strip_prefix("name:")
        .or_else(|| input.strip_prefix("alias:"))
    {
        ConnectionProfileRef::Name(name.to_string())
    } else if input == profile_id {
        ConnectionProfileRef::Id(input.to_string())
    } else {
        ConnectionProfileRef::Name(input.to_string())
    }
}
