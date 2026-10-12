# VoidB Elasticsearch Plugin (`voidb-plugin-elasticsearch`)

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

External process plugin for [VoidB](https://github.com/limmytian/voidb), providing comprehensive Elasticsearch cluster management, index browsing, document search, and mutation capabilities.

## Architecture

This plugin runs as an autonomous, out-of-process plugin adhering to VoidB's `process-plugin-sdk` stdio-jsonrpc protocol.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
- **Tri-Modal Operation**:
  - `test`: Automated connectivity verification.
  - `serve`: `stdio-jsonrpc` protocol server for AI Agents and automated orchestration.
  - Domain commands: Rich cluster health, node inspection, index catalog, search DSL, and document management.
- **Capabilities Exposed**:
  - `diagnostics`: Agent-safe profile diagnostics without opening live cluster connections.
  - `health`: Cluster health inspection.
  - `nodes`: Bounded cluster node listing.
  - `indices`: Index catalog inspection.
  - `search`: Query DSL search execution with pagination/cursor support.
  - `search_stream_read`: Bounded PIT / scroll streaming session consumer.
  - `get`: Document retrieval by ID.
  - `count`: Document count matching query filters.
  - `mapping`: Schema and field mapping inspection.
  - `bulk`: Destructive bulk document indexing, deletion, and updates.
  - `raw_api`: Direct REST API passthrough with safety and confirmation gating.

## Usage & Operational Modes

When installed, `voidb-cli` automatically discovers this plugin and provides seamless transparent dispatch:

### 1. Human Operations (CLI)

```bash
# Connectivity test
voidb-cli elasticsearch test --profile local-es

# Cluster health and nodes
voidb-cli elasticsearch health --profile local-es
voidb-cli elasticsearch nodes --profile local-es

# Indices and mappings
voidb-cli elasticsearch indices --profile local-es
voidb-cli elasticsearch mapping my-index --profile local-es

# Search and documents
voidb-cli elasticsearch search my-index --query '{"match_all":{}}' --size 10 --profile local-es
voidb-cli elasticsearch get my-index doc-123 --profile local-es
voidb-cli elasticsearch count my-index --query '{"match_all":{}}' --profile local-es
```

### 2. Machine Communication (AI Agents & JSON-RPC)

```bash
# Run the stdio-jsonrpc server directly or via voidb-cli
voidb-cli elasticsearch serve
# Or directly run the binary:
./bin/voidb-plugin-elasticsearch serve
```

## Quick Start

### Installation

Place this plugin directory or a packaged release archive under your VoidB plugins directory (`~/Library/Application Support/voidb/plugins/elasticsearch` on macOS, or `~/.local/share/voidb/plugins/elasticsearch` on Linux):

```bash
mkdir -p "$HOME/Library/Application Support/voidb/plugins/elasticsearch"
cp -r plugin.toml bin schemas "$HOME/Library/Application Support/voidb/plugins/elasticsearch/"
```

Verify discovery via `voidb`:

```bash
voidb-cli plugin list
voidb-cli elasticsearch --help
```

### Development & Build

```bash
cargo build --release
mkdir -p bin
cp target/release/voidb-plugin-elasticsearch bin/
```

## Protocol Specifications

Complies with the [VoidB Process Plugin Protocol](https://github.com/limmytian/voidb/blob/main/docs/quickstart-process-plugin.md) specification (v1.0).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Optional local SDK development

Release and CI builds resolve SDK/core from the immutable Git revision in Cargo.toml. To test against a sibling `../voidb` checkout, explicitly opt in with `cargo --config .cargo/config.local.example.toml test --all-targets`. Do not commit an active `.cargo/config.toml` path override.

## Authenticated releases

Starting with v0.3.4, managed installation in VoidB v0.3.5 requires a signed
`provenance.json` covering identity, target, executable, manifest and schemas.
Build jobs produce unsigned staging archives. The separate release signing job
signs all four native targets in the protected `plugin-release-signing` environment
and validates the complete payload before publishing a new immutable release.
The owner must configure environment protection and its signing secret before
publication. Existing v0.3.3 release assets are never replaced.

`VOIDB_PLUGIN_SIGNING_KEY_PEM` belongs only to the protected signing step. Do not
put private keys in this repository, packages, command arguments or logs. Core
contains the trusted public key; registry fields and editable installation records
cannot add runtime trust. Local unsigned development requires both an explicit
`VOIDB_PLUGIN_PATH` directory and `VOIDB_ALLOW_UNSIGNED_PLUGINS=1`.
