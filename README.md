# VoidB Elasticsearch Plugin

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

External process plugin for [VoidB](https://github.com/limmytian/voidb), providing comprehensive Elasticsearch cluster management, index browsing, document search, and mutation capabilities.

## Architecture

This plugin runs as an autonomous, out-of-process plugin adhering to VoidB's `process-plugin-sdk` stdio-jsonrpc protocol.

### Capabilities Exposed

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

## Building and Installing

```bash
cargo build --release
mkdir -p bin
cp target/release/voidb-plugin-elasticsearch bin/
```

Then point VoidB to this directory via:
```bash
export VOIDB_PLUGIN_PATH=/path/to/voidb-plugin-elasticsearch
```

## Running Standalone

```bash
# Run the stdio-jsonrpc RPC server
./bin/voidb-plugin-elasticsearch serve
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
