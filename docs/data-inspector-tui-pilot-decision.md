# Elasticsearch Data-Inspector TUI Pilot Decision (Requirement 83)

- **Plugin / profile type**: `voidb-plugin-elasticsearch` / Elasticsearch HTTP
  profiles (`urls` list, optional basic/API-key/bearer auth, optional TLS
  verification and timeout settings).
- **Selected state**: `Accepted pilot`.
- **Command**: `voidb-plugin-elasticsearch tui` (plugin-owned, CLI-launched),
  also routed as `elasticsearch tui` through `voidb-cli`.
- **Scope**: search triage. This is new plugin-owned UI work built on the
  existing `EsService` channel contract; it adds no backend operations and
  does not depend on removed router-hosted factories, shared legacy data
  widgets, or shell-owned plugin UI state.
- **Gate precedent**: this record follows the accepted Redis data-inspector
  TUI pilot (`voidb-plugin-redis/docs/data-inspector-tui-pilot-decision.md`)
  and the MySQL adaptation of it
  (`voidb-plugin-mysql/docs/data-inspector-tui-pilot-decision.md`), which
  established that a bounded plugin-owned pilot with fixture, target-error,
  and real PTY evidence produced in the same requirement satisfies the
  Requirement 83 gate. Elasticsearch previously sat at `Later candidate`,
  shipping as `CLI-only`, in
  `voidb/docs/data-inspector-cli-first-decisions.md`, which required that
  "a future `elasticsearch tui` must prove search triage value over CLI and
  generic invoke". That pilot is implemented, validated, and accepted here.

## Named human workflow

**Search triage on a live index space.** An operator responds to an error
spike or a bad-release report:

1. **Start**: the operator connects through an existing profile
   (`elasticsearch tui --profile prod-search --purpose triage`). The TUI opens
   read-only on the indices view; the first viewport is the bounded
   `/_cat/indices` listing with doc counts, so oversized or red indices are
   visible before anything is opened.
2. **Browse**: the operator moves the cursor to `logs-app-2026.11` and presses
   `i`; the full-screen paged hits viewer opens on the first bounded search
   page (`size=10, from=0` through `DocumentCommand::Search`), never a full
   dump, with the page title reporting `from=`, `total=`, and `size=`.
3. **Page**: `n` fetches `from+10`, `b` walks back through offset history;
   pages are **replaced**, not appended, so the viewport stays bounded. `/`
   filters the current page client-side with a live match count.
4. **Inspect**: `Enter` on a hit opens the typed hit viewer — `_id`, `_index`,
   `_score` (literal `null` when absent), and pretty-printed `_source` with
   nested objects scrollable inside a bounded line buffer. `m` opens the
   mapping viewer (property list plus raw mapping JSON) for the selected index
   or the index already open in the hits viewer.
5. **Act**: for a poisoned document (a leaked secret, a test artifact) the
   operator presses `d`, **types the exact index name**, and presses `y`; the
   delete is a single `DELETE /<index>/_doc/<_id>` through the existing
   `DocumentCommand::Delete`, and the current page is refetched so the
   document disappears and the total drops. A failed delete (locked index,
   cluster blocks, permissions) shows a redacted target error in a non-modal
   banner and the operator keeps working.
6. **Success state**: without leaving the TUI, the operator has walked
   indices → hits page → hit → one removed document, with the refreshed page
   (`total=13`, `1/9` in the fixture transcript) as confirmation. Index
   management DDL, document writes, `delete_by_query`, and bulk deletes are
   **not** part of this slice (explicitly deferred below).

**Why the CLI is materially worse here**: the same loop requires
`elasticsearch indices`, then one
`elasticsearch search -i <index> -n 10 --from <offset>` invocation per page,
plus a separate `elasticsearch get` per candidate document and a raw API call
for the delete. Every step pays a fresh process startup and none share index,
page, filter, or cursor context; the operator re-derives pagination offsets in
shell history. The TUI keeps the index → page → hit state, per-view selection
memory, per-page filter, and typed hit inspection in one viewport, so an
operator walks a suspect index page by page in one sitting. Generic "browse
data" is not the claim; the claim is cursor-preserving triage over repeated
browse-page-inspect cycles across the indices of one cluster.

## Gate-by-gate evidence

| Gate | Evidence in this implementation |
|---|---|
| Named human workflow | Above; exercised end to end in fixture mode and by the PTY transcript (open `logs-app-2026.11` → page forward/back → typed hit viewer → filter → typed delete of `log-0001`, refetch shows `total=13` and the document gone from the page). |
| Capability-backed bounded reads | Reads are the existing service-layer operations only: `IndexCommand::List` (`/_cat/indices` with doc counts, system indices excluded by the service), `DocumentCommand::Search { index, query, size, from }` (explicit `size=10`/`from` bound in `service/document.rs`), `IndexCommand::GetMapping` (typed property list plus raw body), and `ClusterCommand::Health`/`ListNodes`. The TUI adds no new backend operations; there is no free-form query DSL or raw-API surface in the pilot. |
| Pagination and large-result safety | One search page (`size=10`) is rendered at a time (`hits <index> page N (sel/vis) from=… total=… size=10` in the viewer title); `n` fetches `from+10` only and pages are **replaced**, never appended, so the viewport stays bounded; the previous offset is kept in a history stack for `b`. Empty state (`metrics-empty`: "No hits on this page…"), partial page (`orders-prod`: 3 hits on one page), and end-of-hits state are all visible; hit-detail and mapping viewer bodies are bounded by `BoundedLines` at 2,000 lines / 2 MiB with dropped-line counters. Doc counts come from `/_cat/indices`, never a scroll. |
| Redaction and error shape | Preflight (`--format json`) reports auth as `"basic"|"api_key"|"bearer"|"none"` with `"secret_material": "redacted"` and no connection URL (only a userinfo-stripped `url_host_label`); `secret_leak_markers` scan covers `es_password_value`, `es_api_key_value`, `es_bearer_token_value`, `plaintext_credential_value`, `raw_connection_url` and passes (`passed: true`, 0 markers) in `build_elasticsearch_tui_evidence` output; errors render through `safe_error_summary` in the non-modal banner and keep the service-layer error text (`HTTP <status> <body>` target errors, connection failures, and fixture `target error:` strings). |
| Mutation safety | First viewport is read-only. `--readonly` launch stages plans but blocks at confirm with `readonly launch blocked delete document on <index>/<id>`. The only mutation is `DocumentCommand::Delete { index, id }` — **one document by `_id`** — and requires **typed confirmation of the exact index name** plus explicit `y` (two confirmations, `destructive` risk), with the `index/_id` target echoed in the confirmation dialog, the staged-plan details panel, and the execution status. There is no `delete_by_query`, no bulk delete, no index delete/create, no document upsert, and no raw-API access anywhere in the TUI (recorded in `evidence["never_shipped"]`). Mutation policy is recorded in `evidence["mutation_refetch_policy"]`. |
| Fixture and target-error evidence | `fixtures/elasticsearch_tui_operations.json` covers 6 indices (green/yellow/red, empty and populated), a 14-hit index over two pages (full/partial/empty), nested `_source` documents with a `null` score, mappings for populated and empty indices, cluster health plus 3 nodes, and a deliberately erroring document (`log-locked-003` → `target error: permission denied while deleting locked document (fixture)`). Startup carries a `target error: cluster_block_exception …` banner; the PTY proves the TUI stays fully responsive across it (all assertions pass with the banner present on the first frame). |
| PTY lifecycle evidence | Real PTY run (`pty.fork()` + `TIOCSWINSZ` `HHHH` at 36x130, winsize set pre-exec): 53/53 assertions across 3 consecutive runs. First frame markers (`Elasticsearch Search TUI`, `1:indices`, `2:cluster`, `fixture-elasticsearch-ops`, fixture rows, hint footer, target-error banner); hits viewer paging forward/back with offset history; typed hit viewer with pretty `_source`; mapping and cluster viewers; filter with narrowed rows; typed-delete confirmation and refetch; resize to 24x100 keeps content visible and reports `resized elasticsearch view to 100x24`; `q` exits 0; raw-mode cleanup is delegated to `ratatui::restore()` after the loop (raw input is not used: `raw_input = false`). |
| No legacy dependency | The command lives in the plugin (`src/tui.rs`), is launched from the plugin CLI (`elasticsearch tui`), consumes only `EsService` channel events, and contains no reference to removed router-hosted data-inspector adapters or shared legacy widgets. The agent-session/`search_stream_read`/agent_live machinery is untouched. |

## Interaction surface

- Views and hotkeys: `1` indices browser, `2` cluster view (Tab cycles);
  pending fetches show the family `⟳` marker in the header tabs.
- Indices browser: name/health/doc-count/store-size columns with a details
  panel (status, shards); `i`/`Enter` opens the full-screen paged hits viewer
  (`size=10, from=0` pages from `DocumentCommand::Search`) with `j/k`/PgUp/
  PgDn row cursor, `n` next page, `b` back through offset history, and `/`
  client-side filter on the current page with a live match count.
- Hit viewer: `Enter` on a hit opens the typed full-screen viewer (`_id`,
  `_index`, `_score` or literal `null`, pretty `_source`) with `j/k`/PgUp/PgDn
  scroll and a bounded line budget; `d` stages the delete for the open hit.
- Mapping viewer: `m` on an index (indices view or inside the hits viewer)
  renders the typed property list plus the raw mapping JSON.
- Cluster view: node list with health summary and a full-screen cluster viewer
  (`i`/Enter); `r` refreshes health and nodes.
- Manage mutation: `d` typed delete (type the exact index name + `y`, `_id`
  echo throughout); every mutation refetches the current page.
- Confirm-quit fires while an index/search/cluster/mapping fetch or a staged
  plan is active; the error banner is non-modal (any key acknowledges while
  the key keeps its meaning).

## Deferred inside the accepted pilot

- **Index management DDL** (`IndexCommand::Create`, `IndexCommand::Delete`,
  `IndexCommand::Stats`): `later candidate`; blast radius is beyond any slice
  mutation gate — an index delete must never sit one keystroke away from a
  document delete.
- **Document writes** (`DocumentCommand::Index` upsert): `later candidate`;
  value-editing UX needs its own fixture, confirmation, and redaction
  evidence before it can ship.
- **`delete_by_query` and bulk deletes**: never shipped, and recorded in
  `evidence["never_shipped"]`; the service layer exposes no such command and
  the TUI must not synthesize one through `RawApi`.
- **Aliases view** (`IndexCommand::ListAliases`) and cluster info view
  (`ClusterCommand::Info`): `later candidate`; present in the service channel
  contract but outside the search-triage surface of this pilot.
- **Server-side query DSL editing**: `later candidate`; the pilot's `/` stays
  a client-side filter on the current page. Until server-side query editing
  ships, docs and release copy must keep pointing to the CLI
  (`elasticsearch search/get/indices/health/count/mapping/api`, and
  `voidb-cli invoke run elasticsearch.<capability> …`) as the supported
  general path for queries, writes, and arbitrary API calls.
- `plugin.toml` declares `[ui] tui = true, raw_input = false` for this bounded
  pilot only.

## Validation commands and evidence

All run in `voidb-plugin-elasticsearch` with `VOIDB_DISABLE_KEYRING=1` (no OS
keyring touch):

```bash
VOIDB_DISABLE_KEYRING=1 cargo test            # 38 passed (19 pre-existing + 19 TUI)
cargo clippy --all-targets -- -D warnings     # 0 warnings (CI enforces this)
cargo fmt --all && cargo fmt --all -- --check # clean (CI enforces this)
cargo run --example export_contract -- --check # manifest/contract drift green
```

TUI interaction tests (in `src/tui.rs#[cfg(test)]`): fixture evidence +
secret-leak scan, preflight auth redaction, non-modal startup banner, view
hotkeys and tab cycling, per-view selection memory, hits paging with
offset/total state and history, filter match counts (indices and hits), hit
viewer typed fields with pretty `_source` and `null` score, mapping viewer,
cluster health/nodes viewer, typed-delete exact-index-name gate with `_id`
echo, delete-from-hit-viewer targeting, delete-execute + page refetch,
readonly block, erroring-document responsiveness, empty-index state,
partial-page state, confirm-quit, page-key movement.

Preflight and evidence:

```bash
target/debug/voidb-plugin-elasticsearch tui --fixture fixtures/elasticsearch_tui_operations.json --format json
target/debug/voidb-plugin-elasticsearch tui --fixture fixtures/elasticsearch_tui_operations.json --evidence evidence.json
```

Evidence JSON: `kind: elasticsearch_tui_fixture_evidence`, quality gate
`retained_tui_quality_gate("elasticsearch", ["fixture-elasticsearch-ops", "fixture-elasticsearch-local"], ["target error"], 80, 24, 10_000)`,
23 coverage entries including the new interactions
(`hits_paging_offset_history`, `typed_delete_confirmation`, `delete_id_echo`,
`delete_execute_refetch`, `hit_detail_viewer_pretty_source`, `mapping_viewer`,
`cluster_health_viewer`, `mutation_refetch`, …), `never_shipped` list,
`mutation_refetch_policy`, and `secret_leak_scan.passed = true`.

PTY lifecycle record (macOS, Python 3.9 stdlib only, `struct "HHHH"`, 36 rows
x 130 cols, winsize set pre-exec): 53/53 assertions, 3 consecutive clean runs,
exit code 0 each run; script retained out-of-repo at
`/tmp/es_tui_pty_smoke.py` (run `python3 -I /tmp/es_tui_pty_smoke.py`).
Covered: first frame (non-blank, marker-checked), non-modal banner
acknowledgement, indices filter with match count, cluster viewer, mapping
viewer, full-screen hits viewer with paging forward/back and offset history,
typed hit viewer with pretty `_source`, hits filter with narrowed rows, empty
index state, typed-delete confirmation (wrong name refused, `_id` echo) and
refetch, live resize to 24x100 with content retention, normal `q` quit with
clean exit 0 and terminal restore. Local Python is 3.9 (no tomllib), so the
python provenance tests cannot run on this machine (CI runs 3.11+); this does
not affect the PTY evidence, which uses only stdlib.

## Migration note

This pilot is new plugin-owned UI work. It does not restore, wrap, or depend
on the router-hosted Elasticsearch data-inspector behavior removed in
Requirement 77, and it does not reintroduce shared legacy data widgets or
shell-owned plugin UI state. The default TUI guidance for Elasticsearch
profiles remains the CLI/capability path; `elasticsearch tui` is a bounded,
profile-scoped search-triage pilot whose only supported mutation is a single
document delete by `_id` with typed confirmation. Release docs must not
advertise the deferred index DDL, document-write, alias, cluster-info,
server-side query editing, `delete_by_query`, or bulk-delete workflows as
available in the TUI.
