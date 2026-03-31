# Changelog

All notable changes to Dux are documented here.

## [0.3.0] - 2026-03-31

### Bug Fixes

- Mermaid rendering in ex_doc (exdoc:loaded + mermaid v11)
- Asof_join doc group :join → :joins (#31)

### Documentation

- Add pre-production callout to README (#28)
- FLAME cluster guide + fixes for distributed AVG rewrite, macro replay, n_rows (#47)

### Features

- Add Dux.exec/1 for raw DDL execution (#29)
- Add cond/if/in to Query macro, document function pass-through (#32)
- Per-worker stats in meta, remove pool-name status overload (#38)
- Connection pool with persistent_term dispatch (#41)
- SQL macros — Dux.define, define_table, undefine, list_macros (#42)
- Shuffle hardening — memory limits, spill-to-disk, exchange serialization, skew mitigation (#43)

### Miscellaneous

- Post release (#26)
- Upgrade ADBC 0.10 → 0.11 (#45)

### Performance

- Streaming merger delegates to DuckDB instead of Elixir-side folding (#34)
- Use IPC instead of Elixir rows for table replication to workers (#35)
- Eliminate data round-trip in Backend.query/2 (#40)
- View-based compute with schema derivation (#46)

### Refactoring

- Build graph adjacency in DuckDB instead of Elixir MapSets (#36)

## [0.2.0] - 2026-03-24

### Bug Fixes

- Nx zero-copy types, connected components convergence, ADBC v0.10.0 (#14)

### Documentation

- Comprehensive documentation uplift (#3)
- Comprehensive distributed execution guide + docs uplift (#24)

### Features

- Telemetry instrumentation across all layers
- FLAME elastic compute integration
- Migrate to ADBC (#2)
- Distributed & graph algorithm improvements (#4)
- Shuffle skew detection + bloom filter pre-filtering (#5)
- Lattice framework for distributed aggregate merging (8b Phase A) (#6)
- Approximate betweenness centrality (Brandes' algorithm) (#7)
- Streaming merger for lattice-compatible distributed aggregation (#8)
- ASOF JOIN + JSON processing verbs (#9)
- Cross-source joins via ATTACH (#10)
- Update ADBC + zero-copy IPC ingest and Nx tensor path (#12)
- Insert_into verb + distributed DuckLake reads (#13)
- Size-balanced file assignment in Partitioner (#15)
- Hive partition pruning in distributed reads (#18)
- Distributed writes — workers write directly to storage (#19)
- Distributed Postgres reads via hash-partitioned ATTACH (#20)
- Partition_by option for Hive-partitioned Parquet output (#21)
- Distributed insert_into — workers INSERT in parallel (#22)
- Excel IO — from_excel/2 and to_excel/2 (#23)

### Miscellaneous

- Bump to dev version
- Docs touchups (#11)
- Expand CI matrix to full OTP × Elixir lattice (#17)
- Prep for v0.2.0 (#25)

### Testing

- Add peer tests for size-balanced partitioner (#16)

## [0.1.1] - 2026-03-22

### Bug Fixes

- SQL injection hardening — escape identifiers and options
- Eliminate temp table GC race via process dictionary
- Livebook setup cells — Mix.install alone in setup block
- Unused variable warning in distributed test
- Quote identifiers in Merger and Broadcast SQL
- V0.1.0 release readiness fixes
- GC race in Worker.register_table + distributed PageRank
- Distributed parquet partitioning — Merger re-aggregation + compute idempotency
- Make rustler dep unconditionally optional for Hex
- Release workflow — use cross-version: from-source
- Add NIF version features to Cargo.toml for precompilation
- NIF features — default to 2_16, pass through to rustler crate
- Drop x86_64-unknown-linux-musl target
- Link rstrtmgr.lib on Windows for DuckDB Restart Manager APIs

### Documentation

- Document non-deterministic ordering of distinct/1
- README, guides, cheatsheet, changelog for 0.1.0
- Convert guides to .livemd with atom keys
- Remove commented output from livebooks, fix examples
- Proper Livebook format — setup cell, self-contained examples
- Remove spark-killer language
- Add RELEASING.md with rustler_precompiled release flow
- Comprehensive docs pass — Graph options, README verbs, cheatsheet
- Update distributed guide — auto join routing, remove Broadcast ref

### Features

- Initial Dux project scaffolding
- DuckDB NIF layer with connection management
- Start Connection in supervisor, add property tests
- Verb API with lazy pipelines and CTE compilation
- Query macro compiling Elixir expressions to DuckDB SQL
- IO via DuckDB — CSV, Parquet, NDJSON read/write
- Dux.Graph — graph analytics as verb compositions
- GC sentinel NIF + upgrade to Rustler 0.37
- Distributed resource tracking (Remote GC)
- Remote Worker — DuckDB per BEAM node with :pg discovery
- Coordinator — distributed query fan-out and merge
- Broadcast joins for star-schema distributed queries
- Nx zero-copy interop — LazyContainer + to_tensor
- Benchmark suite with Benchee
- Atom_keys option for collect/1 and to_columns/1
- Pretty-printed SQL preview
- Display — Inspect protocol, Table.Reader, peek
- Graph inspect with summary stats
- Distributed shuffle join for large-large joins
- Pivot_wider and pivot_longer verbs
- Distributed correctness — pipeline splitter, aggregate fixes
- Distributed PageRank via broadcast pattern
- Distributed connected components via broadcast-iterate pattern
- Distributed shortest_paths and triangle_count
- Distributed STDDEV/VARIANCE decomposition + shared SQL helpers
- Distributed join routing — auto-broadcast local right sides
- Shuffle join routing for large right sides in distributed joins
- Multi-column join support for Shuffle + peer tests for join routing
- Show distribution info in Inspect display
- Embedded test datasets + data-driven E2E tests

### Miscellaneous

- Add dependency lockfiles
- Add credo linting, mise config, format code
- CI workflows, rustler_precompiled, Hex packaging
- Cargo fmt
- Fix all credo issues — alias, nesting, TODO tag
- Clean up distributed graph — keep distributed PageRank, defer CC
- Bump version
- Prepare for v0.1.1 release (#1)

### Refactoring

- Macros on Dux, verb/verb_with split, remove Dux.Q
- Atom keys in from_list, cleaner docs
- Distribution as a struct property
- Rename API — distributed→distribute, collect→to_rows, new collect semantics
- Consolidate escape_ident/esc/qi to shared Dux.SQL.Helpers.qi/1

### Testing

- Comprehensive NIF and connection tests
- Comprehensive verb API tests
- SQL injection security tests + fix from_list identifier escaping
- End-to-end integration tests + fix join ON clause aliasing
- Distributed :peer integration tests
- Full distributed workflow — create, distribute, aggregate, collect
- :peer distributed correctness tests on real BEAM nodes
- Distribute API verification tests + update distributed_test to new API
- Peer-node integration test for auto-broadcast join routing


