# Changelog

All notable changes to Dux are documented here.

## [Unreleased]

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
- Credo issues — reduce arity, replace length > 0

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
- Color-coded Inspect display using Inspect.Algebra
- Head/1 defaults to 10 rows

### Miscellaneous

- Add dependency lockfiles
- Add credo linting, mise config, format code
- CI workflows, rustler_precompiled, Hex packaging
- Cargo fmt
- Fix all credo issues — alias, nesting, TODO tag
- Clean up distributed graph — keep distributed PageRank, defer CC
- Bump version
- Mix format
- Add git-cliff for automated changelog generation

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
