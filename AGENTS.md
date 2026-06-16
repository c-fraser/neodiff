# Agents

This file provides guidance to coding agents collaborating on this repository.

## Project Vision

A convenient and efficient tool for identifying the differences between Neo4j graphs.

## Project Requirements

- Always use English in code, examples, and comments.
- Features should be implemented concisely, efficiently, and maximally maintainable.
- Code is not just for execution, but also for readability.
- Only add meaningful comments and tests.

## Development Commands

### Rust Development

* Check, format, and lint code: `make check`
* Run tests: `make test`

## Key Technical Details

* **Async-first Architecture**: Heavy use of tokio and async/await throughout Rust codebase.
* **Sorted Merge Join**: Node/relationship comparison streams both sides ordered by `__id` from Neo4j and performs a
  linear merge join. No full collections are loaded into memory; the stream is consumed incrementally.
* **APOC Dependency**: Both source and target Neo4j instances must have the APOC plugin installed. It is used for
  `apoc.hashing.fingerprint` (unconstrained node identity) and `apoc.map.fromPairs` (property exclusion filtering).
* **Node Identity Strategy**: Nodes with `UNIQUENESS` or `NODE_KEY` constraints are identified by their constraint
  property values. Nodes without constraints fall back to an APOC property fingerprint. Similarity matching then
  handles
  fuzzy pairing of fingerprint-identified nodes above a configurable threshold.
* **TUI Threading Model**: The TUI renders on a dedicated sync thread; the async diff engine runs on the tokio thread
  pool. They communicate via `mpsc::UnboundedChannel<TuiMessage>` and share `Arc<StdMutex<TuiState>>`. Tracing events
  are
  forwarded to the TUI log panel via a custom `TuiTracingLayer`.
* **Sled-Backed Diff Storage**: The TUI writes diffs to a temporary sled database keyed by monotonically increasing
  `u64` sequences. Only a lightweight `DiffIndex` is kept in memory; diffs are loaded lazily at render time, bounded by
  `TREE_PAGE_SIZE` and `REL_PAGE_SIZE`.

## Development Notes

* `lib.rs` is the published crate (diff engine and public API); `main.rs` is the CLI/TUI binary. Keep them clearly
  separated: library code must not depend on TUI types.
* All public APIs should have comprehensive documentation with examples.

## Code Style

* ALl files should adhere to 100-character line limit.
* All inline comments should begin with a lowercase letter and not end with punctuation.

### Rust API

* Design public APIs so they can be evolved easily in the future without breaking changes. Often this means using
  builder patterns or options structs instead of long argument lists.
* For public APIs, prefer inputs that use `Into<T>` or `AsRef<T>` traits to allow more flexible inputs. For example,
  use `name: Into<String>` instead of `name: String`, so we don't have to write `func("my_string".to_string())`.
* Errors are `Box<dyn Error + Send + Sync>` throughout; do not introduce `anyhow` or other error-wrapping crates.
* Avoid `unwrap()` in library code; prefer `unwrap_or_default()`, `?`, or explicit handling. `unwrap()` is acceptable
  in tests and in cases where an invariant is statically guaranteed.

### Testing

* Ensure all new public APIs have documentation and examples.
* Ensure that all bugfixes and features have corresponding tests.
* Integration tests share a single Neo4j container pair initialized once per test run via
  `OnceLock<AsyncMutex<Option<TestEnv>>>`. Do not start new containers per test.
* Each integration test must call `env.clear()` at the start to reset graph state left by previous tests.
* Test Neo4j instances require the APOC plugin (`Neo4jLabsPlugin::Apoc`); tests that exercise unconstrained nodes or
  property exclusion will fail without it.
* Unit tests (no containers) cover pure Rust logic only: property diffing, similarity computation, tag names, etc.
  Integration tests cover end-to-end `diff_graphs` behavior.

### Documentation

* New features must include updates to the rust documentation comments. Link to relevant structs and methods to
  increase the value of documentation.
