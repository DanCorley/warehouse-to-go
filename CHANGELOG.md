# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project aims for semantic-release compatibility.

## [0.2.0] - 2026-08-28

### Added

- **Multi-adapter protocol** (`warehouse/`)
  - `SourceAdapter` ABC (`connect`, `test_connection`, `fetch`, `close`,
    `_qualified_reference`/`build_fetch_query`, `build_layout`)
  - `Table` payload carrying a `pyarrow.Table` (columns, types, nullability from source)
  - Self-registering adapters via `@register("<type>")`, discovered through a
    PEP 682 entry-point group (`warehouse_to_go.adapters` in `pyproject.toml`)
  - `CatalogLayout` / `CatalogDatabase` mapping source namespaces onto DuckDB's
    `database.schema.table` topology (one sibling `.duckdb` file per source database)
- **DuckDB sink** (`sink/duckdb_sink.py`) — the single, dialect-blind write path:
  `ATTACH` each sibling database, `CREATE SCHEMA IF NOT EXISTS`, then one
  `CREATE OR REPLACE TABLE ... AS SELECT * FROM src` per table
- **Snowflake adapter** (`warehouse/snowflake_adapter.py`) — `fetch()` pulls each
  capped table as a single `pyarrow.Table` via `cursor.fetch_arrow_all()`, with
  PEM→DER auth isolated behind the adapter
- Tests: `test_sink.py`, `test_registry.py`, `test_discovery.py`, `test_config.py`

### Changed

- Extracted source knowledge into adapters; the CLI, sink, manifest parser, and
  tests no longer know about Snowflake
- Dropped `pandas` as a direct dependency (was only needed by the old extractor);
  `snowflake-connector-python` upgraded 3.14.0 → 4.2.0 (`[pandas]` extra kept);
  added `pyarrow>=17.0.0`; `duckdb` → 1.5.5, `PyYAML` → 6.0.1, `rich` → 14.0.0
- Bumped minimum Python to `>=3.11`
- Rewrote `README.md` (lighter, dialect-agnostic overview with a "why",
  `uv sync` install, and how the pipeline works)
- Removed `dbt/config.yml`

### Removed

- `extractor/snowflake_extractor.py` — monolithic extractor with pandas batching
- `warehouse/types.py` — unused Snowflake→DuckDB `Type` registry
- `tests/test_extract.py`, `tests/test_manifest_parser.py`, `tests/test_warehouse_types.py`

### Documentation

- Added `docs/add-adapter.md` (the pattern for adding a warehouse)
- Updated `dbt/README.md` for the new layout
