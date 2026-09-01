# Warehouse-to-Go

Create a local **DuckDB** mirror of your data-warehouse sources so your dbt project runs
against tiny, instant local copies instead of the real warehouse. Easily paired with [dbt-polyglot](https://pypi.org/project/dbt-polyglot/) to transpile sql dialects on the fly with 0 re-writes.

The tool reads your dbt project's `manifest.json`, connects to the warehouse, extracts each
source table, and writes a faithful — but small, capped, and fast — copy back onto disk.

<div align="center">
  <img src="./terminalizer_render.gif"  style="max-width: 75%;" />
</div>

## ✨ Why

warehouse-to-go exists for two reasons.

- **Data stays available offline.** Once a source is mirrored, it lives on disk — no warehouse
  login, no API gateway, and no internet needed to query it. A local copy is a permanent data
  island you can build against anytime.
- **Cloud costs stay down.** Running dbt against a huge warehouse pays for planning, catalog scans,
  warehouse concurrency, and auth round-trips on every query. A tiny local copy runs fast and nearly free.

A `row_limit` lets you grab a capped slice of each table just for local work, and you iterate on your
logic and SQL against local copies — your warehouse and production queries stay untouched.

warehouse-to-go gives you a **local DuckDB** whose `database.schema.table` namespace matches the warehouse,
seeded from that capped snapshot of your real sources.

## 📦 Installation

```bash
git clone https://github.com/dancorley/warehouse-to-go.git
cd warehouse-to-go

# Create and activate a virtual environment
uv sync
source .venv/bin/activate      # On Windows: .venv\Scripts\activate
```

## 🧪 Try it

A ready-to-run sample project lives in [`dbt/`](./dbt/):

```bash
# Seed a local mirror from the bundled sample sources
warehouse-to-go -m dbt/target/manifest.json extract

# Build + run your dbt models against the local DuckDB mirror
dbt build --project-dir dbt --target duck
```

See [dbt/README.md](./dbt/README.md) for the sample project's setup and commands.


## 🧱 How it works

warehouse-to-go is built around a clean split of responsibility. The CLI changes **zero code**
when you add a new warehouse — you only "select the adapter from config."

```mermaid
---
title: 'How warehouse-to-go works'
---
flowchart LR
    A(["dbt<br/>manifest.json"]) -->|plan| B(["warehouse-to-go<br/>parse → connect → fetch"])
    B -->|"capped Table<br/>(pyarrow: columns, types, rows)"| C
    C -->|"sink: ATTACH sibling .duckdb per<br/>source database, write rows"| F
    F -->|"point dbt here"| G(["dbt<br/>models from source(...)"]))

    subgraph disk["on disk — one sibling .duckdb per source database"]
        F
    end
```

| Layer | Responsibility | Why it's separated |
|---|---|---|
| **Manifest parser** | Turns `manifest.json` into a `db.schema → [tables]` extraction plan | Same for every warehouse |
| **Config** | Picks the warehouse **type**, target, and where to write (`database_path`) | One selection point; no code forks |
| **SourceAdapter** | `connect` / `test_connection` / `fetch` / `close` | All source knowledge (auth, SQL, types, namespace) lives here |
| **Table payload** | `fetch()` yields a `Table` whose `rows` is a `pyarrow.Table` (columns, types, rows from source) | The sink consumes only this; no source-specific type knowledge needed |
| **DuckDB sink** | Attaches the databases, creates schemas, writes each `Table` in one statement | Never sees Snowflake vs. Postgres vs. BigQuery |

The connector between source and sink is a **pyarrow-backed intermediate** — each `fetch()` yields a
`Table` object whose `rows` is a `pyarrow.Table` carrying column names, types, and nullability from
the source. The sink writes each table with a single `CREATE OR REPLACE TABLE ... AS SELECT * FROM
<arrow>` — no batching, no Python-object round-trip.

## 🧠 Key design decisions (locked, so the architecture doesn't drift)

1. **DuckDB is a pure sink.** The sink knows only a `Table` (a `pyarrow.Table`) + a `CatalogLayout`;
   it never knows which warehouse the data came from. Add an adapter → no sink changes.
2. **One database per namespace on disk.** The configured `database_path` is a **directory** that
   holds a **sibling** `.duckdb` per source namespace (defaulting to the current directory). The
   **primary is always the in-memory hub** `:memory:` — you never point it at a `.duckdb` file. The
   sink `ATTACH`es each sibling and creates the schemas there. **The number of databases is not
   fixed** — it's adapter-declared and user-configurable.
3. **`row_limit` caps each table independently** (default `10000`). It is *not* the total row budget —
   each table gets up to `row_limit` rows.
4. **The CLI is generic.** "Select the adapter from `warehouse.type`; run." — the same command works
   for any warehouse once an adapter ships.

```
db               DuckDB database  DuckDB schema  DuckDB table   Storage
─────────────────────────────────────────────────────────────────────────────────────
Snowflake        database         schema         table          one .duckdb per database, ATTACHed
Postgres         database         schema         table          one .duckdb per database, ATTACHed
BigQuery         project          dataset        table          one .duckdb per project, ATTACHed
```

## ⚙️ Configuration

### `config.yml`

A `config.yml` in your project directory customizes the tool. Values are also settable on the
command line (`--profile`, `--target`, `--config`, `--manifest`).

```yaml
# config.yml
warehouse:
  # Which dbt profile holds your warehouse credentials.
  # If omitted, the first warehouse profile is used.
  profile_name: portable_warehouse

  # Which target within that profile (default: the profile's target).
  target: snow

  # The adapter is selected from the chosen dbt profile target's `type`.

duckdb:
  # Directory that holds every source database's sibling .duckdb
  # This should be the root dbt points at.
  database_path: dbt

extract:
  # Max rows per table. Capped per-table (default 10,000) — speeds local iteration.
  row_limit: 10000
```

### Configuration precedence

1. Command-line options (`--profile`, `--target`, `--config`, `--manifest`)
2. `config.yml` in the current directory (if present)
3. `~/.dbt/profiles.yml` for credentials (Snowflake now; any warehouse once its adapter ships)

warehouse-to-go reads your warehouse credentials from your existing dbt
`~/.dbt/profiles.yml`. It uses the profile and target named in `config.yml`, or the first
warehouse profile automatically.

## ▶️ Usage

```bash
# 1. Test warehouse + DuckDB setup
warehouse-to-go debug

# 2. List the sources dbt found, grouped by database.schema (with table counts)
warehouse-to-go -m dbt/target/manifest.json analyze

# 3. Preview what would be extracted from one source (no writes)
warehouse-to-go -m dbt/target/manifest.json extract --source sf100tcl --dry-run

# 4. Extract every reachable source into the local mirror
warehouse-to-go -m dbt/target/manifest.json extract

# 5. Run your dbt project against the local mirror (target: duck)
dbt build --project-dir dbt --target duck
```

- `analyze` groups sources by their `database.schema` and prints the table count per group —
  this is exactly the grouping each source namespace maps into the local mirror.
- `extract` prints each table's row count as it is written, followed by an aggregate table and row
  count. `--source NAME` narrows extraction to a single source.

## 💾 Storage model — how dbt consumes the mirror

This is the part most people ask about, so it deserves its own section.

The `database_path` is the folder that holds each `.duckdb` per source database. The sink opens an
in-memory primary and `ATTACH`es each sibling (by name) and creates the schemas, so **the
`database.schema.table` paths are identical to the warehouse** — only the data lives locally and is
row-capped.

```
dbt/                             ← the `database_path` directory (default: current directory)
├── snowflake_sample_data.duckdb ← sibling (source database)
│     └── tpch_sf1              ← schema (tables inside)
└── other_source.duckdb         ← sibling, when the source has its own database
```

In `dbt/profiles.yml`, point the `duck` target at a separate writable DuckDB file and attach the
sibling `.duckdb` files from `database_path`:

```yaml
duck:
  type: duckdb
  # Primary file used by dbt for its own models; source databases remain siblings.
  path: ./dbt/warehouse_mirror.duckdb
  database: warehouse_mirror
  attach: # the ATTACHed siblings
    - path: ./snowflake_sample_data.duckdb
      alias: snowflake_sample_data # aliased as named in warehouse
    - path: ./other_source.duckdb
      alias: other_source
```

Now `select * from snowflake_sample_data.tpch_sf1.customer` resolves to your local, capped copy.
This layout scales: add another source database → another sibling `.duckdb` in the same directory,
and the sink picks it up automatically. **You don't have to pick a single merged file** — the
sibling-per-database model mirrors the warehouse and keeps each namespace isolated.


## 🗺️ Adapters

**Snowflake** is the reference adapter — implemented and verified end-to-end. Adding another
warehouse means writing one file that implements the `SourceAdapter` protocol; the CLI, sink,
manifest parsing, and tests change nothing.

Want to contribute a new warehouse? The whole process is a **one-file change** — one new
adapter module that self-registers via `@register("<type>")`. The module is discovered
automatically through a **package entry point** declared in `pyproject.toml`; nothing in
`warehouse/__init__.py` needs editing. See **[docs/add-adapter.md](docs/add-adapter.md)**
for the step-by-step guide, the `SourceAdapter` protocol contract, and a copy-paste
scaffold. Short version:

```python
# 1) warehouse_to_go/warehouse/<name>_adapter.py  (implements SourceAdapter)
@register("<type>")
class <Name>Adapter(SourceAdapter):
    type_name = "<type>"
    def connect(self, config): ...
    def test_connection(self, config): ...
    def fetch(self, identifier, columns, limit): ...
    def close(self): ...
    # `build_fetch_query()` is the default; override it (or `_qualified_reference()`)
    # for a warehouse whose identifiers resolve differently (e.g. Snowflake's `IDENTIFIER(...)`).
    def build_layout(self, config, plan): ...

# 2) register the module in pyproject.toml under
#    [project.entry-points."warehouse_to_go.adapters"]
#    (e.g. <name> = "warehouse_to_go.warehouse.<name>_adapter"), then point a
#    dbt profile at  type: <your type>. No __init__.py edits.
```

The sink, CLI, and manifest parser never change — your adapter only produces
`pyarrow.Table` payloads, and "select it from `warehouse.type`" is all a user has to do.

| Warehouse | Status |
|---|---|
| Snowflake | ✅ Reference adapter (live connection + CLI end-to-end) |
| Postgres | ✅ DuckDB `postgres_scanner` + multi-schema layout. End-to-end sample in [`POSTGRES_ADAPTER.md`](POSTGRES_ADAPTER.md) |
| BigQuery | 🚧 Planned — service-account keyfile auth; native DuckDB extension |


## 🔎 What's tested

The suite locks the machinery — nothing here should silently regress:

- **Registry** — the adapter registry starts empty, dispatches by `warehouse.type`, and raises a
  clear error for unknown types
- **Discovery** — a new adapter is found *only* via its `pyproject.toml` entry point, and discovery
  fails loudly if an adapter is neither registered nor discovered on its own
- **Sink** — Arrow-based writes into multiple sibling databases, single-load row accounting,
  empty-table handling, a guard against writing to a database absent from the layout, and setup
  creating sibling databases inside the configured directory prefix
- **Config** — `from_dbt_profile` reads the warehouse `type` from the profile, strips dbt-only
  keys, validates the type selector against the registered adapters, and preserves generic
  provider fields
- **Adapters** — each adapter module is tested in isolation (no live warehouse needed):
  registry dispatch by `warehouse.type`, injection-safe `build_fetch_query` (manifest values
  can't escape into SQL), `CatalogLayout` mapping schemas and tables onto sibling `.duckdb`
  files, and correct path resolution for the on-disk mirror
