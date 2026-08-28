# Adding a new warehouse adapter

This guide explains what it takes to add support for a new warehouse (Postgres, BigQuery,
Redshift, …). The design goal is that **adding an adapter never touches the sink, the CLI, the
manifest parser, or the tests** — you write one file that implements the `SourceAdapter`
protocol and register it. Then "select it from config and run."

## Why it's a small change

warehouse-to-go is split into four layers:

| Layer | Responsibility | Changes when you add an adapter? |
|---|---|---|
| **Manifest parser** | Turns `manifest.json` into a `db.schema → [tables]` plan | No — same for every warehouse |
| **Config** | Picks the warehouse `type`, target, and write path | No — you only add a `type` to the registry |
| **SourceAdapter** | All source knowledge: auth, SQL, types, naming/namespace | **Yes — you implement this** |
| **DuckDB sink** | Writes each `Table` (a `pyarrow.Table`) into sibling `.duckdb` files | No — adapter-agnostic |

The connector between source and sink is a **pyarrow-backed `Table` payload**. Your adapter's
only job is to produce those payloads. The sink never sees "Snowflake vs. Postgres vs.
BigQuery" — it just consumes `Table` objects. See [../README.md#-how-it-works](../README.md#-how-it-works)
for the full picture.

## The 4 commands every adapter must implement

`SourceAdapter` is an abstract base class in `warehouse_to_go/warehouse/__init__.py`. You **must**
implement these six methods (five required, one optional):

| Method | Signature | What it does |
|---|---|---|
| `connect` | `(config) -> Any` | Open the warehouse connection. Return the **raw handle** (the adapter stays stateless between calls). |
| `test_connection` | `(config) -> None` | Verify connectivity without fetching data. |
| `fetch` | `(identifier, columns, limit) -> Table` | Pull up to `limit` rows for the `identifier` as a **single `Table`** (one `Table` per call). See `fetch()` rules below. |
| `close` | `() -> None` | Release the connection. |
| `build_layout` | `(config, plan) -> CatalogLayout` | Map the extraction plan onto a DuckDB catalog layout. See below. |
| `use_context` | `(**context) -> None` | **Optional.** Session-setup hook (warehouse/role/session). Defaults to a no-op. |

### `fetch()` rules

`fetch()` is the heart of the adapter. Keep these conventions — the sink and CLI depend on them:

1. **One `Table` per call.** Return exactly one `Table` object. No batching, no generator.
2. **PyArrow-backed rows.** `Table.rows` must be a `pyarrow.Table`. Column **names, types, and
   nullability come from the source** — your adapter is responsible for that. The sink consumes
   only this object, so get the schema right.
3. **Honor `limit` per table.** Apply `limit` as SQL inside the query (not just as a total cap) so
   *every* table is capped, not only the aggregate. The default `row_limit` is `10,000`.
4. **Populate the namespace.** Set `Table(database=, schema=, table=)` to match the source
   `database.schema.table` — the sink and dbt use this namespace to locate the data.
5. **No Python-object round-trip.** Stream the whole capped result as a single Arrow batch.
6. **Read from a structured `Identifier`, not a raw query string.** The CLI calls
   `fetch(identifier, columns, limit)` where `identifier` is a `Identifier` (database/schema/table),
   **not** a SQL string. Build the read query with `build_fetch_query(identifier)` — never
   `identifier.split(".")`. Override `_qualified_reference()` (or
   `build_fetch_query()`) to shape the SQL for your dialect; the default already wraps names
   safely. See `fetch()` in the reference adapter below.

### `build_layout()` rules

`build_layout()` decides where each fetched table lands on disk. Follow the existing conventions:

- The configured `config.duckdb.database_path` folder path to save databases - defaults to current directory if omitted.
- Every new **source database (namespace)** each becomes its own sibling `.duckdb` in that directory — one `CatalogDatabase(name, path, schemas={})` per namespace.
- The **primary is always the in-memory hub** `:memory:` — you never set it to a `.duckdb` file. The sink `ATTACH`es each sibling by `name`. So your `build_layout()`
  should return `CatalogLayout(primary=":memory:", databases=[...])`, where each database `name` is the
  namespace dbt will `ATTACH` (usually the source `database`).
- Mirror the warehouse's real namespace. Snowflake → one `.duckdb` per database; BigQuery → one per
  project; Postgres → one `.duckdb` for `_default` (adjust as fits your dialect).

## What you actually edit

To add an adapter you touch **two files**:

### 1. Create `warehouse_to_go/warehouse/<name>_adapter.py`

```python
"""<Warehouse> adapter implementing the SourceAdapter protocol."""
from __future__ import annotations

import <driver>

from warehouse_to_go.utils.config import Config
from warehouse_to_go.warehouse import (
    CatalogDatabase,
    CatalogLayout,
    Identifier,
    SourceAdapter,
    Table,
    register,
)


@register("<type>")          # <-- matches warehouse.type in your dbt profile
class <Name>Adapter(SourceAdapter):
    """Fetches source tables from <Warehouse> as typed ``Table`` payloads."""

    type_name = "<type>"      # <-- must equal the registry key

    def __init__(self, config: Config) -> None:
        self.config = config
        self.conn = None

    # -- connection ------------------------------------------------------- #
    def _conn_params(self) -> dict:
        ...                    # build <driver>.connector.connect() params from config.warehouse

    def connect(self, config: Config) -> None:
        self.conn = <driver>.connector.connect(**self._conn_params())

    def test_connection(self, config: Config) -> None:
        <driver>.connector.connect(**self._conn_params()).close()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def _conn(self):
        # Lazy-connect: open the connection on first use if `connect()` hasn't
        # been called yet, so a bare `fetch()` works without an explicit connect.
        conn = self.conn
        if conn is None:
            self.connect(self.config)
            conn = self.conn
        return conn

    # -- protocol --------------------------------------------------------- #
    # The CLI passes a structured `Identifier`, not a query string. The base
    # class builds the read query with `build_fetch_query(identifier)`; override
    # `_qualified_reference()` when your dialect's names need a different
    # resolution strategy. Never call `identifier.split(".")` yourself.

    def fetch(self, identifier: Identifier, columns, limit):
        conn = self._conn()
        cursor = conn.cursor()
        try:
            query = self.build_fetch_query(identifier)
            limit_clause = f" LIMIT {limit}" if limit is not None else ""
            cursor.execute(query + limit_clause)
            arrow = cursor.fetch_arrow_all(force_return_table=True)
            return Table(
                database=identifier.database,
                schema=identifier.schema,
                table=identifier.table,
                rows=arrow,
            )
        finally:
            cursor.close()

    # -- catalog layout --------------------------------------------------- #
    def build_layout(self, config: Config, plan) -> CatalogLayout:
        # primary is always the in-memory hub ":memory:"; the sibling .duckdb files live under database_path.
        ...
        return CatalogLayout(primary=":memory:", databases=list(databases.values()))


def test_connection(config: Config) -> None:
    """Module-level convenience kept for the `debug` CLI command."""
    <Name>Adapter(config).test_connection(config)


@register("<type>")
def _<name>_factory(config: Config) -> SourceAdapter:
    return <Name>Adapter(config)
```

Copy the shape of `snowflake_adapter.py`: it's deliberately small. The adapter only **reads**.

### 2. Register it as a package entry point in `pyproject.toml`

Add one line under the `warehouse_to_go.adapters` entry-point group. This is the
only place outside your adapter module that changes, and it names the *module*
only — discovery imports the module and its `@register` decorator self-registers
the factory. **Do not edit `warehouse/__init__.py`** — that file now performs
entry-point discovery and must stay adapter-agnostic:

```toml
[project.entry-points."warehouse_to_go.adapters"]
snowflake = "warehouse_to_go.warehouse.snowflake_adapter"
<name> = "warehouse_to_go.warehouse.<name>_adapter"   # <-- add your module here
```

That's it. After a reinstall, `warehouse` imports your module on startup, your
`@register("<type>")` runs, `config` validates your `type` against the registry
(fails fast with a clear message if unknown), and the CLI dispatches via
`get_adapter_factory(config.warehouse.type)`. **Nothing else needs changing —
adding an adapter is a single new file, plus a one-line entry point.**

> Note: entry points are read from installed package metadata, so after adding an
> entry point reinstall the package (`uv pip install -e .`) for discovery to pick
> it up in the current environment.

## Configuration for your warehouse

Registration is only half the work — users also need credentials:

1. **dbt profile.** Add an entry to `~/.dbt/profiles.yml` whose target declares
   `type: <your type>`. `WarehouseConfig.from_dbt_profile()` reads this file and strips
   dbt-specific keys (`type`, `outputs`, `target`) before handing you a clean `WarehouseConfig`.
2. **Auth.** The profile output must include an auth method the adapter knows how to use
   (e.g. `password`, `private_key_path`). See the reference adapter for the auth pattern.
3. **`config.yml` / CLI.** Users select it with `type: <your type>` in `config.yml`, or the CLI
   picks it automatically once it's registered. No docs or flags needed.

## Writing a test

Keep the machinery locked. Add a small test in `tests/` (the suite currently uses
`tests/test_registry.py` and a `FakeAdapter` in that file as the canonical shape). At minimum,
cover: registration/dispatch by `type`, `build_fetch_query`/`_qualified_reference`, and `build_layout` producing the expected
sibling databases. Run:

```bash
uv run pytest
```

## Quick checklist

- [ ] `warehouse_to_go/warehouse/<name>_adapter.py` implements all six methods.
- [ ] `@register("<type>")` decorator present; `type_name` matches the registry key.
- [ ] Adapter module self-registers via `@register("<type>")` with matching `type_name`..
- [ ] `fetch()` returns one `Table` per call with a `pyarrow.Table` (columns/types/nullability correct) and honors `limit` per table.
- [ ] `build_layout()` returns one sibling `.duckdb` per source database namespace, matching dbt's `attach` names.
- [ ] A dbt profile entry with `type: <your type>` + an auth method exists.
- [ ] A test covers registration, `build_fetch_query`/`_qualified_reference`, and `build_layout`.
- [ ] Added your warehouse to the status table in [../README.md](../README.md#-adapters).

## Reference: the `SourceAdapter` protocol

The authoritative definition lives in
[`warehouse_to_go/warehouse/__init__.py`](../warehouse_to_go/warehouse/__init__.py)
(the `SourceAdapter` ABC and the `register`/registry helpers). If the protocol ever changes,
keep this doc in lockstep — the sink and CLI are the last things a contributor should need to
touch.
