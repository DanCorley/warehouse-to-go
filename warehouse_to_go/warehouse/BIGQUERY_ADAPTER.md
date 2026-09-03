# BigQuery adapter — how it works

Proves the **warehouse-to-go BigQuery adapter** is wired into the pipeline and
locks its adapter-agnostic behaviour (registry dispatch, the `bq.<dataset>.<table>`
query, one-sibling-per-project layout) with offline unit tests — no live BigQuery
needed.

```
BigQuery (project.dataset.table)
  ──extract──▶  local DuckDB mirror  ──dbt──▶  models
     (native)    one .duckdb per project          (polyglot)
```

## How the adapter works

The BigQuery adapter is a **pure-DuckDB** adapter — no separate driver. It opens
its own DuckDB connection and `ATTACH`es BigQuery as a read-only database using
DuckDB's **`bigquery` scanner** extension (a DuckDB *community* extension):

```python
INSTALL bigquery FROM community;   # requires network on FIRST install only
LOAD bigquery;
ATTACH 'project=<project-id>[&dataset=<ds>[&location=<loc>][&credentials_path=<key>]]'
        AS bq (TYPE bigquery, READ_ONLY);
SELECT * FROM bq.<dataset>.<table> LIMIT <row_limit>;
```

Three design points, learned and locked in:

1. **Native extension, not a driver.** The scanner returns a faithful
   `pyarrow.Table` and fetching is a native `cursor.fetch_arrow_table()` — no
   manual Arrow construction, no `snowflake`-style connector.

2. **Query references `bq.<dataset>.<table>`, the project comes from the URI.**
   The GCP `project` is attached via the `ATTACH` URI (`project=...`), so the
   adapter overrides `_qualified_reference` to emit `bq."<dataset>."<table>` and
   drops the `project` part (the base default would emit `project.dataset.table`,
   which the scanner can't resolve).

3. **Fresh DuckDB connection per `fetch()`.** The scanner can hold a lazy GCS
   credential handle, so — same as the Postgres adapter — we open a throwaway
   connection per table. `fetch()` returns a detached `pyarrow.Table`, so this
   is cheap and safe; the base `connect()` only warms `self.conn` for parity.

   > On **MotherDuck** the key/credentials are provisioned globally, so only
   > `project=` is needed (matches the MotherDuck blog example). On standard
   > DuckDB, pass a service-account JSON key via `credentials_path` (dbt's
   > `keyfile`).

## Namespaces

Mirrors dbt's `database.schema.table` (`project.dataset.table`):

| Warehouse field | BigQuery mapping | → DuckDB |
|---|---|---|
| `project.dataset.table` | project | one `.duckdb` per **project** |
| dataset | schema | folder inside that project's sibling |
| table | table | table |

`build_layout()` returns `CatalogLayout(primary=":memory:", databases=[...])`
with one `CatalogDatabase` per project (`<project>.duckdb`), each dataset a
schema inside it — one sibling per project, ATTACHed into the in-memory hub.

## Configuration

Registration is only half the work — users need credentials:

1. **dbt profile.** Add an entry to `~/.dbt/profiles.yml` whose target declares
   `type: bigquery`. `WarehouseConfig.from_dbt_profile()` reads it and strips
   dbt-only keys before handing the adapter a clean `WarehouseConfig`.
2. **Auth.** The output must include a project id (`project`) plus a service
   account JSON key (`credentials_path`, or dbt's `keyfile`) and optionally a
   `dataset` and `location`:
   ```yaml
   bigquery_wh:
   outputs:
     prod:
       type: bigquery
       project: my-gcp-project
       location: US
       dataset: analytics      # optional; attach-level default
       credentials_path: /path/to/service_account_key.json  # or dbt's `keyfile`
   target: prod
   ```
3. **`config.yml` / CLI.** Users select it with `type: bigquery`, or the CLI
   picks it automatically once registered. No docs or flags needed.

## Files

| Part | Where |
|---|---|
| **Adapter** | `warehouse_to_go/warehouse/bigquery_adapter.py` |
| **Entry point** | `pyproject.toml` → `[project.entry-points."warehouse_to_go.adapters"]` `bigquery = "..."` |
| **Tests** | `tests/test_bigquery_adapter.py` (offline; registry, query, layout) |

## Try it (manual)

```bash
# First run installs the community extension (needs network).
warehouse-to-go debug                       # loads + attaches; validates auth
uv run warehouse-to-go -p bigquery_wh \
    -m dbt/bq_sample/target/manifest.json analyze
uv run warehouse-to-go -p bigquery_wh \
    -m dbt/bq_sample/target/manifest.json extract
# -> produces <database_path>/<project>.duckdb  (one per project)

# Run dbt against the local DuckDB mirror (attach each project sibling, target duck).
cd dbt/bq_sample
dbt parse
dbt build --project-dir . --target duck
```

> **Test honesty:** all three adapters are **mocked at the adapter boundary** —
> the suite is green offline with canned/synthetic inputs, no live credentials.
> A real round-trip is gated behind a valid service-account key + network to
> install the extension at first use.
