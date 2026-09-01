# Postgres adapter — end-to-end sample

Proves the **warehouse-to-go Postgres adapter** works end to end:

```
real Postgres  ──extract──▶  local DuckDB mirror  ──dbt──▶  models
  └ 6 Neon sample schemas      (postgres.duckdb)        (polyglot)
```

## How the adapter works

The Postgres adapter is a **pure-DuckDB** adapter — it opens its own DuckDB
connection and `ATTACH`es Postgres directly using DuckDB's bundled
`postgres_scanner` extension (no separate driver, no `pyarrow` construction):

```python
ATTACH 'postgresql://user:pass@host:port/dbname' AS __pg (TYPE postgres);
SELECT * FROM __pg."chinook"."Album" LIMIT <row_limit>;
```

Two design points were learned and locked in:

1. **No `_qualified_reference` / no cross-database SQL.** Postgres (PG14+) refuses
   `database.schema.table` references from a single connection, but DuckDB reads
   them natively. So the adapter references the table as a **2-part**
   `__pg."schema"."table"` (the source `database` part is dropped); the sink
   still writes to `<database>.<schema>.<table>` using the `Table` payload we
   return, so the namespace dbt later reads stays faithful.

2. **A fresh DuckDB connection per `fetch`.** DuckDB's `postgres_scanner` closes
   the ATTACHed libpq connection after the *first* query
   (`Connection already closed!`). Because `fetch()` returns a `pyarrow.Table`
   that's detached from DuckDB, opening a throwaway connection per table is
   cheap and safe — the base `connect()` only warms `self.conn` for parity.

Net effect: **no new dependency**. The adapter runs entirely on the DuckDB
the sink already uses — no psycopg, no pyarrow construction.

## How the sample works

| Part | Where |
|---|---|
| **Adapter** | `warehouse_to_go/warehouse/postgres_adapter.py` |
| **Docker Postgres** | `docker/postgres/` — `docker-compose.yml` + `01-load-sample-dbs.sh` (loads 6 Neon schemas) |
| **dbt project** | `dbt/postgres_sample/` — sources + models for all 6 schemas |
| **Tests** | `tests/test_postgres_adapter.py` |

## Try it (manual)

```bash
# 1. Seed Postgres with the 6 Neon sample schemas
cd docker/postgres && docker compose up -d

# 2. Extract (postgres adapter) -> one sibling .duckdb per database
uv run warehouse-to-go -p portable_warehouse -t postgres \
    -m dbt/postgres_sample/target/manifest.json extract
#    -> produces dbt/postgres_sample/postgres.duckdb

# 3. Run dbt against the local DuckDB mirror (transpiles Postgres SQL on the fly)
cd dbt/postgres_sample
dbt parse
dbt build --project-dir . --target duck
```

> The sample profile is `portable_warehouse` (in `dbt/postgres_sample/profiles.yml`),
> whose target `postgres` points at `127.0.0.1:5433`. Extract with
> `-p portable_warehouse -t postgres`; `analyze` works the same way.

## Available schemas (inside `postgres` database)

| Schema | Tables | Rows | Content |
|---|---|---|---|
| `periodic_table` | 1 | 118 | Elements of the periodic table |
| `world_happiness` | 1 | 156 | World happiness index |
| `titanic` | 1 | 1,309 | Titanic passenger data |
| `netflix` | 1 | 8,807 | Movies & TV shows |
| `dvdrental` | 15 | 15,861 | DVD rental tutorial |
| `chinook` | 11 | 77,929 | Digital media store |

Source: https://github.com/neondatabase/postgres-sample-dbs
