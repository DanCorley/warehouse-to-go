# Neon sample schemas (dbt)

End-to-end demo of the **warehouse-to-go Postgres adapter**: real Postgres →
extract → local DuckDB mirror → dbt builds against the mirror.

## Layout

```
dbt/postgres_sample/
├── dbt_project.yml              # profile='portable_warehouse', polyglot postgres->duckdb
├── profiles.yml                 # `dev` = Postgres source, `duck` = local mirror
├── models/sources.yml           # 6 Neon sample schemas (all in `postgres` database)
└── models/
    ├── periodic_table/          # 1 table
    ├── world_happiness/         # 1 table
    ├── titanic/                 # 1 table
    ├── netflix/                 # 1 table
    ├── dvdrental/               # 15 tables
    └── chinook/                 # 11 tables
```

All six sample datasets live as **schemas** inside the connected `postgres`
database.  warehouse-to-go extracts them into a single `postgres.duckdb`
sibling file; the `database.schema.table` namespace is preserved so dbt
reads `postgres.chinook."Album"`, etc.

## Prerequisites

- `dbt-polyglot` — installed into the project environment by `uv sync --extra dev`.
- Postgres running from `../../docker/postgres/` (see that README).

## Run it

```bash
cd dbt/postgres_sample

# 1. Install deps + sync the venv (from repo root)
uv sync --extra dev
dbt parse

# 2. Extract sources into the sibling .duckdb file (postgres adapter)
cd ../.. && uv run warehouse-to-go -p portable_warehouse -t postgres -m dbt/postgres_sample/target/manifest.json extract

# 3. Build + run models against the local DuckDB mirror
cd dbt/postgres_sample
dbt build --project-dir . --target duck
```

The `--target duck` output attaches `postgres.duckdb` (written by step 2)
and transpiles the Postgres model SQL to DuckDB on the fly — no model
rewrites.
