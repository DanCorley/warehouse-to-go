# Run the Neon sample Postgres schemas and then
# exercise the warehouse-to-go Postgres adapter end to end.
#
# On first boot, 01-load-sample-dbs.sh downloads and loads six Neon
# sample schemas into the `postgres` database (periodic_table,
# world_happiness, titanic, netflix, dvdrental, chinook).

# 1. Start Postgres with the seed script bound in (first-boot only).
docker compose up -d

# 2. Point warehouse-to-go at your dbt profile + a Postgres `type`, then extract.
#    (Add a `postgres_sample` entry to ~/.dbt/profiles.yml, OR a config.yml:)
#
#    warehouse:
#      profile_name: postgres_sample
#      target: duck
#    extract:
#      row_limit: 1000          # keep the local mirror tiny / fast
#
uv sync --extra dev
warehouse-to-go -m dbt/postgres_sample/target/manifest.json analyze
warehouse-to-go -m dbt/postgres_sample/target/manifest.json extract

# 3. Query the local DuckDB mirror (dbt-polyglot transpiles Postgres SQL on the fly).
dbt build --project-dir dbt/postgres_sample --target duck

# Tear down:
# docker compose down -v

# ── Available schemas (inside `postgres` database) ───────────────────
# periodic_table      – elements of the periodic table (118 rows)
# world_happiness     – world happiness index (156 rows)
# titanic             – titanic passenger data (1309 rows)
# netflix             – movies & TV shows (8807 rows)
# dvdrental           – dvd rental tutorial (15861 rows)
# chinook             – digital media store (77929 rows)
#
# Source: https://github.com/neondatabase/postgres-sample-dbs
