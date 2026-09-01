#!/usr/bin/env bash
#
# Downloads and loads Neon's postgres-sample-dbs as SCHEMAS inside the
# connected postgres database.  Runs once during first-boot initialisation
# (docker-entrypoint-initdb.d).
#
# Schemas created (inside the default postgres database):
#   periodic_table  (118 rows)  – elements of the periodic table
#   world_happiness (156 rows)  – world happiness index
#   titanic         (1309 rows) – titanic passenger data
#   netflix         (8807 rows) – movies & TV shows
#   dvdrental       (15861 rows)– dvd rental tutorial
#   chinook         (77929 rows)– digital media store
#
# The Neon dumps use `public.` schema-qualified names.  We sed-replace
# `public.` with the target schema so everything lands in the right place
# without cross-database references.
#
# Source: https://github.com/neondatabase/postgres-sample-dbs
# Licence: each dataset has its own licence (see repo README).

set -euo pipefail

REPO_BASE="https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main"
SAMPLE_DIR="/tmp/sample-dbs"

# schema name -> SQL filename
declare -A SCHEMAS=(
  [periodic_table]="periodic_table.sql"
  [world_happiness]="happiness_index.sql"
  [titanic]="titanic.sql"
  [netflix]="netflix.sql"
  [dvdrental]="dvdrental.sql"
  [chinook]="chinook.sql"
)

mkdir -p "$SAMPLE_DIR"

echo "--- Downloading sample databases from Neon ---"
for schema in "${!SCHEMAS[@]}"; do
  file="${SCHEMAS[$schema]}"
  dest="$SAMPLE_DIR/$file"
  if [ ! -f "$dest" ]; then
    echo "  Downloading $file ..."
    wget -q -O "$dest" "$REPO_BASE/$file"
  else
    echo "  $file already downloaded"
  fi
done

echo ""
echo "--- Creating schemas and loading sample data ---"
for schema in "${!SCHEMAS[@]}"; do
  file="${SCHEMAS[$schema]}"
  src="$SAMPLE_DIR/$file"

  # Skip if schema already exists (idempotent re-runs)
  if psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
       "SELECT 1 FROM information_schema.schemata WHERE schema_name = '$schema'" \
       | grep -q 1; then
    echo "  Schema '$schema' already exists — skipping"
    continue
  fi

  echo "  Creating schema '$schema' ..."
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE SCHEMA $schema;"

  # The Neon dumps are pg_dump files that use `public.` everywhere.
  # Replace `public.` with the target schema so tables land in the right place.
  modified="$SAMPLE_DIR/${schema}_modified.sql"
  sed "s/public\./${schema}./g" "$src" > "$modified"

  echo "  Loading $file into schema $schema ..."
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$modified" --quiet

  rm -f "$modified"
done

echo ""
echo "--- Schemas loaded ---"
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
  "SELECT schema_name FROM information_schema.schemata
   WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast')
   ORDER BY schema_name;"
