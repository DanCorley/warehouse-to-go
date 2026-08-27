"""The single DuckDB write path. Dialect-blind: it consumes a CatalogLayout plus
``Table`` objects (each carrying a ``pyarrow.Table``) and writes them into the
right database/schema.

Storage model (see ROADMAP): the configured ``database_path`` is the primary
container ``.duckdb``; each source namespace is its own sibling ``.duckdb`` that
gets ATTACHed. The adapter pulls each source table as Arrow; the sink turns each
one into a DuckDB table with a single
``CREATE OR REPLACE TABLE ... AS SELECT * FROM <arrow>`` — one load per table,
no batching, and no Python-object round-trip (the Arrow table is referenced
directly, so there is nothing to drop on failure).
"""
from __future__ import annotations

from typing import Iterable

import duckdb

from warehouse_to_go.utils.output import print_status
from warehouse_to_go.warehouse import CatalogDatabase, CatalogLayout, Table

_IDENT_QUOTE = '"'


def _q(name: str) -> str:
    return f"{_IDENT_QUOTE}{name.replace(_IDENT_QUOTE, _IDENT_QUOTE * 2)}{_IDENT_QUOTE}"


def _ref(db: str, schema: str, table: str) -> str:
    parts = [_q(db)]
    if schema:
        parts.append(_q(schema))
    parts.append(_q(table))
    return ".".join(parts)


def load(layout: CatalogLayout, tables: Iterable[Table] | Table) -> int:
    if isinstance(tables, Table):
        tables = [tables]
    primary_path = str(layout.primary)
    con = duckdb.connect(primary_path)
    try:
        for db in layout.databases:
            if str(db.path) != primary_path:
                con.execute(f"ATTACH IF NOT EXISTS DATABASE '{db.path}' AS {_q(db.name)}")
            for schema in db.schemas:
                parts = [_q(db.name)]
                if schema:
                    parts.append(_q(schema))
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(parts)}")

        total_rows = 0
        for tbl in tables:
            db = layout.database(tbl.database)
            schema = tbl.schema or ""
            target = _ref(db.name, schema, tbl.table)
            with print_status(f"Writing {target}..."):
                src = tbl.rows
                con.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM src")
                total_rows += len(src)

        return total_rows
    finally:
        con.close()
