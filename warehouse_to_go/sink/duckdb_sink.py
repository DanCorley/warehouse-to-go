"""The single DuckDB write path. Dialect-blind: it consumes a CatalogLayout plus
``Table`` objects (each carrying a ``pyarrow.Table``) and writes them into the
right database/schema.

Storage model: duckdb connects to a :memory: database then each source namespace is its own sibling ``.duckdb`` that
gets ATTACHed into the configured database_path folder directory. The adapter pulls each source table as Arrow; the sink turns each
one into a DuckDB table with a single
``CREATE OR REPLACE TABLE ... AS SELECT * FROM <arrow>`` — one load per table,
no batching, and no Python-object round-trip (the Arrow table is referenced
directly, so there is nothing to drop on failure).
"""
from __future__ import annotations

from pathlib import Path

from typing import Iterable, Optional

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


def _attach_databases(con: duckdb.DuckPyConnection, layout: CatalogLayout) -> None:
    """ATTACH each sibling database into the primary. Idempotent: safe to run
    repeatedly on the same connection, which is what lets one connection be held
    open across the whole extraction loop."""
    primary_path = str(layout.primary)
    for db in layout.databases:
        escaped_path = str(db.path).replace("'", "''")
        if str(db.path) != primary_path:
            con.execute(f"ATTACH IF NOT EXISTS DATABASE '{escaped_path}' AS {_q(db.name)}")


def _create_schemas(con: duckdb.DuckPyConnection, layout: CatalogLayout) -> None:
    """Create every schema/namespace up front. Idempotent (CREATE SCHEMA IF
    NOT EXISTS)."""
    for db in layout.databases:
        for schema in db.schemas:
            parts = [_q(db.name)]
            if schema:
                parts.append(_q(schema))
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(parts)}")


def setup(layout: CatalogLayout, connection: Optional[duckdb.DuckPyConnection] = None) -> duckdb.DuckPyConnection:
    """Attach the sibling databases and create their schemas. If no connection
    is supplied, open one here (the caller is responsible for closing it); pass
    an existing connection to reuse one DuckDB handle across many writes.

    With ``primary == ':memory:'`` no container file is created; the siblings on
    disk are the persistent stores.
    """
    primary_path = Path(layout.primary)
    if layout.primary != ":memory:":
        primary_path.parent.mkdir(parents=True, exist_ok=True)
    con = connection if connection is not None else duckdb.connect(str(primary_path))
    try:
        # The sibling files are real on-disk databases that get ATTACHed, so
        # their parent directories must exist *before* we open them. The
        # primary is ":memory:", so only the sibling paths need a directory.
        for db in layout.databases:
            db.path.parent.mkdir(parents=True, exist_ok=True)
        _attach_databases(con, layout)
        _create_schemas(con, layout)
    except Exception:
        if connection is None:
            con.close()
        raise
    return con


def load_table(con: duckdb.DuckPyConnection, layout: CatalogLayout, tbl: Table) -> int:
    """Write a single table into the primary connection. Assumes setup() ran."""
    db = layout.database(tbl.database)
    schema = tbl.schema or ""
    target = _ref(db.name, schema, tbl.table)
    with print_status(f"Writing {target}..."):
        src = tbl.rows
        con.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM src")
        return len(src)


def load(layout: CatalogLayout, tables: Iterable[Table] | Table, connection: Optional[duckdb.DuckPyConnection] = None) -> int:
    """Write one or more tables into the sink. Opens and closes its own
    connection unless ``connection`` is supplied (in which case the caller owns
    the handle and should pass the same one across repeated calls)."""
    if isinstance(tables, Table):
        tables = [tables]
    con = setup(layout, connection)
    try:
        total_rows = 0
        for tbl in tables:
            total_rows += load_table(con, layout, tbl)
        return total_rows
    finally:
        if connection is None:
            con.close()
