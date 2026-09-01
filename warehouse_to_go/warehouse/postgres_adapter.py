"""Postgres adapter implementing the :class:`SourceAdapter` protocol.

The "quick win" README hint for Postgres is *DuckDB + transpile*. We go further:
instead of a separate driver, this adapter opens its own **DuckDB** connection
and ``ATTACH``es Postgres directly using DuckDB's bundled ``postgres_scanner``
extension::

    ATTACH 'postgresql://user:pass@host:port/dbname' AS __pg (TYPE postgres);

That means:

* **No new dependency** — everything runs on the same DuckDB the sink already uses.
* **DuckDB does the transpile** — ``SELECT * FROM __pg.sales.customers`` comes
  back as a faithful Arrow schema (int32, date32, …), and fetching is a native
  ``cursor.fetch_arrow_table()`` (no manual Arrow construction).
* **No cross-database SQL problem** — Postgres (PG14+) refuses ``db.schema.table``
  references from a single connection, but DuckDB reads them for us natively,
  so a warehouse with multiple databases/schemas extracts cleanly in one connection.

Namespaces: each source **database** becomes one sibling ``.duckdb`` under
``config.duckdb.database_path`` (e.g. ``postgres.duckdb``). ``schema`` (e.g.
``sales`` / ``marketing``) lives inside it, exactly like Snowflake.
"""
from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import duckdb

from warehouse_to_go.utils.config import Config
from warehouse_to_go.warehouse import (
    CatalogDatabase,
    CatalogLayout,
    Identifier,
    SourceAdapter,
    Table,
    register,
)


class PostgresAdapter(SourceAdapter):
    """Fetches source tables from Postgres as typed ``Table`` payloads."""

    type_name = "postgres"

    def __init__(self, config: Config) -> None:
        self.config = config
        self.conn = None

    # -- connection ------------------------------------------------------- #
    def _build_connstr(self) -> str:
        """Build a connection string safe for SQL string literals."""
        raw = self._conn_params()
        user = quote(raw["user"], safe="")
        host = quote(raw["host"], safe="")
        dbname = quote(raw["dbname"], safe="")
        port = int(raw["port"])
        sslmode = raw["sslmode"]
        if "password" in raw:
            password = quote(raw["password"], safe="")
            connstr = (
                f"postgresql://{user}:{password}@{host}:{port}"
                f"/{dbname}?sslmode={sslmode}"
            )
        else:
            passfile = quote(raw["passfile"], safe="")
            connstr = (
                f"postgresql://{user}@{host}:{port}"
                f"/{dbname}?sslmode={sslmode}&passfile={passfile}"
            )
        # Escape single quotes so the literal cannot break out of ATTACH '...'
        return connstr.replace("'", "''")

    def _conn_params(self) -> dict:
        # Connection + auth are parsed from the raw profile here, behind the
        # selected factory, so the adapter owns Postgres-specific fields and
        # fails fast with a Postgres-specific message.
        raw = self.config.warehouse.raw or {}
        missing = [k for k in ("user", "dbname") if not raw.get(k)]
        if missing or raw.get("password") is None and not raw.get("passfile"):
            raise ValueError(
                "No Postgres auth/endpoint provided. "
                "Expected 'user'/'dbname' and one of 'password' or 'passfile' "
                "(plus optional 'host'/'port'/'sslmode') in the warehouse profile."
            )
        params = {
            "host": raw.get("host", "localhost"),
            "port": raw.get("port", 5432),
            "user": raw.get("user"),
            "dbname": raw.get("dbname"),
            "sslmode": raw.get("sslmode", "prefer"),
        }
        if raw.get("password") is not None:
            params["password"] = raw["password"]
        else:
            params["passfile"] = raw["passfile"]
        return params

    def connect(self, config: Config):
        # Attach Postgres as a hidden database. The alias ``__pg`` is internal;
        # tables are addressed as ``__pg."schema"."table"``. A dedicated alias
        # (rather than the real namespace name) keeps the on-disk siblings clean.
        self.conn = duckdb.connect()
        self.conn.execute(
            f"ATTACH '{self._build_connstr()}' AS __pg (TYPE postgres);"
        )
        return self.conn

    def test_connection(self, config: Config) -> None:
        conn = duckdb.connect()
        try:
            conn.execute(
                f"ATTACH '{self._build_connstr()}' AS __pg (TYPE postgres);"
            )
            conn.execute("SELECT 1;")
        finally:
            conn.close()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def _pg_connection(self) -> duckdb.DuckPyConnection:
        # DuckDB's postgres_scanner closes the ATTACHed libpq connection after the
        # first query, so we open a fresh (cheap) connection per fetch. That's safe
        # here because `fetch()` returns a pyarrow.Table that's detached from the
        # DuckDB handle; nothing downstream holds the connection open. The base
        # `connect()` warms `self.conn` for parity with `test_connection()`.
        conn = duckdb.connect()
        conn.execute(
            f"ATTACH '{self._build_connstr()}' AS __pg (TYPE postgres);"
        )
        return conn

    # -- protocol --------------------------------------------------------- #
    # Postgres resolves ``schema.table`` against the *connected* database, and
    # (PG14+) refuses ``database.schema.table`` cross-database references from
    # one connection — but DuckDB reads them natively via the scanner. So drop
    # the database part and reference ``__pg."schema"."table"``. The sink still
    # writes to ``<database>.<schema>.<table>`` using the payload we return, so
    # the namespace dbt later reads stays faithful to the warehouse.
    def _qualified_reference(self, identifier: Identifier) -> str:
        table = identifier.table.replace('"', '""')
        if identifier.schema:
            schema = identifier.schema.replace('"', '""')
            return f'"{schema}"."{table}"'
        return f'"{table}"'

    def build_fetch_query(self, identifier: Identifier) -> str:
        return f"SELECT * FROM __pg.{self._qualified_reference(identifier)}"

    def fetch(self, identifier: Identifier, columns, limit):
        # `build_fetch_query` reads the table via ``__pg."schema"."table"``. The
        # capped result is pulled as one pyarrow.Table and returned as a single
        # Table — one load per table, no batching. `limit` caps rows per table
        # (row_limit), applied as SQL LIMIT so every table is capped, and is
        # never string-interpolated as bare statement SQL.
        conn = self._pg_connection()
        query = self.build_fetch_query(identifier)
        limit_clause = f" LIMIT {limit}" if limit is not None else ""
        cur = conn.execute(query + limit_clause)
        try:
            return Table(
                database=identifier.database,
                schema=identifier.schema,
                table=identifier.table,
                rows=cur.to_arrow_table(),
            )
        finally:
            cur.close()

    # -- catalog layout --------------------------------------------------- #
    def build_layout(self, config: Config, plan) -> CatalogLayout:
        # ``config.duckdb.database_path`` is an optional prefix for the on-disk
        # sibling files (None/empty -> current directory). The primary is an
        # in-memory hub (":memory:") that ATTACHes those siblings; the data
        # actually persists in the sibling files on disk. Each source *database*
        # becomes one sibling; schema is just a folder inside it.
        prefix = Path(config.duckdb.database_path or ".")
        databases: dict = {}
        for key, table_list in plan.items():
            database, schema = key.split(".", 1)
            db = databases.setdefault(
                database,
                CatalogDatabase(
                    name=database,
                    path=prefix / f"{database}.duckdb",
                    schemas={schema: set()},
                ),
            )
            for t in table_list:
                db.schemas.setdefault(schema, set()).add(t["table_name"])
        return CatalogLayout(primary=":memory:", databases=list(databases.values()))


def test_connection(config: Config) -> None:
    """Module-level convenience kept for the `debug` CLI command."""
    PostgresAdapter(config).test_connection(config)


@register("postgres")
def _postgres_factory(config: Config) -> SourceAdapter:
    return PostgresAdapter(config)
