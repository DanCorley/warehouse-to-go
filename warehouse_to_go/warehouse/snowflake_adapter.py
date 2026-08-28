"""Snowflake adapter implementing the :class:`SourceAdapter` protocol.

Migrated from ``extractor/snowflake_extractor.py``. The adapter is responsible
only for *reading* from Snowflake (connection, session setup, Arrow fetch).
Writing into the DuckDB catalog is the sink's job (`sink.load`), so this file
stays small; the adapter pulls each capped table as a single pyarrow.Table.
"""
from __future__ import annotations

from pathlib import Path

import snowflake.connector

from warehouse_to_go.utils.config import Config
from warehouse_to_go.warehouse import (
    CatalogDatabase,
    CatalogLayout,
    Identifier,
    SourceAdapter,
    Table,
    register,
)


class SnowflakeAdapter(SourceAdapter):
    """Fetches source tables from Snowflake as typed ``Table`` payloads."""

    type_name = "snowflake"

    def __init__(self, config: Config) -> None:
        self.config = config
        self.conn = None

    # -- connection ------------------------------------------------------- #
    def _conn_params(self) -> dict:
        # Connection + auth are parsed from the raw profile here, behind the
        # selected factory, so the adapter owns Snowflake-specific fields and
        # fails fast with a Snowflake-specific message. Other warehouses provide
        # their own fields in the same `raw` dict (which we ignore).
        raw = self.config.warehouse.raw or {}
        params = {
            "account": raw.get("account"),
            "user": raw.get("user"),
            "warehouse": raw.get("warehouse"),
            "role": raw.get("role"),
            "database": raw.get("database"),
            "schema": raw.get("schema"),
            "client_session_keep_alive": raw.get("client_session_keep_alive", False),
            "query_tag": raw.get("query_tag"),
        }
        if raw.get("private_key_path"):
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend

            with open(raw["private_key_path"], "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=(
                        raw.get("private_key_passphrase").encode()  # type: ignore[union-attr]
                        if raw.get("private_key_passphrase")
                        else None
                    ),
                    backend=default_backend(),
                )
            params["private_key"] = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        elif raw.get("password"):
            params["password"] = raw["password"]
        else:
            raise ValueError(
                "No Snowflake authentication method provided. "
                "Expected 'password' or 'private_key_path' in the warehouse profile."
            )
        return params

    def connect(self, config: Config):
        self.conn = snowflake.connector.connect(**self._conn_params())
        return self.conn

    def test_connection(self, config: Config) -> None:
        snowflake.connector.connect(**self._conn_params()).close()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def _qualified_reference(self, identifier: Identifier) -> str:
        # Snowflake resolves a fully-qualified name *inside* IDENTIFIER(), so
        # the name is never emitted as bare statement SQL the way raw f-string
        # interpolation would be. This is the injection-safe direction to
        # prefer for warehouse names: a manifest name can't escape into the
        # statement body. Unquoted, so it supports simple/UPPERCASE and
        # fully-qualified names; names with spaces or mixed-case casing are
        # not expressible here (that is the tradeoff vs. per-part quoting).
        qualified = super()._qualified_reference(identifier)
        return f"IDENTIFIER('{qualified}')"

    # -- protocol --------------------------------------------------------- #
    def _conn(self):
        conn = self.conn
        if conn is None:
            self.connect(self.config)
            conn = self.conn
        return conn

    def fetch(self, identifier: Identifier, columns, limit):
        # `build_fetch_query` references the table via
        # ``IDENTIFIER('db.schema.table')``. Because the name sits inside the
        # function it is never emitted as bare statement SQL, so it is safe
        # against identifier injection. The capped result is pulled as one
        # pyarrow.Table and returned as a single Table — one load per table,
        # no batching. `limit` caps rows per table (the row_limit), applied as
        # a SQL LIMIT so every table is capped, not just the total.
        conn = self._conn()
        cursor = conn.cursor()
        try:
            limit_clause = f" LIMIT {limit}" if limit is not None else ""
            cursor.execute(self.build_fetch_query(identifier) + limit_clause)
            arrow = cursor.fetch_arrow_all(force_return_table=True)
            return Table(database=identifier.database, schema=identifier.schema, table=identifier.table, rows=arrow)
        finally:
            cursor.close()

    # -- catalog layout --------------------------------------------------- #
    def build_layout(self, config: Config, plan) -> CatalogLayout:
        # ``config.duckdb.database_path`` is an optional prefix for the on-disk
        # sibling files (None/empty -> current directory). The primary is an
        # in-memory hub (":memory:") that ATTACHes those siblings; the data
        # actually persists in the sibling files on disk.
        prefix = Path(config.duckdb.database_path or ".")
        databases: dict = {}
        for key, table_list in plan.items():
            database, schema = key.split(".")
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
    SnowflakeAdapter(config).test_connection(config)


@register("snowflake")
def _snowflake_factory(config: Config) -> SourceAdapter:
    return SnowflakeAdapter(config)
