"""Snowflake adapter implementing the :class:`SourceAdapter` protocol.

Migrated from ``extractor/snowflake_extractor.py``. The adapter is responsible
only for *reading* from Snowflake (connection, session setup, Arrow fetch).
Writing into the DuckDB catalog is the sink's job (`sink.load`), so this file
stays small; the adapter pulls each capped table as a single pyarrow.Table.
"""
from __future__ import annotations

import re
from pathlib import Path

import snowflake.connector

from warehouse_to_go.utils.config import Config
from warehouse_to_go.warehouse import (
    CatalogDatabase,
    CatalogLayout,
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

    def connect(self, config: Config) -> None:
        self.conn = snowflake.connector.connect(**self._conn_params())

    def test_connection(self, config: Config) -> None:
        snowflake.connector.connect(**self._conn_params()).close()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    # -- protocol --------------------------------------------------------- #
    def quote_ident(self, reference: str) -> str:
        # Snowflake unquotes double-quoted identifiers, so each part is safe.
        return f'"{reference.replace(chr(34), chr(34) * 2)}"'

    def _relation_from_query(self, query: str) -> str:
        match = re.search(r"FROM\s+identifier\('(.+?)'\)", query, re.IGNORECASE)
        if not match:
            raise ValueError("SnowflakeAdapter: query must reference identifier('db.schema.table')")
        return match.group(1)

    def fetch(self, query, columns, limit):
        # Snowflake-idiomatic table reference; the adapter derives the
        # database.schema.table namespace from it. `limit` caps rows per table
        # (the row_limit), applied as a SQL LIMIT so every table is capped, not
        # just the total. The whole capped result is pulled as one pyarrow.Table
        # and returned as a single Table — one load per table, no batching.
        relation = self._relation_from_query(query)
        database, schema, table = relation.split(".")

        conn = self.conn
        if conn is None:
            self.connect(self.config)
            conn = self.conn
        cursor = conn.cursor()
        try:
            if limit is not None:
                query = f"{query} LIMIT {limit}"
            cursor.execute(query)
            arrow = cursor.fetch_arrow_all(force_return_table=True)
            return Table(database=database, schema=schema, table=table, rows=arrow)
        finally:
            cursor.close()

    # -- catalog layout --------------------------------------------------- #
    def build_layout(self, config: Config, plan) -> CatalogLayout:
        db_dir = Path(config.duckdb.database_path).parent
        databases: dict = {}
        for key, table_list in plan.items():
            database, schema = key.split(".")
            db = databases.setdefault(
                database,
                CatalogDatabase(
                    name=database,
                    path=db_dir / f"{database}.duckdb",
                    schemas={schema: set()},
                ),
            )
            for t in table_list:
                db.schemas.setdefault(schema, set()).add(t["table_name"])
        return CatalogLayout(
            primary=config.duckdb.database_path,
            databases=list(databases.values()),
        )


def test_connection(config: Config) -> None:
    """Module-level convenience kept for the `debug` CLI command."""
    SnowflakeAdapter(config).test_connection(config)


@register("snowflake")
def _snowflake_factory(config: Config) -> SourceAdapter:
    return SnowflakeAdapter(config)
