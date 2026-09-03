"""BigQuery adapter implementing the :class:`SourceAdapter` protocol.

warehouse-to-go reads BigQuery natively through DuckDB's **`bigquery`
scanner** — a DuckDB *community* extension::

    INSTALL bigquery FROM community;   # requires network on first install
    LOAD bigquery;

BigQuery's identity is ``project.dataset.table``. Unlike Snowflake/Postgres,
there is no driver to talk to directly, so we open our own **DuckDB**
connection and ``ATTACH`` BigQuery as a read-only database, addressing tables
as ``bq.<dataset>.<table>``:

    ATTACH 'project=<project-id>[&dataset=<ds>[&location=<loc>][&credentials_path=<key>]]'
            AS bq (TYPE bigquery, READ_ONLY);
    SELECT * FROM bq.<dataset>.<table> LIMIT <row_limit>;

Namespaces: **one sibling ``.duckdb`` per source *project*** (the GCP project
id), with each *dataset* living inside it as a schema — exactly mirroring
dbt's ``database.schema.table`` (``project.dataset.table``) namespace. The
primary is the usual in-memory hub (``:memory:``); the sink ``ATTACH``es the
siblings by project name.

Auth: a service-account JSON key (``credentials_path`` / dbt's ``keyfile``) is
passed to the scanner via the ``credentials_path`` ATTACH parameter. When
running on MotherDuck the key/credentials are provisioned globally and only
``project`` is needed, matching the MotherDuck blog example.
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


class BigQueryAdapter(SourceAdapter):
    """Fetches source tables from BigQuery as typed ``Table`` payloads."""

    type_name = "bigquery"

    def __init__(self, config: Config) -> None:
        self.config = config
        self.conn = None

    # -- connection ------------------------------------------------------- #
    def _conn_params(self) -> dict:
        # Connection params are parsed from the raw profile here, behind the
        # selected factory, so this adapter owns BigQuery-specific fields and
        # fails fast with a BigQuery-specific message. Other fields in ``raw``
        # (e.g. a Snowflake-shaped profile) are simply ignored.
        raw = self.config.warehouse.raw or {}
        project = raw.get("project")
        if not project:
            raise ValueError(
                "No BigQuery project id provided. Expected 'project' in the "
                "warehouse profile (plus optional 'dataset', 'location', and "
                "'credentials_path'/keyfile for service-account auth)."
            )
        params = {"project": project}
        if raw.get("dataset"):
            params["dataset"] = raw["dataset"]
        if raw.get("location"):
            params["location"] = raw["location"]
        # Accept both the dbt profile field name (`keyfile`) and the scanner's
        # native parameter name (`credentials_path`).
        keyfile = raw.get("credentials_path") or raw.get("keyfile")
        if keyfile:
            params["credentials_path"] = keyfile
        return params

    def _attach_statement(self) -> str:
        """Build the ``ATTACH ... AS bq ...`` statement for the configured
        project (plus any optional dataset/location/credentials)."""
        params = self._conn_params()
        query = "&".join(
            f"{k}={v.replace(chr(39), chr(39) * 2)}" for k, v in params.items()
        )
        return f"ATTACH '{query}' AS bq (TYPE bigquery, READ_ONLY);"

    def _ensure_extension(self, conn: duckdb.DuckPyConnection) -> None:
        """Make sure the ``bigquery`` scanner is loaded.

        If it is statically linked / already installed, ``LOAD bigquery``
        succeeds. Otherwise we try ``INSTALL bigquery FROM community`` which
        needs network access on first install; a failure raises a clear,
        actionable message instead of a cryptic DuckDB binder error.
        """
        try:
            conn.execute("LOAD bigquery;")
        except Exception:
            try:
                conn.execute("INSTALL bigquery FROM community;")
                conn.execute("LOAD bigquery;")
            except Exception as exc:  # pragma: no cover - network-gated
                raise RuntimeError(
                    "The BigQuery scanner extension is not available. Install it "
                    "first:\n"
                    "    INSTALL bigquery FROM community;\n"
                    "    LOAD bigquery;\n"
                    "(this requires network access on first use)."
                ) from exc

    def connect(self, config: Config):
        conn = duckdb.connect()
        self._ensure_extension(conn)
        conn.execute(self._attach_statement())
        self.conn = conn
        return conn

    def test_connection(self, config: Config) -> None:
        conn = duckdb.connect()
        try:
            self._ensure_extension(conn)
            conn.execute(self._attach_statement())
            conn.execute("SELECT 1;")
        finally:
            conn.close()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def _conn(self) -> duckdb.DuckPyConnection:
        # Lazy-connect: open on first use if ``connect()`` hasn't been called,
        # so a bare ``fetch()`` works without an explicit connect.
        conn = self.conn
        if conn is None:
            self.connect(self.config)
            conn = self.conn
        return conn

    def _read_conn(self) -> duckdb.DuckPyConnection:
        # The BigQuery scanner can keep a lazy GCS credential handle open, so we
        # open a fresh (cheap) DuckDB connection per fetch. ``fetch()`` returns a
        # detached pyarrow.Table, so nothing downstream needs the handle. The
        # base ``connect()`` warms ``self.conn`` for parity with test_connection.
        conn = duckdb.connect()
        self._ensure_extension(conn)
        conn.execute(self._attach_statement())
        return conn

    # -- protocol --------------------------------------------------------- #
    # BigQuery references tables as ``bq.<dataset>.<table>``; the GCP ``project``
    # is attached via the ATTACH URI (project=...), not the query. The base
    # default emits ``database.schema.table`` (= project.dataset.table), which
    # the scanner can't resolve, so we override to drop the project part.
    def _qualified_reference(self, identifier: Identifier) -> str:
        schema = identifier.schema or ""
        table = identifier.table.replace('"', '""')
        target = f'bq."{table}"' if not schema else f'bq."{schema}"."{table}"'
        return target

    def build_fetch_query(self, identifier: Identifier) -> str:
        return f"SELECT * FROM {self._qualified_reference(identifier)}"

    def fetch(self, identifier: Identifier, columns, limit):
        # `build_fetch_query` reads ``bq.<dataset>.<table>``. The capped result is
        # pulled as one pyarrow.Table and returned as a single Table — one load
        # per table, no batching. ``limit`` caps rows per table (row_limit),
        # applied as a SQL LIMIT so every table is capped. The project in the
        # ATTACH URI is read-only, so no session/``use_context`` is needed.
        conn = self._read_conn()
        query = self.build_fetch_query(identifier)
        limit_clause = f" LIMIT {limit}" if limit is not None else ""
        cur = conn.execute(query + limit_clause)
        try:
            return Table(
                database=identifier.database,
                schema=identifier.schema,
                table=identifier.table,
                rows=cur.fetch_arrow_table(),
            )
        finally:
            cur.close()

    # -- catalog layout --------------------------------------------------- #
    def build_layout(self, config: Config, plan) -> CatalogLayout:
        # ``config.duckdb.database_path`` is an optional prefix for the on-disk
        # siblings (None/empty -> current directory). The primary is the in-memory
        # hub (":memory:") that ATTACHes those siblings; data persists in the
        # sibling files on disk. Each source *project* becomes one sibling; each
        # *dataset* is a schema inside it — one .duckdb per project.
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
    """Module-level convenience kept for the ``debug`` CLI command."""
    BigQueryAdapter(config).test_connection(config)


@register("bigquery")
def _bigquery_factory(config: Config) -> SourceAdapter:
    return BigQueryAdapter(config)
