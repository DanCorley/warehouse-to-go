from __future__ import annotations

from pathlib import Path

import pyarrow as pa

import duckdb

from warehouse_to_go.warehouse import CatalogDatabase, CatalogLayout, Identifier, Table
from warehouse_to_go.sink import load


def _events_arrow() -> pa.Table:
    return pa.table(
        {
            "name": pa.array(["acme", "w3rsa", None, "zed"], type=pa.string()),
            "id": pa.array([1, 2, None, 4], type=pa.int64()),
            "score": pa.array([3.5, 4.5, None, 6.5], type=pa.float64()),
            "ok": pa.array([True, False, None, True], type=pa.bool_()),
        }
    )


class MockAdapter:
    """Returns canned Tables so the sink can be exercised without a warehouse."""

    def fetch(self, identifier, columns, limit):
        return Table(database="analytics", schema="raw", table="events", rows=_events_arrow())


def _make_layout(tmp_path: Path) -> CatalogLayout:
    primary = ":memory:"
    analytics = tmp_path / "analytics.duckdb"
    analytics.parent.mkdir(parents=True, exist_ok=True)
    return CatalogLayout(
        primary=primary,
        databases=[
            CatalogDatabase(name="analytics", path=analytics, schemas={"raw": set()}),
        ],
    )


def _connect_with_attached(layout: CatalogLayout) -> duckdb.DuckDBPyConnection:
    """Open a *fresh* connection with sibling databases attached, the way a
    downstream tool (dbt) reaches the mirror. Attaches are per-connection, so
    this mirrors real consumption after the sink has closed."""
    conn = duckdb.connect(str(layout.primary))
    try:
        for db in layout.databases:
            if str(db.path) != str(layout.primary):
                conn.execute(f"ATTACH IF NOT EXISTS DATABASE '{db.path}' AS {db.name}")
    except Exception:
        conn.close()
        raise
    return conn


def _all_rows(conn, table: str) -> list[tuple]:
    return set(conn.execute(f"SELECT * FROM {table}").fetchall())


def test_sink_writes_typed_table_and_rows(tmp_path: Path) -> None:
    layout = _make_layout(tmp_path)
    n = load(
        layout,
        MockAdapter().fetch(Identifier("analytics", "raw", "events"), ["id", "name", "score", "ok"], 100),
    )

    assert n == 4

    conn = _connect_with_attached(layout)
    try:
        info = conn.execute("DESCRIBE analytics.raw.events").fetchall()
        types = {row[1] for row in info}
        # Arrow infers BIGINT (not INTEGER) for int64 — a faithful int64->BIGINT map.
        assert {"VARCHAR", "BIGINT", "DOUBLE", "BOOLEAN"}.issubset(types)

        # nulls are preserved: every source column has its NULL at row 2, so row 2
        # round-trips as all-null; row 3 stays ("zed", 4, 6.5, True).
        assert _all_rows(conn, "analytics.raw.events") == {
            ("acme", 1, 3.5, True),
            ("w3rsa", 2, 4.5, False),
            (None, None, None, None),
            ("zed", 4, 6.5, True),
        }
    finally:
        conn.close()


def test_sink_writes_all_rows_in_one_load(tmp_path: Path) -> None:
    """The whole table is pulled as one Arrow Table and written in a single load."""
    primary = ":memory:"
    db_file = tmp_path / "analytics.duckdb"
    db_file.parent.mkdir(parents=True, exist_ok=True)
    layout = CatalogLayout(
        primary=primary,
        databases=[
            CatalogDatabase(name="analytics", path=db_file, schemas={"raw": {"events"}}),
        ],
    )

    rows = pa.table(
        {
            "id": pa.array(list(range(50)), type=pa.int64()),
            "name": pa.array([f"row{i}" for i in range(50)], type=pa.string()),
        }
    )
    load(
        layout,
        Table(database="analytics", schema="raw", table="events", rows=rows),
    )

    conn = _connect_with_attached(layout)
    try:
        count = conn.execute("SELECT COUNT(*) FROM analytics.raw.events").fetchone()
        assert count[0] == 50
        types = {row[1] for row in conn.execute("DESCRIBE analytics.raw.events").fetchall()}
        assert {"BIGINT", "VARCHAR"}.issubset(types)
    finally:
        conn.close()


def test_sink_handles_empty_rows(tmp_path: Path) -> None:
    primary = ":memory:"
    db_file = tmp_path / "raw.duckdb"
    db_file.parent.mkdir(parents=True, exist_ok=True)
    layout = CatalogLayout(
        primary=primary,
        databases=[CatalogDatabase(name="raw", path=db_file, schemas={"d": {"t"}})],
    )

    empty = Table(
        database="raw",
        schema="d",
        table="t",
        rows=pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())}),
    )
    n = load(layout, empty)

    assert n == 0
    conn = _connect_with_attached(layout)
    try:
        rows = conn.execute("SELECT COUNT(*) FROM raw.d.t").fetchone()
        assert rows[0] == 0
    finally:
        conn.close()


def test_sink_reports_target_not_in_layout(tmp_path: Path) -> None:
    layout = _make_layout(tmp_path)
    from warehouse_to_go.warehouse import Table as T

    missing = T(
        database="raw",  # not in layout (layout only has 'analytics')
        schema="d",
        table="t",
        rows=pa.table({"a": pa.array([], type=pa.int64())}),
    )
    try:
        load(layout, missing)
    except KeyError as exc:
        assert "No database named 'raw'" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected KeyError for unknown database")


def test_sink_setup_exercises_directory_prefix(tmp_path: Path) -> None:
    """Regression for the `debug` command.

    ``config.duckdb.database_path`` is a *directory prefix* (default ``.``), not a
    database file. The old `debug` test did ``duckdb.connect(str(database_path))``
    which raised ``Is a directory``. The debug command now exercises the real sink
    setup path: a ``":memory:"`` primary ATTACHes a sibling ``.duckdb`` that lives
    *inside* the configured prefix directory. This test proves that path.
    """
    from warehouse_to_go.sink import setup

    prefix = tmp_path / "configured_db"
    prefix.mkdir(parents=True, exist_ok=True)  # mimics the configured directory prefix

    analytics = prefix / "analytics.duckdb"
    layout = CatalogLayout(
        primary=":memory:",
        databases=[
            CatalogDatabase(name="analytics", path=analytics, schemas={"raw": {"events"}}),
        ],
    )

    # setup() must create the sibling file inside the configured prefix directory,
    # never fail with "Is a directory", and persist data there.
    rows = pa.table({"id": pa.array([1, 2], type=pa.int64()), "v": pa.array(["a", "b"])})
    con = setup(layout)
    try:
        load(layout, Table(database="analytics", schema="raw", table="events", rows=rows), connection=con)
    finally:
        con.close()

    # The sibling database was created inside the configured prefix directory.
    assert analytics.exists()

    # A fresh connection reaching the sibling directly reads the persisted data.
    conn = duckdb.connect(str(analytics))
    try:
        count = conn.execute("SELECT COUNT(*) FROM raw.events").fetchone()[0]
        assert count == 2
    finally:
        conn.close()
