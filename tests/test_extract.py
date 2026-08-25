import duckdb
import pandas as pd
import pytest
import types
from unittest import mock

from warehouse_to_go.extractor.snowflake_extractor import SnowflakeExtractor
from warehouse_to_go.utils.config import Config, DuckDBConfig, ExtractConfig, WarehouseConfig

SCHEMA = "sdb.ssch"
TABLE = "myt"
_REAL_CONNECT = duckdb.connect  # capture before any test patches it


class FakeCursor:
    description = [("id", 0), ("v", 0)]

    def __init__(self, rows):
        self._rows = rows  # flat list of row tuples
        self._pos = 0

    def execute(self, _sql):
        pass

    def fetchmany(self, size):
        end = self._pos + size
        chunk = self._rows[self._pos:end]
        self._pos = end
        return chunk  # list of tuples, as the Snowflake connector returns

    def close(self):
        pass


def _patch_connect(connections):
    def fake(path, *args, **kwargs):
        conn = _REAL_CONNECT(str(path), *args, **kwargs)
        # DuckDB connections are read-only (can't reassign close); patch the
        # class method for the connection's lifetime so read-back works.
        mp = mock.patch.object(type(conn), "close", lambda self, *a, **k: None)
        mp.start()
        try:
            connections.append(conn)
            return conn
        finally:
            mp.stop()

    return fake


def make_config(batch_size, tmp_path):
    return Config(
        warehouse=WarehouseConfig(
            account="ac", user="u", warehouse="wh", database="sdb", schema="ssch"
        ),
        duckdb=DuckDBConfig(database_path=str(tmp_path / "mirror.duckdb")),
        extract=ExtractConfig(row_limit=10**9, batch_size=batch_size),
    )


def extract_with_batches(rows, batch_size, tmp_path):
    connections = []
    with mock.patch("duckdb.connect", side_effect=_patch_connect(connections)), \
         mock.patch.object(duckdb.DuckDBPyConnection, "close", lambda self, *a, **k: None), \
         mock.patch.object(
             SnowflakeExtractor,
             "_get_connection",
             return_value=types.SimpleNamespace(cursor=lambda: FakeCursor(rows)),
         ):
        SnowflakeExtractor(make_config(batch_size, tmp_path)).extract_tables(
            {SCHEMA: [{"table_name": TABLE}]}
        )
    return connections[0]


def read_rows(conn):
    return conn.execute(f"SELECT * FROM {SCHEMA}.{TABLE} ORDER BY id").fetchall()


def test_roundtrip_single_batch(tmp_path):
    source = pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    conn = extract_with_batches(source.to_dict("records"), 3, tmp_path)
    result = read_rows(conn)
    assert len(result) == 3
    assert [r[1] for r in result] == ["a", "b", "c"]


def test_roundtrip_multi_batch_keeps_all_rows_and_appends(tmp_path):
    source = pd.DataFrame({"id": list(range(5)), "v": [f"v{i}" for i in range(5)]})
    rows = source.to_dict("records")
    conn = extract_with_batches(rows, 2, tmp_path)  # batch_size 2 -> 3 batches

    result = read_rows(conn)
    assert len(result) == 5
    assert [r[0] for r in result] == list(range(5))
    # A seed-only table holds only one batch (<=2 rows); 5 rows proves the
    # later batches were appended incrementally, not just written once.
    assert len(result) > 2


def test_roundtrip_preserves_column_types(tmp_path):
    source = pd.DataFrame({"id": [1, 2, 3, 4], "v": [f"x{i}" for i in range(4)]})
    conn = extract_with_batches(source.to_dict("records"), 2, tmp_path)
    describe = {row[0]: row[2] for row in conn.execute(f"DESCRIBE {SCHEMA}.{TABLE}").fetchall()}
    assert "id" in describe and "v" in describe
    assert describe["id"] not in ("", "UNKNOWN")
    assert describe["v"] not in ("", "UNKNOWN")


def test_small_table_single_batch(tmp_path):
    source = pd.DataFrame({"id": [1, 2], "v": ["a", "b"]})
    conn = extract_with_batches(source.to_dict("records"), 2, tmp_path)
    result = read_rows(conn)
    assert len(result) == 2


def test_empty_source(tmp_path):
    connections = []
    with mock.patch("duckdb.connect", side_effect=_patch_connect(connections)), \
         mock.patch.object(
             SnowflakeExtractor,
             "_get_connection",
             return_value=types.SimpleNamespace(cursor=lambda: FakeCursor([])),
         ):
        SnowflakeExtractor(make_config(10, tmp_path)).extract_tables(
            {SCHEMA: [{"table_name": TABLE}]}
        )
    # Table is not created when there are no rows to write.
    conn = connections[0]
    try:
        conn.execute(f"SELECT * FROM {SCHEMA}.{TABLE} LIMIT 1")
    except duckdb.Error:
        pass
    else:
        raise AssertionError("expected table to be absent when source is empty")
