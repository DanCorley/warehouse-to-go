"""Dialect-blind DuckDB sink: attaches per-namespace databases and writes typed
tables into the right database/schema."""

from warehouse_to_go.sink.duckdb_sink import load, load_table, setup

__all__ = ["load", "load_table", "setup"]
