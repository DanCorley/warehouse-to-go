"""Lock the Postgres adapter's adapter-agnostic behaviour: registry dispatch,
the injection-safe query, and sibling-database layout. No live Postgres needed.
"""
import pytest
from pathlib import Path

from warehouse_to_go.warehouse import (
    CatalogLayout,
    Identifier,
    adapter_registry,
    get_adapter_factory,
)
from warehouse_to_go.utils.config import Config, DuckDBConfig, WarehouseConfig
from warehouse_to_go.warehouse.postgres_adapter import PostgresAdapter


@pytest.fixture(autouse=True)
def _entry_points(monkeypatch):
    # Postgres is discovered via the pyproject.toml entry point, so make sure
    # the warehouse package (and therefore discovery) has been imported.
    import warehouse_to_go.warehouse  # noqa: F401
    yield


def test_postgres_is_registered_by_type() -> None:
    assert "postgres" in adapter_registry()
    # The registry stores the @register'd factory; calling it yields the adapter.
    assert isinstance(get_adapter_factory("postgres")(Config(
        warehouse=WarehouseConfig(type="postgres"),
        duckdb=DuckDBConfig(database_path="."),
    )), PostgresAdapter)


def test_build_fetch_query_is_injection_safe() -> None:
    # Postgres is referenced as ``__pg."schema"."table"``: the database part is
    # dropped (DuckDB's scanner reads cross-schema natively), each part is quoted
    # so a manifest value can't escape into statement SQL, and `limit` is applied
    # by the caller as plain SQL, never interpolated as bare statement SQL.
    adapter = PostgresAdapter(Config(
        warehouse=WarehouseConfig(type="postgres"),
        duckdb=DuckDBConfig(database_path="."),
    ))
    q = adapter.build_fetch_query(Identifier("sales", "public", "customers"))
    assert q == 'SELECT * FROM __pg."public"."customers"'
    # the database part is ignored (DuckDB reads cross-schema natively)
    assert (
        adapter.build_fetch_query(Identifier("postgres", "sales", "customers"))
        == 'SELECT * FROM __pg."sales"."customers"'
    )


def test_build_layout_maps_one_sibling_per_database() -> None:
    adapter = PostgresAdapter(Config(
        warehouse=WarehouseConfig(type="postgres"),
        duckdb=DuckDBConfig(database_path="."),
    ))
    plan = {
        # One database (`postgres`) with two schemas, like the sample.
        "postgres.sales":  [{"table_name": "customers"}, {"table_name": "orders"}],
        "postgres.marketing": [{"table_name": "campaigns"}],
    }
    layout = adapter.build_layout(adapter.config, plan)

    # Primary is always the in-memory hub; one sibling .duckdb per database.
    assert layout.primary == ":memory:"
    assert {db.name for db in layout.databases} == {"postgres"}

    postgres = layout.database("postgres")
    assert postgres.path.name == "postgres.duckdb"
    assert postgres.schemas == {
        "sales": {"customers", "orders"},
        "marketing": {"campaigns"},
    }


def test_build_layout_default_path_uses_path_object() -> None:
    # `database_path` defaults to None -> must resolve to a Path, not a str, so
    # `Path / "name.duckdb"` doesn't raise.
    adapter = PostgresAdapter(Config(
        warehouse=WarehouseConfig(type="postgres"),
        duckdb=DuckDBConfig(database_path=None),
    ))
    layout = adapter.build_layout(adapter.config, {
        "postgres.sales": [{"table_name": "t"}],
    })
    assert layout.databases[0].path == Path("postgres.duckdb")
