"""Lock the BigQuery adapter's adapter-agnostic behaviour: registry dispatch,
the ``bq.<dataset>.<table>`` query, and one-sibling-per-project layout. No live
BigQuery needed (the scanner extension is a runtime, network-installed DuckDB
community extension, exercised only behind credentials).
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
from warehouse_to_go.warehouse.bigquery_adapter import BigQueryAdapter


@pytest.fixture(autouse=True)
def _entry_points(monkeypatch):
    # The adapter is discovered via the pyproject.toml entry point, so make sure
    # the warehouse package (and therefore discovery) has been imported.
    import warehouse_to_go.warehouse  # noqa: F401
    yield


def _cfg(**overrides):
    raw = {"project": "my-gcp-project"}
    raw.update(overrides)
    warehouse = WarehouseConfig(type="bigquery", raw=raw)
    return Config(warehouse=warehouse, duckdb=DuckDBConfig(database_path="."))


def test_bigquery_is_registered_by_type() -> None:
    assert "bigquery" in adapter_registry()
    assert isinstance(get_adapter_factory("bigquery")(_cfg()), BigQueryAdapter)


def test_build_fetch_query_refs_bq_dataset_table() -> None:
    adapter = BigQueryAdapter(_cfg())
    # The GCP project is attached via the ATTACH URI, so the query references
    # only ``bq.<dataset>.<table>`` — the default ``database.schema.table``
    # (project.dataset.table) would not resolve for the scanner.
    q = adapter.build_fetch_query(Identifier("my-gcp-project", "analytics", "events"))
    assert q == 'SELECT * FROM bq."analytics"."events"'
    # A table with no dataset still resolves under the attached project.
    assert adapter.build_fetch_query(Identifier("proj", None, "t1")) == 'SELECT * FROM bq."t1"'


def test_build_fetch_query_is_injection_safe() -> None:
    adapter = BigQueryAdapter(_cfg())
    # Each part is quoted, so a manifest value can't escape into statement SQL.
    q = adapter.build_fetch_query(Identifier("p", 'bad;"', 'x'))
    assert q == 'SELECT * FROM bq."bad;""."x"'


def test_build_layout_maps_one_sibling_per_project() -> None:
    adapter = BigQueryAdapter(_cfg())
    plan = {
        "proj_a.analytics": [{"table_name": "events"}, {"table_name": "sessions"}],
        "proj_a.raw": [{"table_name": "clicks"}],
        "proj_b.marketing": [{"table_name": "campaigns"}],
    }
    layout = adapter.build_layout(adapter.config, plan)

    assert layout.primary == ":memory:"
    # One sibling .duckdb per *project*, not per dataset.
    assert {db.name for db in layout.databases} == {"proj_a", "proj_b"}

    proj_a = layout.database("proj_a")
    assert proj_a.path.name == "proj_a.duckdb"
    assert proj_a.schemas == {
        "analytics": {"events", "sessions"},
        "raw": {"clicks"},
    }
    assert layout.database("proj_b").path.name == "proj_b.duckdb"


def test_conn_params_require_project() -> None:
    # No project -> clear BigQuery-specific error, not a generic 500 downstream.
    with pytest.raises(ValueError, match="No BigQuery project id provided"):
        BigQueryAdapter(_cfg(project=None))._conn_params()


def test_conn_params_parses_project_dataset_location_credentials() -> None:
    adapter = BigQueryAdapter(_cfg(
        dataset="analytics", location="US", credentials_path="/tmp/key.json",
    ))
    params = adapter._conn_params()
    assert params == {
        "project": "my-gcp-project",
        "dataset": "analytics",
        "location": "US",
        "credentials_path": "/tmp/key.json",
    }


def test_attach_statement_builds_uri() -> None:
    adapter = BigQueryAdapter(_cfg(
        dataset="analytics", location="US", credentials_path="/tmp/key.json",
    ))
    stmt = adapter._attach_statement()
    assert stmt == (
        "ATTACH 'project=my-gcp-project"
        "&dataset=analytics&location=US&credentials_path=/tmp/key.json"
        "' AS bq (TYPE bigquery, READ_ONLY);"
    )


def test_attach_statement_escapes_credentials_path() -> None:
    # A stray quote in the key path must be escaped so it can't break out of the
    # ATTACH '...' literal.
    adapter = BigQueryAdapter(_cfg(credentials_path="/tmp/it's_key.json"))
    stmt = adapter._attach_statement()
    assert "credentials_path=/tmp/it''s_key.json" in stmt
