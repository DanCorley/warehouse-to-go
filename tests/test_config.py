from __future__ import annotations

from pathlib import Path
import textwrap
import yaml


def _write_profile(tmp_path, body: str, target: str = "snowflake_target") -> Path:
    (tmp_path / "profiles.yml").write_text(textwrap.dedent(body).lstrip())
    return tmp_path


def test_generic_config_preserves_provider_fields(tmp_path):
    """A non-Snowflake profile is not coerced into Snowflake fields. The raw
    dict keeps everything verbatim; only dialect-agnostic knobs are promoted to
    attributes."""

    from warehouse_to_go.utils.config import WarehouseConfig

    pg = WarehouseConfig(
        type="postgres",
        raw={
            "host": "db.example.com",
            "port": 5432,
            "dbname": "app",
            "user": "appuser",
            "password": "secret",
            "sslmode": "require",
        },
        threads=8,
    )
    assert pg.type == "postgres"
    # Snowflake-only attributes must not exist on the generic config.
    assert not hasattr(pg, "account")
    assert not hasattr(pg, "warehouse")
    assert sorted(pg.raw) == sorted(
        ["host", "port", "dbname", "user", "password", "sslmode"]
    )
    assert pg.threads == 8
    # as_dict() flattens the raw fields into the adapter view.
    d = pg.as_dict()
    assert d["host"] == "db.example.com" and d["sslmode"] == "require" and d["port"] == 5432


def test_from_dbt_profile_strips_dbt_keys_and_keeps_type(tmp_path):
    """End-to-end loading: type validates against the registry, dbt-only keys
    (`type`/`outputs`/`target`) are stripped, everything else lands in `raw`."""

    from warehouse_to_go.utils.config import WarehouseConfig

    body = """
myproj:
  outputs:
    prod:
      type: snowflake
      account: abc
      user: me
      warehouse: wh
      password: secret
  target: prod
"""
    cfg = _write_profile(tmp_path, body)
    cfg = WarehouseConfig.from_dbt_profile(
        profile_dir=cfg, profile_name="myproj", target="prod"
    )
    assert cfg.type == "snowflake"
    assert "type" not in cfg.raw and "outputs" not in cfg.raw and "target" not in cfg.raw
    assert cfg.raw["account"] == "abc"
    # Config no longer enforces a Snowflake auth mechanism — that is the adapter's
    # job, evaluated behind its factory's connection parsing.
    assert "password" in cfg.raw


def test_from_dbt_profile_rejects_unknown_type(tmp_path):
    """The registry is still the gate: an unregistered adapter fails fast with
    a clear message listing what *is* available."""

    from warehouse_to_go.utils.config import WarehouseConfig

    body = """
myproj:
  outputs:
    prod:
      type: bigquery
      project: p
      dataset: d
  target: prod
"""
    _write_profile(tmp_path, body)
    try:
        WarehouseConfig.from_dbt_profile(
            profile_dir=tmp_path, profile_name="myproj", target="prod"
        )
    except ValueError as e:
        assert "bigquery" in str(e)
        assert "snowflake" in str(e)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for unregistered adapter type")
