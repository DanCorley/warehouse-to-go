import pathlib

import pytest
import yaml

from warehouse_to_go.utils.config import WarehouseConfig


def _write_profiles(tmp_path: pathlib.Path, profiles: dict) -> None:
    (tmp_path / "profiles.yml").write_text(yaml.safe_dump(profiles))


def test_from_dbt_profile_rejects_non_snowflake(tmp_path: pathlib.Path) -> None:
    profiles = {
        "proj": {
            "target": "bigquery",
            "outputs": {
                "bq": {"type": "bigquery", "project": "p", "dataset": "d", "credentials": "{}"}
            },
        }
    }
    _write_profiles(tmp_path, profiles)
    with pytest.raises(ValueError, match="not a supported warehouse connection"):
        WarehouseConfig.from_dbt_profile(
            profile_dir=tmp_path, profile_name="proj", target="bq"
        )


def test_from_dbt_profile_accepts_snowflake(tmp_path: pathlib.Path) -> None:
    profiles = {
        "proj": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "snowflake",
                    "account": "ac",
                    "user": "u",
                    "warehouse": "wh",
                    "database": "db",
                    "schema": "sch",
                    "password": "secret",
                }
            },
        }
    }
    _write_profiles(tmp_path, profiles)
    wh = WarehouseConfig.from_dbt_profile(
        profile_dir=tmp_path, profile_name="proj", target="dev"
    )
    assert wh.account == "ac"
    assert wh.schema == "sch"
    assert wh.password == "secret"
    assert wh.private_key_path is None
