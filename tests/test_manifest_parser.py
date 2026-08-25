from pathlib import Path

import json
from warehouse_to_go.extractor.manifest_parser import ManifestParser


def _write_manifest(tmp_path: Path, sources: dict) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(sources))
    return path


def test_parse_manifest_groups_tables_by_source(tmp_path: Path) -> None:
    manifest = {
        "sources": {
            "src_a.t1": {"source_name": "my_source", "database": "db1", "schema": "s1", "name": "t1"},
            "src_a.t2": {"source_name": "my_source", "database": "db1", "schema": "s1", "name": "t2"},
            "src_a.t3": {"source_name": "my_source", "database": "db1", "schema": "s1", "name": "t3"},
            "src_b.t4": {"source_name": "other", "database": "db2", "schema": "s2", "name": "t4"},
        }
    }
    path = _write_manifest(tmp_path, manifest)
    try:
        parser = ManifestParser(path)
        sources = parser.parse_manifest()
    finally:
        path.unlink()

    assert set(sources) == {"my_source", "other"}

    my = sources["my_source"]
    assert (my.database, my.schema) == ("db1", "s1")
    names = {t.name for t in my.tables}
    assert names == {"t1", "t2", "t3"}
    assert {t.identifier for t in my.tables} == names

    other = sources["other"]
    assert other.database == "db2"
    assert other.schema == "s2"
    assert len(other.tables) == 1
    assert other.tables[0].name == "t4"


def test_parse_manifest_missing_file_raises(tmp_path: Path) -> None:
    path = tmp_path / "does_not_exist.json"
    parser = ManifestParser(path)
    try:
        parser.parse_manifest()
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("expected FileNotFoundError")
