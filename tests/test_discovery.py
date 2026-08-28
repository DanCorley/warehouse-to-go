import importlib
import types
from pathlib import Path

import pytest

ENTRY_POINT_GROUP = "warehouse_to_go.adapters"


def _install_ep(monkeypatch, entry_point):
    """Swap ``importlib.metadata.entry_points`` so discovery sees ``entry_point``."""

    class _EntryPoint:
        def __init__(self, name, dotted):
            self.name = name
            self.value = dotted
            self.group = ENTRY_POINT_GROUP

        def load(self):
            return importlib.import_module(self.value)

    class _EntryPoints:
        def __init__(self, eps):
            self._eps = eps

        def __call__(self, group=None, *, name=None, **kwargs):
            return self._eps if group == ENTRY_POINT_GROUP else []

        def __iter__(self):
            return iter([])

    monkeypatch.setattr(importlib.metadata, "entry_points", _EntryPoints([entry_point]))


def test_new_adapter_discovered_from_entry_point_only(
    monkeypatch, tmp_path
):
    """Adding an adapter touches *only* its own module — no edit to
    ``warehouse/__init__.py`` is required. The entry point + the module's own
    ``@register`` decorator are enough for discovery."""

    from warehouse_to_go.warehouse import discover_adapters, adapter_registry, register, SourceAdapter

    module_source = """
from __future__ import annotations

from warehouse_to_go.utils.config import Config
from warehouse_to_go.warehouse import (
    CatalogLayout,
    SourceAdapter,
    Table,
    register,
)


@register("tmp_discovery_wh")


class _TmpAdapter(SourceAdapter):
    type_name = "tmp_discovery_wh"

    def connect(self, config: Config) -> None: ...
    def test_connection(self, config: Config) -> None: ...
    def fetch(self, identifier, columns, limit):
        return Table(database="d", schema="s", table="t")
    def close(self) -> None: ...
    def build_fetch_query(self, identifier: str) -> str:
        return f'"{identifier}"'
    def build_layout(self, config: Config, plan) -> CatalogLayout:
        return CatalogLayout(primary=":memory:", databases=[])
"""

    module_path = tmp_path / "tmp_discovery_adapter.py"
    # ``tmp_discovery_adapter`` can't be dotted-imported from a temp dir, so place
    # the file inside the package so the entry-point dotted path resolves.
    package_dir = Path("warehouse_to_go/warehouse")
    real_path = package_dir / "_tmp_discovery_adapter.py"
    real_path.write_text(module_source)
    try:
        dotted = "warehouse_to_go.warehouse._tmp_discovery_adapter"
        ep = types.SimpleNamespace(name="tmp_discovery_wh", value=dotted)

        class _EP:
            def __init__(self, name, dotted):
                self.name = name
                self.value = dotted

            def load(self):
                return importlib.import_module(self.value)

        ep = _EP("tmp_discovery_wh", dotted)
        _install_ep(monkeypatch, ep)

        discovered = discover_adapters()
        assert "tmp_discovery_wh" in adapter_registry()
        assert discovered["tmp_discovery_wh"].__name__ == dotted
        # Discovering again re-imports; the factory must still resolve.
        assert discovered["tmp_discovery_wh"]
    finally:
        real_path.unlink(missing_ok=True)


def test_discovery_requires_self_registration(monkeypatch):
    """An entry point whose module forgets ``@register`` is a typo, not silent."""

    import warehouse_to_go.warehouse as wh

    class _Orphan:
        name = "orphan_wh"
        value = "not_registered"

        def load(self):
            return types.SimpleNamespace(__name__="not_registered")

    _install_ep(monkeypatch, _Orphan())
    with pytest.raises(RuntimeError, match="did not self-register"):
        wh.discover_adapters()
