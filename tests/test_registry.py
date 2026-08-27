import pytest

from warehouse_to_go.warehouse import (
    clear_registry,
    get_adapter_factory,
    adapter_registry,
    register,
    SourceAdapter,
)


class FakeAdapter(SourceAdapter):
    type_name = "fake"

    def connect(self, config) -> None:
        pass

    def test_connection(self, config) -> None:
        pass

    def fetch(self, identifier, columns, limit):
        return iter([])

    def close(self) -> None:
        pass

    def quote_ident(self, reference) -> str:
        return f'"{reference}"'

    def build_layout(self, config, plan):
        from warehouse_to_go.warehouse import CatalogLayout

        return CatalogLayout(primary=":memory:", databases=[])


def test_registry_starts_empty() -> None:
    clear_registry()
    assert adapter_registry() == {}


def test_register_and_resolve() -> None:
    clear_registry()
    factory = register("fake")(FakeAdapter)

    assert "fake" in adapter_registry()
    assert get_adapter_factory("fake") is FakeAdapter


def test_unknown_type_raises() -> None:
    clear_registry()
    with pytest.raises(KeyError, match="No adapter registered for warehouse type: 'ghost'"):
        get_adapter_factory("ghost")
