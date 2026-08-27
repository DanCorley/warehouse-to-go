"""Domain models for the multi-adapter pipeline: adapters, sink contract, and
the warehouse-type registry. Intentionally free of any adapter implementation.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
import pyarrow
from dataclasses import dataclass, field
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

from warehouse_to_go.utils.config import Config

__all__ = [
    "SourceAdapter",
    "Table",
    "CatalogDatabase",
    "CatalogLayout",
    "register",
    "adapter_registry",
    "get_adapter_factory",
]


# --------------------------------------------------------------------------- #
# Table / typed payload
# --------------------------------------------------------------------------- #
@dataclass
class Table:
    """A fetched table, ready to write straight into DuckDB.

    `rows` is the fully-pulled, LIMIT-capped data for this table as a
    pyarrow.Table: column names, types, and nullability come from the source
    (the adapter's job). The sink turns each one into a DuckDB table with a
    single `CREATE OR REPLACE TABLE ... AS SELECT * FROM <arrow>` — one load
    per table, no batching, and no Python-object round-trip.
    """

    database: str                     # target namespace -> alias in the layout
    table: str                        # table name within its schema
    schema: Optional[str] = None      # target schema (None when the dialect has none)
    rows: "pyarrow.Table" = field(default_factory=lambda: pyarrow.table({}))

    def as_schema(self) -> str:
        """Schema key (schema + table) as used by the layout's schema map."""
        return f"{self.schema}.{self.table}" if self.schema else self.table


# --------------------------------------------------------------------------- #
# Sink contract: how a source namespace maps onto the DuckDB topology
# --------------------------------------------------------------------------- #
@dataclass
class CatalogDatabase:
    name: str                         # attach alias / namespace name
    path: Path                        # per-namespace .duckdb sibling file
    schemas: Dict[str, set] = field(default_factory=dict)

    def schema_exists(self, schema: str) -> bool:
        return schema in self.schemas


@dataclass
class CatalogLayout:
    """Where every fetched table lives in DuckDB."""

    primary: Path                     # container .duckdb file (config.duckdb.database_path)
    databases: List[CatalogDatabase] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.databases:
            raise ValueError("CatalogLayout must declare at least one database")

    def databases_by_name(self) -> Dict[str, CatalogDatabase]:
        return {db.name: db for db in self.databases}

    def database(self, name: str) -> CatalogDatabase:
        try:
            return self.databases_by_name()[name]
        except KeyError:
            raise KeyError(f"No database named {name!r} in layout")


# --------------------------------------------------------------------------- #
# Source adapter protocol
# --------------------------------------------------------------------------- #
class SourceAdapter(ABC):
    """Unified interface a warehouse implements. Instances are stateless
    between calls; the DatabaseConnectionManager owns connect/close."""

    type_name: str = ""

    @abstractmethod
    def connect(self, config: Config) -> Any:
        """Open the warehouse connection. Return the raw handle."""

    @abstractmethod
    def test_connection(self, config: Config) -> None:
        """Verify connectivity without fetching data."""

    @abstractmethod
    def fetch(self, query: str, columns: List[str], limit: Optional[int]) -> Table:
        """Pull up to `limit` rows from the query as a single pyarrow.Table (one
        Table per call), with column names, types, and nullability from the
        source."""

    @abstractmethod
    def close(self) -> None:
        """Release the warehouse connection."""

    @abstractmethod
    def quote_ident(self, reference: str) -> str:
        """Per-dialect quoting/escaping of an identifier reference."""

    @abstractmethod
    def build_layout(self, config: Config, plan: Dict[str, List[Dict]]) -> CatalogLayout:
        """Map the extraction plan onto a DuckDB catalog layout."""

    def use_context(self, **context: Any) -> None:
        """Optional session setup hook (warehouse/role/session). Default no-op."""


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
_REGISTRY: Dict[str, Callable[[Config], SourceAdapter]] = {}


def register(type_name: str) -> Callable[[Callable[[Config], SourceAdapter]], Callable[[Config], SourceAdapter]]:
    def decorator(factory: Callable[[Config], SourceAdapter]) -> Callable[[Config], SourceAdapter]:
        _REGISTRY[type_name] = factory
        return factory

    return decorator


def adapter_registry() -> Dict[str, Callable[[Config], SourceAdapter]]:
    return dict(_REGISTRY)


def get_adapter_factory(type_name: str) -> Callable[[Config], SourceAdapter]:
    try:
        return _REGISTRY[type_name]
    except KeyError:
        raise KeyError(f"No adapter registered for warehouse type: {type_name!r}")


def clear_registry() -> None:  # pragma: no cover - test helper
    _REGISTRY.clear()


# --------------------------------------------------------------------------- #
# Built-in adapters
# --------------------------------------------------------------------------- #
# Each adapter module self-registers via the `@register` decorator when imported.
# Import them here so `get_adapter_factory(warehouse.type)` works without the
# caller having to import each adapter by hand. Add a new entry here for each
# dialect you ship (e.g. "warehouse_to_go.warehouse.postgres_adapter").
__ALL_ADAPTERS__ = [
    "warehouse_to_go.warehouse.snowflake_adapter",
]

for _module in __ALL_ADAPTERS__:
    __import__(_module)
