"""Domain models for the multi-adapter pipeline: adapters, sink contract, and
the warehouse-type registry. Intentionally free of any adapter implementation.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
import importlib.metadata
import pyarrow
from dataclasses import dataclass, field
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

from warehouse_to_go.utils.config import Config

__all__ = [
    "SourceAdapter",
    "Table",
    "Identifier",
    "CatalogDatabase",
    "CatalogLayout",
    "register",
    "adapter_registry",
    "get_adapter_factory",
    "discover_adapters",
]


# --------------------------------------------------------------------------- #
# Table / typed payload
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Identifier:
    """Dialect-agnostic database/schema/table identity for a table to read.

    This is the *structured* identity passed to :meth:`SourceAdapter.fetch`.
    The adapter turns it into the dialect-specific SQL it can actually
    execute (``db.schema.table``, ``identifier('db.schema.table')``,
    fully-qualified names, ...). The CLI never formats this into SQL, so a
    newly registered adapter always receives an identity it understands instead
    of a Snowflake-flavoured query string it cannot run.
    """

    database: str
    schema: str
    table: str

    def qualified(self) -> str:
        """Dot-joined ``database.schema.table`` for logging/labels."""
        return f"{self.database}.{self.schema}.{self.table}"


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
    """Where fetched tables land in DuckDB.

    ``primary`` is the primary container path **or** the string ``":memory:"``.
    Using ``":memory:"`` means the primary is an in-memory hub: the sink
    ``ATTACH``es the on-disk sibling databases into it and writes the tables
    there. Because DuckDB ATTACH shares storage, the writes land in the sibling
    files on disk — they persist even after the hub connection is closed.
    Downstream tools (dbt) then reach the data by attaching the sibling files
    directly rather than connecting to the (now optional) container.
    """

    primary: Optional[str] = ":memory:"
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
    def fetch(self, identifier: Identifier, columns: List[str], limit: Optional[int]) -> Table:
        """Read the table named by `identifier` (structured
        database/schema/table identity) and return up to `limit` rows as a
        single pyarrow.Table (one Table per call), with column names, types,
        and nullability from the source.

        The dialect-specific query is built by :meth:`build_fetch_query` from
        the structured identity — the CLI never formats SQL itself, so a newly
        registered adapter can no longer be handed a query it cannot execute.
        """

    def build_fetch_query(self, identifier: Identifier) -> str:
        """Dialect-specific query that reads `identifier` (database.schema.table).

        This is the only place a warehouse is allowed to format SQL, so every
        dialect gets a query it can actually execute. The default produces a
        plain ``database.schema.table`` reference. Adapters must override this when
        their dialect uses a different namespace shape or identifier syntax (for
        example, PostgreSQL connections cannot query another database via a
        three-part reference, while BigQuery uses ``project.dataset.table``).
        """
        return (
            f"SELECT * FROM {identifier.database}.{identifier.schema}.{identifier.table}"
        )

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
# Adapter discovery (PEP 682 entry points)
# --------------------------------------------------------------------------- #
# Discovery replaces the old hard-coded `__ALL_ADAPTERS__` list. There is no
# central module list to edit when adding a dialect: a new adapter module only
# needs to implement `SourceAdapter` and self-register with `@register("<type>")`.
# The list of adapter *modules* to import lives in `pyproject.toml` under the
# `warehouse_to_go.adapters` entry-point group. On import we resolve those entry
# points, load each adapter module (which runs its `@register` decorator and so
# self-registers), and verify the expected type key landed in the registry so a
# typo isn't silently ignored.

_ENTRY_POINT_GROUP = "warehouse_to_go.adapters"


def discover_adapters() -> Dict[str, Any]:
    """Import every entry-point adapter module.

    Returns a mapping of ``module_path -> module`` for the adapters discovered
    via entry points this call. Loading a module runs its ``@register``
    decorator, which self-registers the factory under ``<type_name>``. Any entry
    point that fails to register its declared type raises a clear error.
    """
    discovered: Dict[str, Any] = {}
    for entry_point in importlib.metadata.entry_points(group=_ENTRY_POINT_GROUP):
        module = entry_point.load()  # imports the adapter module -> self-registers
        discovered.setdefault(entry_point.name, module)
        if entry_point.name not in _REGISTRY:
            raise RuntimeError(
                f"Adapter module {getattr(module, '__name__', entry_point.value)!r} "
                f"was discovered via entry point {entry_point.name!r} but did not "
                f"self-register under that type name. Ensure it applies "
                f"`@register({entry_point.name!r})`."
            )
    return discovered


# Auto-discover built-in adapters on import so `get_adapter_factory(warehouse.type)`
# resolves without the caller importing each adapter module by hand.
discover_adapters()

