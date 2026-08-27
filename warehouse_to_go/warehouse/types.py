"""Typed intermediate that houses all source-type knowledge.

`Type` is a value object: its `name` *is* the DuckDB column type string.
Adapters map their native type names to `Type` via `Type.from_native`; the
sink uses `Type.to_sql()` to build CREATE TABLE column definitions and
`Type.coerce` to normalise incoming raw values. Pandas is never required to do
type conversion.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

from warehouse_to_go.utils.output import print_info


@dataclass(frozen=True)
class Type:
    """A DuckDB column type. `name` is the literal DuckDB type string."""

    name: str

    def to_sql(self) -> str:
        return self.name

    def coerce(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return value

    @classmethod
    def varchar(cls, length: int = -1) -> "Type":
        return cls(f"VARCHAR({length})" if length and length > 0 else "VARCHAR")

    @classmethod
    def integer(cls) -> "Type":
        return cls("INTEGER")

    @classmethod
    def bigint(cls) -> "Type":
        return cls("BIGINT")

    @classmethod
    def double(cls) -> "Type":
        return cls("DOUBLE")

    @classmethod
    def boolean(cls) -> "Type":
        return cls("BOOLEAN")

    @classmethod
    def timestamp(cls) -> "Type":
        return cls("TIMESTAMP")

    @classmethod
    def date(cls) -> "Type":
        return cls("DATE")

    @classmethod
    def decimal(cls, precision: int, scale: int) -> "Type":
        return cls(f"DECIMAL({precision},{scale})")

    @classmethod
    def uuid(cls) -> "Type":
        return cls("UUID")

    @classmethod
    def array(cls) -> "Type":
        return cls("ARRAY")

    @classmethod
    def binary(cls) -> "Type":
        return cls("BLOB")

    @classmethod
    def from_native(cls, native: str) -> "Type":
        key = native.strip().upper().strip("'\"")
        base = key.split("(")[0].strip()
        if base in {"NUMBER", "DECIMAL", "NUMERIC"}:
            return _number(key)
        factory = _NATIVE_TO_TYPE.get(base)
        if factory is None:
            return cls("DOUBLE")  # unknown -> lose precision rather than fail
        return factory()


_NUMBER_RE = re.compile(
    r"\s*(?:NUMBER|DECIMAL|NUMERIC)\s*\(\s*(-?\d+)\s*(?:,\s*(-?\d+)\s*)?\)",
    re.IGNORECASE,
)


def _number(native: str) -> "Type":
    match = _NUMBER_RE.match(native)
    if not match:
        return Type.decimal(38, 0)
    precision = int(match.group(1))
    if match.group(2) is None:
        return Type.decimal(precision, 0)
    return Type.decimal(precision, int(match.group(2)))


# Native (pandas / Snowflake / future-dialect) type name -> Type factory.
# Adapters extend this table with their own native names; unknown types fall
# back to DOUBLE.
_NATIVE_TO_TYPE: dict[str, Any] = {
    "VARCHAR": Type.varchar,
    "CHAR": Type.varchar,
    "STRING": Type.varchar,
    "TEXT": Type.varchar,
    "BOOLEAN": Type.boolean,
    "BOOL": Type.boolean,
    "INT": Type.integer,
    "INTEGER": Type.integer,
    "INT64": Type.integer,
    "BIGINT": Type.bigint,
    "FLOAT": Type.double,
    "FLOAT64": Type.double,
    "DOUBLE": Type.double,
    "NUMBER": _number,
    "DECIMAL": _number,
    "NUMERIC": _number,
    "DATETIME": Type.timestamp,
    "TIMESTAMP_NTZ": Type.timestamp,
    "TIMESTAMP_NTZ_AT_TIMEZONE": Type.timestamp,
    "TIMESTAMP_LTZ": Type.timestamp,
    "TIMESTAMP_TZ": Type.timestamp,
    "DATE": Type.date,
    "ARRAY": Type.array,
    "OBJECT": Type.binary,
    "VARIANT": Type.binary,
    "GEOGRAPHY": Type.binary,
    "BINARY": Type.binary,
    "FILE": Type.binary,
}


def print_native_map() -> None:  # pragma: no cover - debug aid
    print_info("Native type map:", style="bold dim")
    for name, factory in _NATIVE_TO_TYPE.items():
        sample = Type.from_native(name).to_sql()
        print_info(f"  {name:20s} -> {sample}")
