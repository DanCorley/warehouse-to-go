from warehouse_to_go.warehouse.types import Type


def test_varchar_factory_respects_length() -> None:
    assert Type.varchar().to_sql() == "VARCHAR"
    assert Type.varchar(10).to_sql() == "VARCHAR(10)"


def test_scalar_factories() -> None:
    assert Type.integer().to_sql() == "INTEGER"
    assert Type.bigint().to_sql() == "BIGINT"
    assert Type.double().to_sql() == "DOUBLE"
    assert Type.boolean().to_sql() == "BOOLEAN"
    assert Type.timestamp().to_sql() == "TIMESTAMP"
    assert Type.date().to_sql() == "DATE"


def test_decimal_factory() -> None:
    assert Type.decimal(38, 6).to_sql() == "DECIMAL(38,6)"


def test_number_parsing_fixed_scale() -> None:
    assert Type.from_native("NUMBER(38,6)").to_sql() == "DECIMAL(38,6)"
    assert Type.from_native("NUMBER(8)").to_sql() == "DECIMAL(8,0)"


def test_number_parsing_no_scale() -> None:
    assert Type.from_native("NUMBER").to_sql() == "DECIMAL(38,0)"


def test_from_native_mapping_for_pandas_and_snowflake() -> None:
    assert Type.from_native("INT64").to_sql() == "INTEGER"
    assert Type.from_native("BIGINT").to_sql() == "BIGINT"
    assert Type.from_native("DOUBLE").to_sql() == "DOUBLE"
    assert Type.from_native("TIMESTAMP_NTZ").to_sql() == "TIMESTAMP"
    assert Type.from_native("VARCHAR").to_sql() == "VARCHAR"
    assert Type.from_native("BOOL").to_sql() == "BOOLEAN"
    assert Type.from_native("   varchar(50)  ").to_sql() == "VARCHAR"


def test_from_native_unknown_falls_back_to_double() -> None:
    assert Type.from_native("SOMETHING_NEW").to_sql() == "DOUBLE"


def test_coerce_nulls_and_nan() -> None:
    assert Type.varchar().coerce(None) is None
    assert Type.varchar().coerce(float("nan")) is None
    assert Type.varchar().coerce("ok") == "ok"
