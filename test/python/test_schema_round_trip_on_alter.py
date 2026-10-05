"""DuckDB schema changes keep the requiredness of fields they don't touch.

DuckDB has no syntax for required list elements or map values, so the table is created with PyIceberg and then
altered by DuckDB.
"""

from uuid import uuid4

import pytest

from duckdb_unittest import DuckDBUnittestRunner

pytest.importorskip("pyiceberg")
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


def test_alter_preserves_nested_requiredness(rest_catalog, unittest_binary, unittest_test_config, print_unittest_stdin):
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "l", ListType(3, IntegerType(), element_required=True), required=False),
        NestedField(4, "m", MapType(5, StringType(), 6, IntegerType(), value_required=True), required=False),
        NestedField(7, "s", StructType(NestedField(8, "a", IntegerType(), required=True)), required=False),
    )
    name = "schema_round_trip_" + uuid4().hex
    rest_catalog.create_namespace_if_not_exists("default")
    rest_catalog.create_table(f"default.{name}", schema=schema)
    try:
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            # Each statement commits a new schema built by DuckDB from its column definitions.
            test.statement_ok(f"ALTER TABLE my_datalake.default.{name} ADD COLUMN extra INTEGER")
            test.statement_ok(f"ALTER TABLE my_datalake.default.{name} RENAME COLUMN extra TO extra2")
            test.statement_ok(f"COMMENT ON COLUMN my_datalake.default.{name}.extra2 IS 'added by duckdb'")

        table = rest_catalog.load_table(f"default.{name}")
        # The three ALTERs above each added a schema.
        assert len(table.metadata.schemas) == 4
        schema = table.schema()
        extra2 = schema.find_field("extra2")
        assert extra2.field_type == IntegerType()
        assert extra2.doc == "added by duckdb"
        assert schema.find_field("id").required
        assert schema.find_field("l").field_type.element_required
        assert schema.find_field("m").field_type.value_required
        assert schema.find_field("s.a").required
    finally:
        rest_catalog.drop_table(f"default.{name}")
