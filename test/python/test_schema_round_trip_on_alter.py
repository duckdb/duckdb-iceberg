"""DuckDB schema changes keep the types and requiredness of fields they don't touch.

DuckDB can't create some fields itself (it has no fixed-length binary type, and no syntax for required list
elements or map values), so the table is created with PyIceberg and then altered by DuckDB.
"""

from uuid import uuid4

import pytest

from duckdb_unittest import DuckDBUnittestRunner

pytest.importorskip("pyiceberg")
from pyiceberg.schema import Schema
from pyiceberg.types import (
    FixedType,
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


def _alter_with_duckdb(rest_catalog, schema, unittest_binary, unittest_test_config, print_unittest_stdin, queries=()):
    """Create a table with PyIceberg, apply three DuckDB schema changes, and return the reloaded table.

    `queries` are (sql_template, expected_rows) pairs run in the same DuckDB session after the ALTERs;
    `{name}` in the template is replaced with the table name.
    """
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
            for sql, expected in queries:
                test.query("T", sql.format(name=name), expected)

        table = rest_catalog.load_table(f"default.{name}")
        # The three ALTERs above each added a schema.
        assert len(table.metadata.schemas) == 4
        extra2 = table.schema().find_field("extra2")
        assert extra2.field_type == IntegerType()
        assert extra2.doc == "added by duckdb"
        return table
    finally:
        rest_catalog.drop_table(f"default.{name}")


def test_alter_preserves_fixed_type(rest_catalog, unittest_binary, unittest_test_config, print_unittest_stdin):
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "f", FixedType(16), required=False),
        NestedField(3, "lf", ListType(4, FixedType(4), element_required=False), required=False),
        NestedField(5, "s", StructType(NestedField(6, "a", FixedType(8), required=False)), required=False),
    )
    table = _alter_with_duckdb(
        rest_catalog,
        schema,
        unittest_binary,
        unittest_test_config,
        print_unittest_stdin,
        # A fixed column still reads as BLOB.
        queries=[
            (
                "SELECT data_type FROM information_schema.columns WHERE table_name = '{name}' AND column_name = 'f'",
                [("BLOB",)],
            )
        ],
    )
    schema = table.schema()
    assert schema.find_field("f").field_type == FixedType(16)
    assert schema.find_field("lf").field_type.element_type == FixedType(4)
    assert schema.find_field("s.a").field_type == FixedType(8)


def test_alter_preserves_nested_requiredness(rest_catalog, unittest_binary, unittest_test_config, print_unittest_stdin):
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "l", ListType(3, IntegerType(), element_required=True), required=False),
        NestedField(4, "m", MapType(5, StringType(), 6, IntegerType(), value_required=True), required=False),
        NestedField(7, "s", StructType(NestedField(8, "a", IntegerType(), required=True)), required=False),
    )
    table = _alter_with_duckdb(rest_catalog, schema, unittest_binary, unittest_test_config, print_unittest_stdin)
    schema = table.schema()
    assert schema.find_field("id").required
    assert schema.find_field("l").field_type.element_required
    assert schema.find_field("m").field_type.value_required
    assert schema.find_field("s.a").required
