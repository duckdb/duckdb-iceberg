import uuid

import pytest

from duckdb_unittest import DuckDBUnittestRunner

pytest.importorskip("pyiceberg")
from pyiceberg.schema import Schema
from pyiceberg.types import BinaryType, FixedType, IntegerType, ListType, MapType, NestedField, StructType


@pytest.mark.parametrize("required", [False, True])
def test_fixed_schema_round_trip_on_alter(
    rest_catalog, unittest_binary, unittest_test_config, print_unittest_stdin, required
):
    name = f"fixed_schema_round_trip_{uuid.uuid4().hex}"
    identifier = ("default", name)
    schema = Schema(
        NestedField(1, "f", FixedType(4), required=required),
        NestedField(2, "l", ListType(3, FixedType(2), element_required=required), required=False),
        NestedField(
            4,
            "m",
            MapType(
                5,
                FixedType(1),
                6,
                StructType(
                    NestedField(7, "f", FixedType(3), required=required),
                    NestedField(8, "l", ListType(9, IntegerType(), element_required=required), required=False),
                ),
                value_required=required,
            ),
            required=False,
        ),
        NestedField(10, "b", BinaryType(), required=False),
    )
    table = rest_catalog.create_table(identifier, schema=schema)
    # Catalogs may assign fresh field IDs. Compare with the committed schema.
    original_fields = table.schema().fields
    try:
        for alter in [
            "ADD COLUMN extra INTEGER",
            "RENAME COLUMN extra TO renamed",
            "DROP COLUMN renamed",
        ]:
            with DuckDBUnittestRunner(
                unittest_binary, test_config=unittest_test_config, print_stdin=print_unittest_stdin
            ) as test:
                test.statement_ok(f"ALTER TABLE my_datalake.default.{name} {alter}")
            table.refresh()
            assert table.schema().fields[: len(original_fields)] == original_fields

        with DuckDBUnittestRunner(
            unittest_binary, test_config=unittest_test_config, print_stdin=print_unittest_stdin
        ) as test:
            test.statement_ok(f"COMMENT ON COLUMN my_datalake.default.{name}.b IS 'binary column'")
        table.refresh()
        assert table.schema().fields[:3] == original_fields[:3]
        assert table.schema().find_field("b").field_type == BinaryType()
        assert table.schema().find_field("b").doc == "binary column"
    finally:
        rest_catalog.drop_table(identifier)
