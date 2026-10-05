"""DuckDB writes check required fields at every level, the way Iceberg's Java writer does.

DuckDB has no syntax for required nested fields, so the table is created with PyIceberg.
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


# Each case changes one column of an otherwise valid row:
# (id, l, m, s, s2, mk) = (1, [1], MAP {'k': 1}, {'a': 1}, {'inner': {'x': 1}}, MAP {{'a': 1, 'b': 'x'}: 1})
_VALID_ROW = {
    "id": "1",
    "l": "[1]",
    "m": "MAP {'k': 1}",
    "s": "{'a': 1}",
    "s2": "{'inner': {'x': 1}}",
    "mk": "MAP {{'a': 1, 'b': 'x'}: 1}",
}

_ACCEPTED = [
    ("l", "NULL"),
    ("l", "[]"),
    ("m", "NULL"),
    ("m", "MAP {}"),
    ("s", "NULL"),
    ("s2", "NULL"),
    ("s2", "{'inner': {'x': NULL}}"),
    # only key.a is required
    ("mk", "MAP {{'a': 1, 'b': NULL}: 1}"),
]

_REJECTED = [
    ("id", "NULL", "id"),
    ("l", "[1, NULL]", "l.element"),
    ("m", "MAP {'k': NULL}", "m.value"),
    ("s", "{'a': NULL}", "s.a"),
    ("s2", "{'inner': NULL}", "s2.inner"),
    # a map key is never NULL itself, but a required field inside a struct key can be
    ("mk", "MAP {{'a': NULL, 'b': 'x'}: 1}", "mk.key.a"),
]


def _row(column, value):
    row = dict(_VALID_ROW, **{column: value})
    return "(" + ", ".join(row[c] for c in ("id", "l", "m", "s", "s2", "mk")) + ")"


@pytest.mark.parametrize("statement", ["INSERT", "UPDATE"])
def test_write_checks_nested_requiredness(
    rest_catalog, unittest_binary, unittest_test_config, print_unittest_stdin, statement
):
    """A required field is only violated by a NULL where its parent is present."""
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "l", ListType(3, IntegerType(), element_required=True), required=False),
        NestedField(4, "m", MapType(5, StringType(), 6, IntegerType(), value_required=True), required=False),
        NestedField(7, "s", StructType(NestedField(8, "a", IntegerType(), required=True)), required=False),
        NestedField(
            9,
            "s2",
            StructType(NestedField(10, "inner", StructType(NestedField(11, "x", IntegerType())), required=True)),
            required=False,
        ),
        NestedField(
            12,
            "mk",
            MapType(
                13,
                StructType(NestedField(14, "a", IntegerType(), required=True), NestedField(15, "b", StringType())),
                16,
                IntegerType(),
            ),
            required=False,
        ),
    )
    name = "nested_not_null_" + uuid4().hex
    rest_catalog.create_namespace_if_not_exists("default")
    rest_catalog.create_table(
        f"default.{name}",
        schema=schema,
        properties={"write.update.mode": "merge-on-read", "write.delete.mode": "merge-on-read"},
    )
    table = f"my_datalake.default.{name}"
    try:
        with DuckDBUnittestRunner(
            unittest_binary,
            test_config=unittest_test_config,
            print_stdin=print_unittest_stdin,
        ) as test:
            if statement == "UPDATE":
                test.statement_ok(f"INSERT INTO {table} VALUES {_row('id', '1')}")
            for column, value in _ACCEPTED:
                if statement == "INSERT":
                    test.statement_ok(f"INSERT INTO {table} VALUES {_row(column, value)}")
                else:
                    test.statement_ok(f"UPDATE {table} SET {column} = {value}")
            for column, value, path in _REJECTED:
                expected = f"NOT NULL constraint failed: {name}.{path}"
                if statement == "INSERT":
                    test.statement_error(f"INSERT INTO {table} VALUES {_row(column, value)}", expected)
                else:
                    test.statement_error(f"UPDATE {table} SET {column} = {value}", expected)
            expected_rows = len(_ACCEPTED) if statement == "INSERT" else 1
            test.query("I", f"SELECT count(*) FROM {table}", [(expected_rows,)])
    finally:
        rest_catalog.drop_table(f"default.{name}")
