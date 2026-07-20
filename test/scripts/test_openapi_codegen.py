from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from generate_cpp_code import CPPClass, ParseInfo
from parse_openapi_spec import Property, ResponseObjectsGenerator


def parse_spec(path=SCRIPTS_DIR / "api.yaml"):
    parser = ResponseObjectsGenerator(str(path))
    parser.parse_all_schemas()
    parse_info = ParseInfo(
        recursive_schemas=parser.recursive_schemas,
        schemas=parser.schemas,
        parsed_schemas=parser.parsed_schemas,
    )
    return parser, parse_info


def render_class(parser, parse_info, name):
    cpp_class = CPPClass(name, parse_info)
    cpp_class.from_property(parser.parsed_schemas[name])
    return cpp_class, "\n".join(cpp_class.write_header()), "\n".join(cpp_class.write_source([]))


def test_parser_uses_supplied_spec_path(tmp_path):
    spec_path = tmp_path / "minimal.yaml"
    spec_path.write_text(
        """
components:
  schemas:
    OnlySchema:
      type: string
  responses: {}
"""
    )

    parser, _ = parse_spec(spec_path)
    assert set(parser.schemas) == {"OnlySchema"}


def test_all_vendored_schemas_render():
    parser, parse_info = parse_spec()

    for name in parser.schemas:
        _, header, source = render_class(parser, parse_info, name)
        assert f"class {name}" in header
        assert f"{name}::TryFromJSON" in source


def test_inline_one_of_variant_becomes_nested_schema():
    parser, parse_info = parse_spec()
    schema = parser.parsed_schemas["FunctionDataType"]

    assert schema.one_of[0].type == Property.Type.SCHEMA_REFERENCE
    assert schema.one_of[0].ref == "FunctionDataTypeOneOf1"

    _, header, source = render_class(parser, parse_info, "FunctionDataType")
    assert "class FunctionDataTypeOneOf1" in header
    assert "FunctionDataType::FunctionDataTypeOneOf1::TryFromJSON" in source


def test_table_requirement_uses_discriminator_union():
    parser, parse_info = parse_spec()
    schema = parser.parsed_schemas["TableRequirement"]

    assert [item.ref for item in schema.one_of][:2] == ["AssertCreate", "AssertTableUUID"]
    assert not parser.parsed_schemas["AssertCreate"].all_of

    _, header, source = render_class(parser, parse_info, "TableRequirement")
    assert "optional<AssertCreate> assert_create" in header
    assert 'if (discriminator == "assert-create")' in source
    assert "unknown discriminator value" in source


def test_overlapping_primitive_one_of_keeps_all_matching_views():
    parser, parse_info = parse_spec()
    cpp_class, _, source = render_class(parser, parse_info, "PrimitiveTypeValue")

    assert not cpp_class.one_of
    assert len(cpp_class.any_of) == 16
    assert "integer_type_value.emplace()" in source
    assert "long_type_value.emplace()" in source
    assert "string_type_value.emplace()" in source


def test_inherited_const_and_array_valued_map_are_generated():
    parser, parse_info = parse_spec()

    _, _, update_source = render_class(parser, parse_info, "RemoveStatisticsUpdate")
    assert 'action_refinement != "remove-statistics"' in update_source

    _, map_header, map_source = render_class(parser, parse_info, "MultiValuedMap")
    assert "case_insensitive_map_t<vector<string>> additional_properties" in map_header
    assert "tmp.emplace_back(std::move(tmp_item))" in map_source
    assert "yyjson_mut_arr_append(value_obj" in map_source

    _, nullable_header, nullable_source = render_class(parser, parse_info, "AssertRefSnapshotId")
    assert "optional<int64_t> snapshot_id" in nullable_header
    assert "yyjson_mut_obj_add_null(doc, obj, \"snapshot-id\")" in nullable_source
    assert nullable_source.count('yyjson_mut_obj_add_null(doc, obj, "snapshot-id")') == 1

    transaction_source = (REPO_ROOT / "src/catalog/rest/transaction/iceberg_transaction.cpp").read_text()
    assert "AddExplicitNullSnapshotIds" not in transaction_source


def test_table_metadata_accepts_null_current_snapshot_without_patching_spec():
    parser, parse_info = parse_spec()
    schema = parser.parsed_schemas["TableMetadata"]

    assert schema.properties["current-snapshot-id"].nullable is True

    _, _, source = render_class(parser, parse_info, "TableMetadata")
    assert "if (yyjson_is_null(current_snapshot_id_val))" in source
    assert "property is explicitly nullable" in source


def test_openapi_31_null_syntax_and_nullable_references(tmp_path):
    spec_path = tmp_path / "nullable.yaml"
    spec_path.write_text(
        """
openapi: 3.1.1
components:
  schemas:
    NullableInteger:
      type: [integer, "null"]
      format: int64
    Container:
      type: object
      required:
        - required-value
      properties:
        required-value:
          oneOf:
            - type: string
            - type: "null"
        optional-value:
          type: [string, "null"]
        strict-value:
          type: string
        referenced-value:
          $ref: '#/components/schemas/NullableInteger'
  responses: {}
"""
    )

    parser, parse_info = parse_spec(spec_path)
    container = parser.parsed_schemas["Container"]
    assert parser.parsed_schemas["NullableInteger"].nullable is True
    assert container.properties["required-value"].nullable is True
    assert container.properties["optional-value"].nullable is True
    assert container.properties["strict-value"].nullable is None
    assert container.properties["referenced-value"].nullable is True

    _, header, source = render_class(parser, parse_info, "Container")
    assert "optional<string> required_value" in header
    assert "optional<NullableInteger> referenced_value" in header
    assert "if (yyjson_is_null(required_value_val))" in source
    assert "if (yyjson_is_null(optional_value_val))" in source
    assert "if (yyjson_is_null(referenced_value_val))" in source
    assert "if (yyjson_is_null(strict_value_val))" not in source


def test_nullable_page_token_reference_collapses_null_to_missing():
    parser, parse_info = parse_spec()
    schema = parser.parsed_schemas["ListTablesResponse"]

    assert parser.parsed_schemas["PageToken"].nullable is True
    assert schema.properties["next-page-token"].nullable is True

    _, _, source = render_class(parser, parse_info, "ListTablesResponse")
    null_branch = source.index("if (yyjson_is_null(next_page_token_val))")
    parse_branch = source.index("next_page_token_tmp.TryFromJSON(next_page_token_val)")
    assert null_branch < parse_branch
