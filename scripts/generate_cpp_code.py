from parse_openapi_spec import (
    ResponseObjectsGenerator,
    Property,
    ArrayProperty,
    PrimitiveProperty,
    SchemaReferenceProperty,
    ObjectProperty,
)
import os
from typing import Dict, List, Set, Optional, cast, Callable
from enum import Enum, auto
from dataclasses import dataclass, field

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
OUTPUT_HEADER_DIR = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'objects')
OUTPUT_SOURCE_DIR = os.path.join(SCRIPT_PATH, '..', 'src', 'rest_catalog', 'objects')
API_SPEC_PATH = os.path.join(SCRIPT_PATH, 'api.yaml')

CPP_KEYWORDS = {
    'namespace',
    'class',
    'template',
    'operator',
    'private',
    'public',
    'protected',
    'virtual',
    'default',
    'delete',
    'final',
    'override',
    'error',  # add 'error' to avoid conflicts with the 'error' variable in TryFromJSON
}


def to_snake_case(name: str):
    res = ''
    prev_was_lower = False
    for x in name:
        is_lower = x.islower()
        if not is_lower and prev_was_lower:
            res += '_'
        prev_was_lower = is_lower
        res += x.lower()
    return res


def safe_cpp_name(name: str) -> str:
    """Convert property name to safe C++ variable name."""
    name = name.replace('-', '_')
    if name in CPP_KEYWORDS:
        return '_' + name
    return name


HEADER_FORMAT = """
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
{ADDITIONAL_HEADERS}

using namespace duckdb_yyjson;

namespace duckdb {{
namespace rest_api_objects {{

{FORWARD_DECLARATIONS}

{CLASS_DECLARATION}

}} // namespace rest_api_objects
}} // namespace duckdb

"""

SOURCE_FORMAT = """
#include "rest_catalog/objects/{HEADER_NAME}.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {{
namespace rest_api_objects {{

{CLASS_DEFINITION}

}} // namespace rest_api_objects
}} // namespace duckdb

"""

CMAKE_LISTS_FORMAT = """
add_library(
	rest_catalog_objects
	OBJECT
{ALL_SOURCE_FILES}
)

set(ALL_OBJECT_FILES
    ${{ALL_OBJECT_FILES}} $<TARGET_OBJECTS:rest_catalog_objects>
    PARENT_SCOPE)
"""


@dataclass
class ParseInfo:
    """Data taken from the parser"""

    recursive_schemas: Set[str]
    schemas: dict
    parsed_schemas: Dict[str, Property]


@dataclass
class OneOf:
    """An option of the OneOf construct"""

    name: str
    dereference_style: str
    class_name: str


@dataclass
class AnyOf:
    """An option of the AnyOf construct"""

    name: str
    dereference_style: str
    class_name: str


@dataclass
class AllOf:
    """An option of the AllOf construct"""

    name: str
    dereference_style: str
    class_name: str


@dataclass
class RequiredProperty:
    """A property that is required to be present in the JSON"""

    # The variable name in the generated cpp code
    variable_name: str
    # The property name in the JSON code
    property_name: str
    body: List[str]
    default: Optional[List[str]]


@dataclass
class OptionalProperty:
    """A property that is can or can't be present in the JSON"""

    # The variable name in the generated cpp code
    variable_name: str
    # The property name in the JSON code
    property_name: str
    body: List[str]


@dataclass
class AdditionalProperty:
    """The additional (typed) properties not covered by the spec"""

    body: List[str]
    exclude_list: List[str] = field(default_factory=list)
    skip_if_excluded: List[str] = field(default_factory=list)


@dataclass
class PrimitiveTypeMapping:
    conversion: str
    type_check: str
    cpp_type: str
    formats: Dict[str, "PrimitiveTypeMapping"] = field(default_factory=dict)


PRIMITIVE_TYPE_MAPPING = {
    'string': PrimitiveTypeMapping(type_check='yyjson_is_str', conversion='yyjson_get_str', cpp_type='string'),
    'integer': PrimitiveTypeMapping(
        type_check='yyjson_is_int',
        conversion='yyjson_get_int',
        cpp_type='int32_t',
        formats={
            'int64': PrimitiveTypeMapping(type_check='yyjson_is_sint', conversion='yyjson_get_sint', cpp_type='int64_t')
        },
    ),
    'boolean': PrimitiveTypeMapping(type_check='yyjson_is_bool', conversion='yyjson_get_bool', cpp_type='bool'),
    'number': PrimitiveTypeMapping(type_check='yyjson_is_num', conversion='yyjson_get_num', cpp_type='double'),
}


class CPPClass:
    def __init__(self, class_name, parse_info: ParseInfo):
        self.name = class_name
        self.parse_info = parse_info
        # The base classes that make up this class
        self.one_of: List[OneOf] = []
        self.all_of: List[AllOf] = []
        self.any_of: List[AnyOf] = []

        # Parsing code of the TryFromJSON method
        self.required_properties: Dict[str, RequiredProperty] = {}
        self.optional_properties: Dict[str, OptionalProperty] = {}
        self.additional_properties: Optional[AdditionalProperty] = None

        # Nested classes of this class (referenced by variables)
        self.nested_classes: Dict[str, "CPPClass"] = {}
        # (member) variables of the class
        self.variables: List[str] = []
        self.referenced_schemas: Set[str] = set()
        self.try_from_json_body: List[str] = []

    def get_all_referenced_schemas(self) -> Set[str]:
        res = set()
        res.update(self.referenced_schemas)
        for item in self.nested_classes.values():
            res.update(item.get_all_referenced_schemas())
        return res

    def from_object_property(self, schema: ObjectProperty):
        assert schema.type == Property.Type.OBJECT
        object_property = cast(ObjectProperty, schema)

        # Parse any base classes required for the schema (anyOf, allOf, oneOf)
        self.generate_all_of(schema)
        self.generate_one_of(schema)
        self.generate_any_of(schema)

        required = object_property.required
        if not required:
            required = []
        remaining_properties = [x for x in object_property.properties if x not in required]

        required_properties = {}
        optional_properties = {}
        for item in remaining_properties:
            optional_properties[item] = object_property.properties[item]
        for item in required:
            required_properties[item] = object_property.properties[item]

        self.generate_required_properties(name, required_properties)
        self.generate_optional_properties(name, optional_properties)
        self.generate_additional_properties(object_property.properties.keys(), object_property.additional_properties)

        res = []
        for _, item in self.required_properties.items():
            res.extend([f'\t{x}' for x in self.write_required_property(item)])
        for _, item in self.optional_properties.items():
            res.extend([f'\t{x}' for x in self.write_optional_property(item)])
        res.extend([f'\t{x}' for x in self.write_additional_properties()])
        self.try_from_json_body = res
        self.generate_nested_class_definitions()

    def from_array_property(self, schema: ArrayProperty):
        assert schema.type == Property.Type.ARRAY
        array_property = cast(ArrayProperty, schema)

        assert not array_property.all_of
        assert not array_property.one_of
        assert not array_property.any_of

        self.try_from_json_body = self.generate_array_loop('obj', 'value', array_property)

        nested_classes = self.generate_nested_class_definitions()

        variable_type = self.generate_variable_type(schema)
        self.variables.append(f'\t{variable_type} value;')

    def from_primitive_property(self, schema: PrimitiveProperty):
        assert not schema.all_of
        assert not schema.one_of
        assert not schema.any_of

        self.try_from_json_body = self.generate_assignment(schema, 'value', 'obj', True)

        variable_type = self.generate_variable_type(schema)
        self.variables.append(f'\t{variable_type} value;')

    def from_property(self, schema: Property) -> None:
        if schema.type == Property.Type.OBJECT:
            self.from_object_property(schema)
        elif schema.type == Property.Type.ARRAY:
            self.from_array_property(schema)
        elif schema.type == Property.Type.PRIMITIVE:
            self.from_primitive_property(schema)
        else:
            print(f"Unrecognized 'from_property' type {schema.type}")
            exit(1)

    def write_required_property(self, required_property: RequiredProperty) -> List[str]:
        res = []
        res.extend(
            [
                f'auto {required_property.variable_name}_val = yyjson_obj_get(obj, "{required_property.property_name}");',
                f'if (!{required_property.variable_name}_val) {{',
            ]
        )
        if required_property.default is not None:
            res.extend([f'\t{x}' for x in required_property.default])
        else:
            res.extend(
                [
                    f"""\treturn "{self.name} required property '{required_property.property_name}' is missing";"""
                ]
            )
        res.extend(
            [
                '} else {'
            ]
        )
        res.extend([f'\t{x}' for x in required_property.body])
        res.append('}')
        return res

    def write_optional_property(self, optional_property: OptionalProperty) -> List[str]:
        res = []
        res.extend(
            [
                f'auto {optional_property.variable_name}_val = yyjson_obj_get(obj, "{optional_property.property_name}");',
                f'if ({optional_property.variable_name}_val) {{',
                f'\thas_{optional_property.variable_name} = true;',
            ]
        )
        res.extend([f'\t{x}' for x in optional_property.body])
        res.append('}')
        return res

    def write_additional_properties(self) -> List[str]:
        if not self.additional_properties:
            return []
        res = []

        res.extend(self.additional_properties.exclude_list)
        res.extend(
            [
                'size_t idx, max;',
                'yyjson_val *key, *val;',
                'yyjson_obj_foreach(obj, idx, max, key, val) {',
            ]
        )
        # FIXME: check for null in returned char*?
        res.append('\tauto key_str = yyjson_get_str(key);')
        res.extend(self.additional_properties.skip_if_excluded)
        res.extend(self.additional_properties.body)
        res.extend(
            [
                '\tadditional_properties.emplace(key_str, std::move(tmp));',
                '}',
            ]
        )
        return res

    def write_all_of(self) -> List[str]:
        if not self.all_of:
            return []
        res = []
        for item in self.all_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend(
                [
                    f'error = {item.name}{item.dereference_style}TryFromJSON(obj);' 'if (!error.empty()) {',
                    '\treturn error;',
                    '}',
                ]
            )
        return res

    def write_one_of(self) -> List[str]:
        if not self.one_of:
            return []
        res = []
        res.append('do {')
        for item in self.one_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend(
                [
                    f'error = {item.name}{item.dereference_style}TryFromJSON(obj);',
                    'if (error.empty()) {',
                    f'\thas_{item.name} = true;',
                    '\tbreak;',
                    '}',
                ]
            )
        res.append(f'\treturn "{self.name} failed to parse, none of the oneOf candidates matched";')
        res.append('} while (false);')
        return res

    def write_any_of(self) -> List[str]:
        if not self.any_of:
            return []
        res = []

        all_options = sorted([f'!has_{item.name}' for item in self.any_of])
        condition = ' && '.join(all_options)

        for item in self.any_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend(
                [
                    f'error = {item.name}{item.dereference_style}TryFromJSON(obj);',
                    'if (error.empty()) {',
                    f'\thas_{item.name} = true;',
                    '}',
                ]
            )
        res.extend(
            [
                f'if ({condition}) {{'
                f'\treturn "{self.name} failed to parse, none of the anyOf candidates matched";'
                '}'
            ]
        )
        return res

    def write_nested_classes_header(self) -> List[str]:
        if not self.nested_classes:
            return []
        res = []
        for item, nested_class in self.nested_classes.items():
            res.extend(nested_class.write_header())
        return [f'\t{x}' for x in res]

    def write_nested_classes_source(self, base_class: List[str]) -> List[str]:
        if not self.nested_classes:
            return []
        res = []
        for item, nested_class in self.nested_classes.items():
            new_base_class = []
            new_base_class.extend(base_class)
            new_base_class.append(self.name)
            res.extend(nested_class.write_source(new_base_class))
        return [f'{x}' for x in res]

    def write_variables(self) -> List[str]:
        res = []
        if self.variables:
            res.append('public:')
            res.extend(self.variables)
        return res

    def write_source(self, base_class: List[str]) -> List[str]:
        res = []
        base = ''
        if base_class:
            base = '::'.join(base_class) + '::'

        res.extend([f'{base}{self.name}::{self.name}() {{}}'])
        res.extend(self.write_nested_classes_source(base_class))
        res.extend(
            [
                '',
                f'{base}{self.name} {base}{self.name}::FromJSON(yyjson_val *obj) {{',
                f'\t{self.name} res;',
                '\tauto error = res.TryFromJSON(obj);',
                '\tif (!error.empty()) {',
                '\t\tthrow InvalidInputException(error);',
                '\t}',
                '\treturn res;',
                '}',
                '',
                f'string {base}{self.name}::TryFromJSON(yyjson_val *obj) {{',
                '\tstring error;',
            ]
        )
        res.extend(self.write_one_of())
        res.extend(self.write_all_of())
        res.extend(self.write_any_of())
        res.extend(self.try_from_json_body)
        res.extend(['\treturn string();', '}'])
        return res

    def write_header(self) -> List[str]:
        res = []
        res.extend(
            [
                f'class {self.name} {{',
                'public:',
                f'\t{self.name}();',
                f'\t{self.name}(const {self.name}&) = delete;',
                f'\t{self.name}& operator=(const {self.name}&) = delete;',
                f'\t{self.name}({self.name}&&) = default;',
                f'\t{self.name} &operator=({self.name}&&) = default;',
            ]
        )
        res.extend(self.write_nested_classes_header())
        res.extend(
            [
                'public:',
                f'\tstatic {self.name} FromJSON(yyjson_val *obj);',
                'public:',
                '\tstring TryFromJSON(yyjson_val *obj);',
            ]
        )
        res.extend(self.write_variables())
        res.append('};')
        return res

    def generate_all_of(self, property: Property):
        if not property.all_of:
            return
        for item in property.all_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'

            self.all_of.append(AllOf(name=property_name, dereference_style=dereference_style, class_name=class_name))
            self.variables.append(f'\t{item.ref} {property_name};')

    def generate_any_of(self, property: Property):
        if not property.any_of:
            return
        for item in property.any_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'

            self.any_of.append(AnyOf(name=property_name, dereference_style=dereference_style, class_name=class_name))
            self.variables.append(f'\t{item.ref} {property_name};')
            self.variables.append(f'\tbool has_{property_name} = false;')

    def generate_one_of(self, property: Property):
        if not property.one_of:
            return
        for item in property.one_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'

            self.one_of.append(OneOf(name=property_name, dereference_style=dereference_style, class_name=class_name))
            self.variables.append(f'\t{item.ref} {property_name};')
            self.variables.append(f'\tbool has_{property_name} = false;')

    def generate_array_loop(self, array_name, destination_name, array_property: ArrayProperty) -> List[str]:
        item_type = array_property.item_type
        body = []
        body.append('size_t idx, max;')
        body.append('yyjson_val *val;')
        body.append(f'yyjson_arr_foreach({array_name}, idx, max, val) {{')

        assignment = 'std::move(tmp)'
        if item_type.type != Property.Type.SCHEMA_REFERENCE:
            body.append(f'{self.generate_variable_type(item_type)} tmp;')
            body.extend(self.generate_item_parse(item_type, 'val', 'tmp', True))
        else:
            schema_property = cast(SchemaReferenceProperty, item_type)
            self.referenced_schemas.add(schema_property.ref)
            item_definition = ''
            if schema_property.ref in self.parse_info.recursive_schemas:
                body.extend([f'\tauto tmp_p = make_uniq<{schema_property.ref}>();', '\tauto &tmp = *tmp_p;'])
                assignment = 'std::move(tmp_p)'
            else:
                body.append(f'\t{schema_property.ref} tmp;')
            body.extend(['\terror = tmp.TryFromJSON(val);', '\tif (!error.empty()) {', '\t\treturn error;', '\t}'])
        body.append(f'\t{destination_name}.emplace_back({assignment});')
        body.append('}')

        res = []
        prefix = ''
        if array_property.nullable is not None:
            prefix = '} else '
            if array_property.nullable == True:
                res.extend(
                    [f'if (yyjson_is_null({array_name})) {{', '\t//! do nothing, property is explicitly nullable']
                )
            else:
                res.extend(
                    [
                        f'if (yyjson_is_null({array_name})) {{',
                        f'''\treturn "{self.name} property '{destination_name}' is not nullable, but is 'null'";''',
                    ]
                )

        res.append(f'{prefix}if (yyjson_is_arr({array_name})) {{')
        res.extend([f'\t{x}' for x in body])
        res.extend(
            [
                '} else {',
                f"""\treturn StringUtil::Format("{self.name} property '{destination_name}' is not of type 'array', found '%s' instead", yyjson_get_type_desc({array_name}));""",
                '}',
            ]
        )

        return res

    def generate_item_parse(self, property: Property, source: str, target: str, is_required: bool) -> List[str]:
        res = []
        prefix = ''
        if property.nullable is not None:
            prefix = '} else '
            if property.nullable == True:
                res.extend([
                    f'if (yyjson_is_null({source})) {{',
                    '\t//! do nothing, property is explicitly nullable',
                ])
                if not is_required:
                    res.extend([
                        f'\thas_{target} = false;'
                    ])
            else:
                res.extend(
                    [
                        f'if (yyjson_is_null({source})) {{',
                        f'''\treturn "{self.name} property '{target}' is not nullable, but is 'null'";''',
                    ]
                )

        if property.type == Property.Type.SCHEMA_REFERENCE:
            print(f"Unrecognized property type {property.type}, {source}")
            exit(1)
        if property.type == Property.Type.ARRAY:
            # TODO: maybe we move the array parse to a function that creates a vector<...>, instead of parsing it inline
            print(f'Nested arrays are not supported, hopefully we dont have to!')
            exit(1)
        elif property.type == Property.Type.PRIMITIVE:
            # FIXME: add a check to see that the yyjson_val* is of the right type
            # FIXME: check for null in returned char* for 'yyjson_get_str?
            primitive_property = cast(PrimitiveProperty, property)
            item_type = primitive_property.primitive_type
            if item_type not in PRIMITIVE_TYPE_MAPPING:
                print(f"Primitive type '{item_type}' not in PRIMITIVE_TYPE_MAPPING")
                exit(1)

            type_mapping: PrimitiveTypeMapping = PRIMITIVE_TYPE_MAPPING[item_type]
            specific_mapping = None
            generic_mapping = None
            if type_mapping.formats and property.format in type_mapping.formats:
                assert item_type == 'integer'
                specific_mapping = type_mapping.formats[property.format]
            generic_mapping = type_mapping
            # NOTE: no need to really check the 'format' of the 'property' here
            # FIXME: 'target' is not the property name in the spec, it's already been transformed to the cpp variable name
            if specific_mapping:
                res.extend(
                    [
                        f'{prefix}if ({specific_mapping.type_check}({source})) {{',
                        f'\t{target} = {specific_mapping.conversion}({source});',
                    ]
                )
                res.extend(
                    [
                        f'}} else if (yyjson_is_uint({source})) {{',
                        f'\t{target} = yyjson_get_uint({source});',
                    ]
                )
            else:
                res.extend(
                    [
                        f'{prefix}if ({generic_mapping.type_check}({source})) {{',
                        f'\t{target} = {generic_mapping.conversion}({source});',
                    ]
                )

            res.extend(
                [
                    '} else {',
                    f"""\treturn StringUtil::Format("{self.name} property '{target}' is not of type '{item_type}', found '%s' instead", yyjson_get_type_desc({source}));""",
                    '}',
                ]
            )
        elif property.type == Property.Type.OBJECT and property.is_raw_object():
            res.extend(
                [
                    f'{prefix}if (yyjson_is_obj({source})) {{',
                    f'\t{target} = {source};',
                    '} else {',
                    f"""\treturn "{self.name} property '{target}' is not of type 'object'";""",
                    '}',
                ]
            )
        elif property.type == Property.Type.OBJECT and property.additional_properties:
            object_property = cast(ObjectProperty, property)
            additional_properties = property.additional_properties

            res.append(f'{prefix}if (yyjson_is_obj({source})) {{')
            res.extend(
                [
                    '\tsize_t idx, max;',
                    '\tyyjson_val *key, *val;',
                    f'\tyyjson_obj_foreach({source}, idx, max, key, val) {{',
                ]
            )
            # FIXME: check for null in returned char*?
            res.append('\t\tauto key_str = yyjson_get_str(key);')
            res.append(f'\t\t{self.generate_variable_type(additional_properties)} tmp;')

            if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
                item_definition = [f'\t\t{x}' for x in self.generate_item_parse(additional_properties, 'val', 'tmp', True)]
                res.extend(item_definition)
            else:
                schema_property = cast(SchemaReferenceProperty, additional_properties)
                self.referenced_schemas.add(schema_property.ref)
                if schema_property.ref in self.parse_info.recursive_schemas:
                    print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                    exit(1)
                res.append(f'\t\t{schema_property.ref} tmp;')
                res.extend(
                    [
                        '\terror = tmp.TryFromJSON(val);',
                        '\tif (!error.empty()) {',
                        '\t\treturn error;',
                        '\t}',
                    ]
                )
            res.extend(
                [
                    f'\t\t{target}.emplace(key_str, std::move(tmp));',
                    '\t}',
                ]
            )
            res.extend(['} else {', f"""\treturn "{self.name} property '{target}' is not of type 'object'";""", '}'])
        else:
            print(f"Unrecognized type in 'generate_item_parse', {property.type}")
            exit(1)
        return res

    def generate_assignment(self, schema: Property, target: str, source: str, is_required: bool) -> List[str]:
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            return self.generate_array_loop(source, target, array_property)
        elif schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            self.referenced_schemas.add(schema_property.ref)
            result = []
            dereference_style = '.'
            if schema_property.ref in self.parse_info.recursive_schemas:
                result.append(f'{target} = make_uniq<{schema_property.ref}>();')
                dereference_style = '->'
            result.extend(
                [
                    f'error = {target}{dereference_style}TryFromJSON({source});',
                    'if (!error.empty()) {',
                    '    return error;',
                    '}',
                ]
            )
            return result
        else:
            return self.generate_item_parse(schema, source, target, is_required)

    def generate_optional_properties(self, name: str, properties: Dict[str, Property]):
        if not properties:
            return
        res = []
        for item, optional_property in properties.items():
            variable_name = safe_cpp_name(item)
            body = self.generate_assignment(optional_property, variable_name, f'{variable_name}_val', False)
            self.optional_properties[item] = OptionalProperty(
                property_name=item, variable_name=variable_name, body=body
            )
            variable_type = self.generate_variable_type(optional_property)
            self.variables.append(f'\t{variable_type} {variable_name};')
            self.variables.append(f'\tbool has_{variable_name} = false;')

    def generate_required_properties(self, name: str, properties: Dict[str, Property]):
        if not properties:
            return
        res = []
        for item, required_property in properties.items():
            variable_name = safe_cpp_name(item)
            body = self.generate_assignment(required_property, variable_name, f'{variable_name}_val', True)
            if required_property.default is not None:
                default = [
                    f'{variable_name} = "{str(required_property.default)}";'
                ]
            else:
                default = None
            self.required_properties[item] = RequiredProperty(
                property_name=item, variable_name=variable_name, body=body, default=default
            )
            variable_type = self.generate_variable_type(required_property)
            self.variables.append(f'\t{variable_type} {variable_name};')

    def generate_additional_properties(self, properties: List[str], additional_properties: Property):
        if not additional_properties:
            return

        skip_if_excluded = []
        exclude_list = []
        if properties:
            exclude_list = [
                'case_insensitive_set_t handled_properties {',
                f"""\t\t{', '.join(f'"{x}"' for x in properties)} }};""",
            ]
            skip_if_excluded = [
                '\tif (handled_properties.count(key_str)) {',
                '\t\tcontinue;',
                '\t}',
            ]

        body = []
        if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
            body.append(f'\t{self.generate_variable_type(additional_properties)} tmp;')
            body.extend(self.generate_item_parse(additional_properties, 'val', 'tmp', True))
        else:
            schema_property = cast(SchemaReferenceProperty, additional_properties)
            self.referenced_schemas.add(schema_property.ref)
            if schema_property.ref in self.parse_info.recursive_schemas:
                print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                exit(1)
            body.append(f'\t{schema_property.ref} tmp;')
            body.extend(
                [
                    'error = tmp.TryFromJSON(val);',
                    'if (!error.empty()) {',
                    '\treturn error;',
                    '}',
                ]
            )
        self.additional_properties = AdditionalProperty(
            body=body, exclude_list=exclude_list, skip_if_excluded=skip_if_excluded
        )
        variable_type = self.generate_variable_type(additional_properties)
        self.variables.append(f'\tcase_insensitive_map_t<{variable_type}> additional_properties;')

    def generate_variable_type(self, schema: Property) -> str:
        if schema.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, schema)
            assert not object_property.properties
            if object_property.additional_properties:
                variable_type = self.generate_variable_type(object_property.additional_properties)
                return f'case_insensitive_map_t<{variable_type}>'
            return 'yyjson_val *'
        elif schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            item_type = self.generate_variable_type(array_property.item_type)
            return f'vector<{item_type}>'
        elif schema.type == Property.Type.PRIMITIVE:
            primitive_property = cast(PrimitiveProperty, schema)
            primitive_type = primitive_property.primitive_type
            if primitive_type in PRIMITIVE_TYPE_MAPPING:
                mapping = PRIMITIVE_TYPE_MAPPING[primitive_type]
                if mapping.formats and schema.format in mapping.formats:
                    return mapping.formats[schema.format].cpp_type
                return PRIMITIVE_TYPE_MAPPING[primitive_type].cpp_type
            elif primitive_type == 'number':
                if not primitive_property.format:
                    print(f"'number' without a 'format' property in the spec!")
                    exit(1)
                return primitive_property.format
            else:
                print(f"Unrecognized primitive type '{primitive_type}' in 'generate_variable_type'")
                exit(1)
        elif schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            if schema_property.ref in self.parse_info.recursive_schemas:
                return f'unique_ptr<{schema_property.ref}>'
            return schema_property.ref
        else:
            print(f"Unrecognized 'generate_variable_type' type {schema.type}")
            exit(1)

    def generate_nested_class_definitions(self):
        generated_schemas_referenced = [x for x in self.referenced_schemas if x not in self.parse_info.schemas]
        for item in generated_schemas_referenced:
            parsed_schema = self.parse_info.parsed_schemas[item]
            nested_class = CPPClass(item, self.parse_info)
            nested_class.from_property(parsed_schema)
            self.nested_classes[item] = nested_class


if __name__ == '__main__':
    openapi_parser = ResponseObjectsGenerator(API_SPEC_PATH)
    openapi_parser.parse_all_schemas()

    # Create directory if it doesn't exist
    os.makedirs(OUTPUT_HEADER_DIR, exist_ok=True)
    os.makedirs(OUTPUT_SOURCE_DIR, exist_ok=True)

    with open(os.path.join(OUTPUT_HEADER_DIR, 'list.hpp'), 'w') as f:
        lines = ["", "// This file is automatically generated and contains all REST API object headers", ""]
        # Add includes for all generated headers
        for name in openapi_parser.schemas:
            lines.append(f'#include "rest_catalog/objects/{to_snake_case(name)}.hpp"')
        f.write('\n'.join(lines))

    with open(os.path.join(OUTPUT_SOURCE_DIR, 'CMakeLists.txt'), 'w') as f:
        file_paths = []
        for name in openapi_parser.schemas:
            file_paths.append(f'\t{to_snake_case(name)}.cpp')
        f.write(CMAKE_LISTS_FORMAT.format(ALL_SOURCE_FILES='\n'.join(file_paths)))

    parse_info = ParseInfo(
        recursive_schemas=openapi_parser.recursive_schemas,
        schemas=openapi_parser.schemas,
        parsed_schemas=openapi_parser.parsed_schemas,
    )

    for name in openapi_parser.schemas:
        schema = openapi_parser.parsed_schemas[name]

        cpp_class = CPPClass(name, parse_info)
        cpp_class.from_property(schema)

        referenced_schemas = cpp_class.get_all_referenced_schemas()
        include_schemas = [x for x in referenced_schemas if x in parse_info.schemas]

        output_path = os.path.join(OUTPUT_HEADER_DIR, f'{to_snake_case(name)}.hpp')
        with open(output_path, 'w') as f:
            content = cpp_class.write_header()
            forward_declarations = [
                f'class {x};' for x in sorted(list(include_schemas)) if x in parse_info.recursive_schemas
            ]
            additional_headers = [
                f'#include "rest_catalog/objects/{to_snake_case(x)}.hpp"'
                for x in sorted(list(include_schemas))
                if x not in parse_info.recursive_schemas
            ]
            file_content = HEADER_FORMAT.format(
                ADDITIONAL_HEADERS='\n'.join(additional_headers),
                FORWARD_DECLARATIONS='\n'.join(forward_declarations),
                CLASS_DECLARATION='\n'.join(content),
            )
            f.write(file_content)

        output_path = os.path.join(OUTPUT_SOURCE_DIR, f'{to_snake_case(name)}.cpp')
        with open(output_path, 'w') as f:
            content = cpp_class.write_source([])
            additional_headers = [
                f'#include "rest_catalog/objects/{to_snake_case(x)}.hpp"' for x in sorted(list(include_schemas))
            ]
            file_content = SOURCE_FORMAT.format(
                HEADER_NAME=to_snake_case(name),
                ADDITIONAL_HEADERS='\n'.join(additional_headers),
                CLASS_DEFINITION='\n'.join(content),
            )
            f.write(file_content)
