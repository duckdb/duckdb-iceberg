from parse_openapi_spec import (
    ResponseObjectsGenerator,
    Property,
    ArrayProperty,
    PrimitiveProperty,
    SchemaReferenceProperty,
    ObjectProperty,
)
import os
import json
from typing import Dict, List, Tuple, Set, Optional, cast, Callable
from enum import Enum, auto
from dataclasses import dataclass, field

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
OUTPUT_HEADER_DIR = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'objects')
OUTPUT_SOURCE_DIR = os.path.join(SCRIPT_PATH, '..', 'src', 'rest_catalog', 'objects')
API_SPEC_PATH = os.path.join(SCRIPT_PATH, 'api.yaml')

#! Schemas whose map keys are case-sensitive by spec. Iceberg ref names (branches and tags) are
#! case-sensitive, so 'Dev' and 'dev' are distinct refs and neither may be dropped while parsing.
CASE_SENSITIVE_MAP_SCHEMAS = {'SnapshotReferences'}

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
    'doc',  # add 'doc' to avoid conflicts with the 'doc' variable in StructField
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

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
{ADDITIONAL_HEADERS}

namespace duckdb {{
namespace rest_api_objects {{

{FORWARD_DECLARATIONS}

{CLASS_DECLARATION}

}} // namespace rest_api_objects
}} // namespace duckdb
"""

SOURCE_FORMAT = """
#include "rest_catalog/objects/{HEADER_NAME}.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {{
namespace rest_api_objects {{

{CLASS_DEFINITION}

}} // namespace rest_api_objects
}} // namespace duckdb
"""

JSON_UTILS_HEADER_FORMAT = """
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {{
namespace rest_api_objects {{
namespace json_utils {{

inline bool IsNull(const JSONValue &value) {{
	return value.IsNull();
}}

inline void *GetNull(const JSONValue &value) {{
	return nullptr;
}}

inline bool IsString(const JSONValue &value) {{
	return value.IsString();
}}

inline string GetString(const JSONValue &value) {{
	return value.GetString();
}}

inline bool IsInteger(const JSONValue &value) {{
	return value.IsInteger();
}}

inline bool IsUnsignedInteger(const JSONValue &value) {{
	return value.GetType() == JSONValueType::UNSIGNED_INTEGER;
}}

inline bool IsBoolean(const JSONValue &value) {{
	return value.GetType() == JSONValueType::BOOLEAN;
}}

inline bool GetBoolean(const JSONValue &value) {{
	return value.GetBoolean();
}}

inline bool IsNumber(const JSONValue &value) {{
	return value.IsInteger() || value.GetType() == JSONValueType::DOUBLE;
}}

inline int64_t GetSignedInteger(const JSONValue &value) {{
	return value.GetType() == JSONValueType::SIGNED_INTEGER ? value.GetSignedInteger()
	                                                        : static_cast<int64_t>(value.GetUnsignedInteger());
}}

inline uint64_t GetUnsignedInteger(const JSONValue &value) {{
	return value.GetType() == JSONValueType::UNSIGNED_INTEGER
	           ? value.GetUnsignedInteger()
	           : static_cast<uint64_t>(value.GetSignedInteger());
}}

inline double GetNumber(const JSONValue &value) {{
	return value.IsInteger() ? static_cast<double>(GetSignedInteger(value)) : value.GetDouble();
}}

inline string GetTypeDescription(const JSONValue &value) {{
	return StringUtil::Format("JSON type %d", static_cast<int>(value.GetType()));
}}

}} // namespace json_utils
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
    discriminator_value: Optional[str] = None


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
    schema: Property  # Store the property schema for serialization


@dataclass
class OptionalProperty:
    """A property that is can or can't be present in the JSON"""

    # The variable name in the generated cpp code
    variable_name: str
    # The property name in the JSON code
    property_name: str
    body: List[str]
    schema: Property  # Store the property schema for serialization
    nullable: bool
    uses_optional_wrapper: bool


@dataclass
class AdditionalProperty:
    """The additional (typed) properties not covered by the spec"""

    body: List[str]
    exclude_list: List[str] = field(default_factory=list)
    skip_if_excluded: List[str] = field(default_factory=list)
    schema: Optional[Property] = None  # Store the property schema for serialization


@dataclass
class CPPMember:
    """A generated C++ class member"""

    variable_name: str
    variable_type: str
    schema: Optional[Property]
    initializer: Optional[str] = None
    copy_guard: Optional[str] = None
    uses_optional_wrapper: bool = False


@dataclass
class PrimitiveTypeMapping:
    conversion: str
    type_check: str
    cpp_type: str
    formats: Dict[str, "PrimitiveTypeMapping"] = field(default_factory=dict)


PRIMITIVE_TYPE_MAPPING = {
    None: PrimitiveTypeMapping(type_check='json_utils::IsNull', conversion='json_utils::GetNull', cpp_type='void*'),
    'string': PrimitiveTypeMapping(
        type_check='json_utils::IsString', conversion='json_utils::GetString', cpp_type='string'
    ),
    'integer': PrimitiveTypeMapping(
        type_check='json_utils::IsInteger',
        conversion='json_utils::GetSignedInteger',
        cpp_type='int32_t',
        formats={
            'int64': PrimitiveTypeMapping(
                type_check='json_utils::IsInteger', conversion='json_utils::GetSignedInteger', cpp_type='int64_t'
            )
        },
    ),
    'boolean': PrimitiveTypeMapping(
        type_check='json_utils::IsBoolean', conversion='json_utils::GetBoolean', cpp_type='bool'
    ),
    'number': PrimitiveTypeMapping(
        type_check='json_utils::IsNumber', conversion='json_utils::GetNumber', cpp_type='double'
    ),
}


class CPPClass:
    def __init__(self, class_name, parse_info: ParseInfo):
        self.name = class_name
        self.parse_info = parse_info
        # The base classes that make up this class
        self.one_of: List[OneOf] = []
        self.all_of: List[AllOf] = []
        self.any_of: List[AnyOf] = []
        self.discriminator_property: Optional[str] = None

        # Parsing code of the TryFromJSON method
        self.required_properties: Dict[str, RequiredProperty] = {}
        self.optional_properties: Dict[str, OptionalProperty] = {}
        self.additional_properties: Optional[AdditionalProperty] = None

        # Nested classes of this class (referenced by variables)
        self.nested_classes: Dict[str, "CPPClass"] = {}
        # (member) variables of the class
        self.variables: List[str] = []
        self.members: List[CPPMember] = []
        self.referenced_schemas: Set[str] = set()
        self.try_from_json_body: List[str] = []

    def add_member(
        self,
        variable_name: str,
        variable_type: str,
        schema: Optional[Property],
        initializer: Optional[str] = None,
        copy_guard: Optional[str] = None,
        uses_optional_wrapper: bool = False,
    ) -> None:
        initializer_text = f' = {initializer}' if initializer is not None else ''
        self.variables.append(f'{variable_type} {variable_name}{initializer_text};')
        self.members.append(
            CPPMember(
                variable_name=variable_name,
                variable_type=variable_type,
                schema=schema,
                initializer=initializer,
                copy_guard=copy_guard,
                uses_optional_wrapper=uses_optional_wrapper,
            )
        )

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

        inherited_properties = self.collect_all_of_property_names(schema)
        self.validate_polymorphic_property_ownership(schema, inherited_properties)
        refinement_body = self.generate_inherited_property_refinements(object_property, inherited_properties)

        required = object_property.required
        if not required:
            required = []
        remaining_properties = [
            x for x in object_property.properties if x not in required and x not in inherited_properties
        ]

        required_properties = {}
        optional_properties = {}
        for item in remaining_properties:
            optional_properties[item] = object_property.properties[item]
        for item in required:
            if item in inherited_properties:
                continue
            required_properties[item] = object_property.properties[item]

        self.generate_required_properties(self.name, required_properties)
        self.generate_optional_properties(self.name, optional_properties)
        self.generate_additional_properties(object_property.properties.keys(), object_property.additional_properties)

        res = []
        for _, item in self.required_properties.items():
            res.extend(self.write_required_property(item))
        for _, item in self.optional_properties.items():
            res.extend(self.write_optional_property(item))
        res.extend(self.write_additional_properties())
        self.try_from_json_body = refinement_body + res
        self.generate_nested_class_definitions()

    def generate_inherited_property_refinements(
        self, object_property: ObjectProperty, inherited_properties: Set[str]
    ) -> List[str]:
        result = []
        required = set(object_property.required or [])
        for property_name in object_property.properties.keys() & inherited_properties:
            property_schema = object_property.properties[property_name]
            variable_name = safe_cpp_name(property_name) + '_refinement'
            value_name = variable_name + '_val'
            result.append(f'auto {value_name} = obj.GetMember("{property_name}");')
            result.append(f'if ({value_name}.IsValid()) {{')
            result.append(f'{self.generate_variable_type(property_schema)} {variable_name};')
            assignment = self.generate_assignment(property_schema, variable_name, value_name, True)
            result.extend(assignment)
            if property_name in required:
                result.extend(
                    [
                        '} else {',
                        f'''return "{self.name} required property '{property_name}' is missing";''',
                        '}',
                    ]
                )
            else:
                result.append('}')
        return result

    def from_array_property(self, schema: ArrayProperty):
        assert schema.type == Property.Type.ARRAY
        array_property = cast(ArrayProperty, schema)

        assert not array_property.all_of
        assert not array_property.one_of
        assert not array_property.any_of

        self.try_from_json_body = self.generate_array_loop('obj', 'value', array_property)

        nested_classes = self.generate_nested_class_definitions()

        variable_type = self.generate_variable_type(schema)
        self.add_member('value', variable_type, schema)

    def from_primitive_property(self, schema: PrimitiveProperty):
        assert not schema.all_of
        assert not schema.one_of
        assert not schema.any_of

        self.try_from_json_body = self.generate_assignment(schema, 'value', 'obj', True)

        variable_type = self.generate_variable_type(schema)
        self.add_member('value', variable_type, schema)

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
                f'auto {required_property.variable_name}_val = obj.GetMember("{required_property.property_name}");',
                f'if (!{required_property.variable_name}_val.IsValid()) {{',
            ]
        )
        if required_property.default is not None:
            res.extend(required_property.default)
        else:
            res.extend([f"""return "{self.name} required property '{required_property.property_name}' is missing";"""])
        res.extend(['} else {'])
        res.extend(required_property.body)
        res.append('}')
        return res

    def write_optional_property(self, optional_property: OptionalProperty) -> List[str]:
        res = []
        res.extend(
            [
                f'auto {optional_property.variable_name}_val = obj.GetMember("{optional_property.property_name}");',
                f'if ({optional_property.variable_name}_val.IsValid()) {{',
            ]
        )
        if optional_property.nullable:
            res.extend(
                [
                    f'if ({optional_property.variable_name}_val.IsNull()) {{',
                    '//! do nothing, property is explicitly nullable',
                    '} else {',
                ]
            )
            res.extend(optional_property.body)
            res.append('}')
        else:
            res.extend(optional_property.body)
        res.append('}')
        return res

    @staticmethod
    def make_callback_safe(lines: List[str]) -> List[str]:
        result = []
        for line in lines:
            statement = line.strip()
            if statement.startswith('return ') and statement.endswith(';'):
                expression = statement[len('return ') : -1]
                if expression != 'error':
                    result.append(f'error = {expression};')
                result.append('return;')
            elif statement == 'continue;':
                result.append('return;')
            else:
                result.append(statement)
        return result

    def write_additional_properties(self) -> List[str]:
        if not self.additional_properties:
            return []
        res = []

        res.extend(self.additional_properties.exclude_list)
        res.append('obj.IterateObject([&](const string &key_str, JSONValue val) {')
        res.append('if (!error.empty()) {')
        res.append('return;')
        res.append('}')
        res.extend(self.make_callback_safe(self.additional_properties.skip_if_excluded))
        res.extend(self.make_callback_safe(self.additional_properties.body))
        res.extend(
            [
                'additional_properties.emplace(key_str, std::move(tmp));',
                '});',
                'if (!error.empty()) {',
                'return error;',
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
                    'return error;',
                    '}',
                ]
            )
        return res

    def write_one_of(self) -> List[str]:
        if not self.one_of:
            return []
        if self.discriminator_property and all(item.discriminator_value is not None for item in self.one_of):
            res = [
                f'auto discriminator_val = obj.GetMember("{self.discriminator_property}");',
                'if (!discriminator_val.IsValid() || !discriminator_val.IsString()) {',
                f'''return "{self.name} discriminator '{self.discriminator_property}' is missing or is not a string";''',
                '}',
                'string discriminator = discriminator_val.GetString();',
            ]
            for index, item in enumerate(self.one_of):
                prefix = 'if' if index == 0 else 'else if'
                res.append(f'{prefix} (discriminator == {json.dumps(item.discriminator_value)}) {{')
                is_recursive = item.class_name in self.parse_info.recursive_schemas
                if is_recursive:
                    res.append(f'{item.name} = make_uniq<{item.class_name}>();')
                else:
                    res.append(f'{item.name}.emplace();')
                res.extend(
                    [
                        f'error = {item.name}->TryFromJSON(obj);',
                        'if (!error.empty()) {',
                        'return error;',
                        '}',
                        '}',
                    ]
                )
            res.extend(
                [
                    'else {',
                    f'''return StringUtil::Format("{self.name} has unknown discriminator value '%s'", discriminator.c_str());''',
                    '}',
                ]
            )
            return res

        res = []
        res.append('do {')
        for item in self.one_of:
            is_recursive = item.class_name in self.parse_info.recursive_schemas
            if is_recursive:
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            else:
                res.append(f'{item.name}.emplace();')
            res.extend(
                [
                    f'error = {item.name}->TryFromJSON(obj);',
                    'if (error.empty()) {',
                    'break;',
                    '} else {',
                    f'{item.name} = {"nullptr" if is_recursive else "nullopt"};',
                    '}',
                ]
            )
        res.append(f'return "{self.name} failed to parse, none of the oneOf candidates matched";')
        res.append('} while (false);')
        return res

    def write_any_of(self) -> List[str]:
        if not self.any_of:
            return []
        res = []

        all_options = sorted(
            [
                f'!({self.presence_condition(item.name, item.class_name not in self.parse_info.recursive_schemas)})'
                for item in self.any_of
            ]
        )
        condition = ' && '.join(all_options)

        for item in self.any_of:
            is_recursive = item.class_name in self.parse_info.recursive_schemas
            if is_recursive:
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            else:
                res.append(f'{item.name}.emplace();')
            res.extend(
                [
                    f'error = {item.name}->TryFromJSON(obj);',
                    'if (error.empty()) {',
                    '} else {',
                    f'{item.name} = {"nullptr" if is_recursive else "nullopt"};',
                    '}',
                ]
            )

        res.extend(
            [
                'if (' + condition + ') {',
                f'return "{self.name} failed to parse, none of the anyOf candidates matched";',
                '}',
            ]
        )
        return res

    def write_nested_classes_header(self) -> List[str]:
        if not self.nested_classes:
            return []
        res = []
        for nested_class in self.nested_classes.values():
            res.extend(nested_class.write_header())
            res.append('')
        return res

    def write_nested_classes_source(self, base_class: List[str]) -> List[str]:
        if not self.nested_classes:
            return []
        res = []
        for nested_class in self.nested_classes.values():
            res.extend(nested_class.write_source(base_class + [self.name]))
        return res

    def write_variables(self) -> List[str]:
        if not self.variables:
            return []
        return ['public:'] + self.variables

    def collect_property_names(self, property: Property, visited: Optional[Set[str]] = None) -> Set[str]:
        if visited is None:
            visited = set()

        if property.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, property)
            if schema_property.ref in visited:
                return set()
            visited = set(visited)
            visited.add(schema_property.ref)
            return self.collect_property_names(self.parse_info.parsed_schemas[schema_property.ref], visited)

        if property.type != Property.Type.OBJECT:
            return set()

        object_property = cast(ObjectProperty, property)
        names = set(object_property.properties.keys())
        for base_property in object_property.all_of:
            names.update(self.collect_property_names(base_property, visited))
        return names

    def collect_all_of_property_names(self, property: Property) -> Set[str]:
        if not property.all_of:
            return set()

        seen: Set[str] = set()
        for base_property in property.all_of:
            base_names = self.collect_property_names(base_property)
            overlap = seen.intersection(base_names)
            if overlap:
                overlap_str = ', '.join(sorted(overlap))
                print(f"Schema '{self.name}' has duplicate allOf base properties: {overlap_str}")
                exit(1)
            seen.update(base_names)
        return seen

    def validate_polymorphic_property_ownership(self, property: Property, inherited_properties: Set[str]) -> None:
        local_properties = set(cast(ObjectProperty, property).properties.keys()) - inherited_properties

        for composition_name, variants in (('anyOf', property.any_of), ('oneOf', property.one_of)):
            for variant in variants:
                variant_properties = self.collect_property_names(variant)
                overlap = local_properties.intersection(variant_properties)
                if overlap:
                    overlap_str = ', '.join(sorted(overlap))
                    print(
                        f"Schema '{self.name}' has duplicate properties shared between local fields and {composition_name}: {overlap_str}"
                    )
                    exit(1)

    def direct_copy_expression(self, source: str, schema: Property) -> str:
        if schema.type == Property.Type.PRIMITIVE:
            return source
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            if self.schema_is_directly_copyable(array_property.item_type):
                return source
            print(f"Unhandled array copy expression for '{source}'")
            exit(1)
        if schema.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, schema)
            if object_property.is_raw_object():
                return source
            print(f"Unhandled object copy expression for '{source}'")
            exit(1)
        if schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            if schema_property.ref in self.parse_info.recursive_schemas:
                return f'{source} ? make_uniq<{schema_property.ref}>({source}->Copy()) : nullptr'
            return f'{source}.Copy()'
        print(f"Unhandled direct copy expression type for '{source}': {schema.type}")
        exit(1)

    def schema_is_directly_copyable(self, schema: Property) -> bool:
        if schema.type == Property.Type.PRIMITIVE:
            return True
        if schema.type == Property.Type.ARRAY:
            return self.schema_is_directly_copyable(cast(ArrayProperty, schema).item_type)
        if schema.type == Property.Type.OBJECT:
            return cast(ObjectProperty, schema).is_raw_object()
        return False

    def uses_pointer_storage(self, schema: Property) -> bool:
        return (
            schema.type == Property.Type.SCHEMA_REFERENCE
            and cast(SchemaReferenceProperty, schema).ref in self.parse_info.recursive_schemas
        )

    def uses_optional_wrapper(self, schema: Property) -> bool:
        return not self.uses_pointer_storage(schema)

    def presence_condition(self, variable_name: str, uses_optional_wrapper: bool) -> str:
        if uses_optional_wrapper:
            return f'{variable_name}.has_value()'
        return f'{variable_name} != nullptr'

    def optional_member_type(self, schema: Property) -> str:
        variable_type = self.generate_variable_type(schema)
        if self.uses_optional_wrapper(schema):
            return f'optional<{variable_type}>'
        return variable_type

    def value_access_expression(self, variable_name: str, uses_optional_wrapper: bool) -> str:
        if uses_optional_wrapper:
            return f'(*{variable_name})'
        return variable_name

    def generate_optional_assignment(self, schema: Property, target: str, source: str) -> List[str]:
        if self.uses_optional_wrapper(schema):
            tmp_name = f'{target}_tmp'
            variable_type = self.generate_variable_type(schema)
            res = [f'{variable_type} {tmp_name};']
            res.extend(self.generate_assignment(schema, tmp_name, source, True, handle_nullable=False))
            res.append(f'{target} = std::move({tmp_name});')
            return res
        return self.generate_assignment(schema, target, source, True, handle_nullable=False)

    def generate_nullable_assignment(self, schema: Property, target: str, source: str) -> List[str]:
        uses_optional_wrapper = self.uses_optional_wrapper(schema)
        result = [f'if ({source}.IsNull()) {{']
        result.append(f'{target} = {"nullopt" if uses_optional_wrapper else "nullptr"};')
        result.append('} else {')
        if uses_optional_wrapper:
            temporary = f'{target}_tmp'
            result.append(f'{self.generate_variable_type(schema)} {temporary};')
            assignment = self.generate_assignment(schema, temporary, source, True, handle_nullable=False)
            result.extend(assignment)
            result.append(f'{target} = std::move({temporary});')
        else:
            assignment = self.generate_assignment(schema, target, source, True, handle_nullable=False)
            result.extend(assignment)
        result.append('}')
        return result

    def write_copy_assignment_lines(self, target: str, source: str, schema: Optional[Property]) -> List[str]:
        if schema is None:
            return [f'{target} = {source};']
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            item_type = array_property.item_type
            item_copy = self.direct_copy_expression('item', item_type)
            return [
                f'{target}.reserve({source}.size());',
                f'for (auto &item : {source}) {{',
                f'{target}.emplace_back({item_copy});',
                '}',
            ]
        if schema.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, schema)
            if object_property.additional_properties:
                value_copy = self.direct_copy_expression('entry.second', object_property.additional_properties)
                return [
                    f'for (auto &entry : {source}) {{',
                    f'{target}.emplace(entry.first, {value_copy});',
                    '}',
                ]
        return [f'{target} = {self.direct_copy_expression(source, schema)};']

    def write_copy_method_source(self, base: str) -> List[str]:
        res = [
            '',
            f'{base}{self.name} {base}{self.name}::Copy() const {{',
            f'{self.name} res;',
        ]
        for member in self.members:
            target = f'res.{member.variable_name}'
            source = member.variable_name
            if member.uses_optional_wrapper:
                lines = [f'{target}.emplace();']
                lines.extend(
                    self.write_copy_assignment_lines(
                        self.value_access_expression(target, True),
                        self.value_access_expression(source, True),
                        member.schema,
                    )
                )
            else:
                lines = self.write_copy_assignment_lines(target, source, member.schema)
            if member.copy_guard is not None:
                lines = [f'if ({member.copy_guard}) {{'] + lines + ['}']
            res.extend(lines)
        res.extend(['return res;', '}'])
        return res

    def write_source(self, base_class: List[str]) -> List[str]:
        res = []
        base = '::'.join(base_class) + '::' if base_class else ''
        qualified_name = f'{base}{self.name}'
        supports_population = self.supports_json_object_population()

        res.append(f'{qualified_name}::{self.name}() {{}}')
        res.extend(self.write_nested_classes_source(base_class))

        # Deserialization method
        res.extend(
            [
                '',
                f'{qualified_name} {qualified_name}::FromJSON(JSONValue obj) {{',
                f'{self.name} res;',
                'auto error = res.TryFromJSON(obj);',
                'if (!error.empty()) {',
                'throw InvalidInputException(error);',
                '}',
                'return res;',
                '}',
            ]
        )
        res.extend(self.write_copy_method_source(base))
        res.extend(
            [
                '',
                f'string {qualified_name}::TryFromJSON(JSONValue obj) {{',
                'string error;',
            ]
        )
        res.extend(self.write_all_of())
        res.extend(self.write_one_of())
        res.extend(self.write_any_of())
        res.extend(self.try_from_json_body)
        res.extend(
            [
                'return "";',
                '}',
                '',
            ]
        )

        # Serialization methods
        if supports_population:
            res.extend([''])
            res.extend(self.generate_populate_json_method(qualified_name))
            res.extend([''])
        res.extend(self.generate_to_json_method(qualified_name))
        return res

    def write_header(self) -> List[str]:
        res = []
        supports_population = self.supports_json_object_population()
        res.extend(
            [
                f'class {self.name} {{',
                'public:',
                f'{self.name}();',
                f'{self.name}(const {self.name}&) = delete;',
                f'{self.name}& operator=(const {self.name}&) = delete;',
                f'{self.name}({self.name}&&) = default;',
                f'{self.name} &operator=({self.name}&&) = default;',
            ]
        )
        res.extend(self.write_nested_classes_header())
        res.extend(
            [
                'public:',
                '// Deserialization',
                f'static {self.name} FromJSON(JSONValue obj);',
                'string TryFromJSON(JSONValue obj);',
                '',
                '// Copy',
                f'{self.name} Copy() const;',
                '',
                '// Serialization',
            ]
        )
        if supports_population:
            res.append('void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;')
        res.extend(
            [
                'JSONMutableValue ToJSON(JSONWriter &writer) const;',
                '',
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
            self.add_member(property_name, self.generate_variable_type(item), item)

    def generate_any_of_items(self, items: List[Property]) -> None:
        if not items:
            return
        for item in items:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'
            uses_optional_wrapper = item.ref not in self.parse_info.recursive_schemas

            self.any_of.append(AnyOf(name=property_name, dereference_style=dereference_style, class_name=class_name))
            self.add_member(
                property_name,
                self.optional_member_type(item),
                item,
                copy_guard=self.presence_condition(property_name, uses_optional_wrapper),
                uses_optional_wrapper=uses_optional_wrapper,
            )

    def generate_any_of(self, property: Property):
        self.generate_any_of_items(property.any_of)

    def composition_items_are_primitive(self, items: List[Property]) -> bool:
        if not items:
            return False
        for item in items:
            if item.type != Property.Type.SCHEMA_REFERENCE:
                return False
            if self.parse_info.parsed_schemas[item.ref].type != Property.Type.PRIMITIVE:
                return False
        return True

    def generate_one_of(self, property: Property):
        if not property.one_of:
            return
        # Primitive Iceberg values have semantic alternatives that intentionally
        # overlap at the JSON-type level (for example int/long and many strings).
        # Preserve all matching views so the caller's Iceberg type can select one.
        if self.composition_items_are_primitive(property.one_of):
            self.generate_any_of_items(property.one_of)
            return

        discriminator_mapping = {}
        if property.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, property)
            if object_property.discriminator:
                self.discriminator_property = object_property.discriminator.get('propertyName')
                discriminator_mapping = {
                    mapped_ref.split('/')[-1]: value
                    for value, mapped_ref in object_property.discriminator.get('mapping', {}).items()
                }
        for item in property.one_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            self.referenced_schemas.add(item.ref)

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.parse_info.recursive_schemas else '.'
            uses_optional_wrapper = item.ref not in self.parse_info.recursive_schemas

            self.one_of.append(
                OneOf(
                    name=property_name,
                    dereference_style=dereference_style,
                    class_name=class_name,
                    discriminator_value=discriminator_mapping.get(class_name),
                )
            )
            self.add_member(
                property_name,
                self.optional_member_type(item),
                item,
                copy_guard=self.presence_condition(property_name, uses_optional_wrapper),
                uses_optional_wrapper=uses_optional_wrapper,
            )

    def generate_array_loop(
        self, array_name, destination_name, array_property: ArrayProperty, handle_nullable: bool = True
    ) -> List[str]:
        item_type = array_property.item_type
        item_name = f'{destination_name}_item'
        item_value_name = f'{destination_name}_item_val'
        body = []
        body.append(f'{array_name}.IterateArray([&](JSONValue {item_value_name}) {{')
        body.append('if (!error.empty()) {')
        body.append('return;')
        body.append('}')

        assignment = f'std::move({item_name})'
        if item_type.type != Property.Type.SCHEMA_REFERENCE:
            body.append(f'{self.generate_variable_type(item_type)} {item_name};')
            body.extend(self.make_callback_safe(self.generate_assignment(item_type, item_name, item_value_name, True)))
        else:
            schema_property = cast(SchemaReferenceProperty, item_type)
            self.referenced_schemas.add(schema_property.ref)
            if schema_property.ref in self.parse_info.recursive_schemas:
                item_pointer_name = f'{item_name}_p'
                body.extend(
                    [
                        f'auto {item_pointer_name} = make_uniq<{schema_property.ref}>();',
                        f'auto &{item_name} = *{item_pointer_name};',
                    ]
                )
                assignment = f'std::move({item_pointer_name})'
            else:
                body.append(f'{schema_property.ref} {item_name};')
            body.extend(
                self.make_callback_safe(
                    [
                        f'error = {item_name}.TryFromJSON({item_value_name});',
                        'if (!error.empty()) {',
                        'return error;',
                        '}',
                    ]
                )
            )
        body.append(f'{destination_name}.emplace_back({assignment});')
        body.append('});')
        body.append('if (!error.empty()) {')
        body.append('return error;')
        body.append('}')

        res = []
        prefix = ''
        if handle_nullable and array_property.nullable is not None:
            prefix = '} else '
            if array_property.nullable == True:
                res.extend([f'if ({array_name}.IsNull()) {{', '//! do nothing, property is explicitly nullable'])
            else:
                res.extend(
                    [
                        f'if ({array_name}.IsNull()) {{',
                        f'''return "{self.name} property '{destination_name}' is not nullable, but is 'null'";''',
                    ]
                )

        res.append(f'{prefix}if ({array_name}.IsArray()) {{')
        res.extend(body)
        res.extend(
            [
                '} else {',
                f"""return StringUtil::Format("{self.name} property '{destination_name}' is not of type 'array', found %s instead", json_utils::GetTypeDescription({array_name}).c_str());""",
                '}',
            ]
        )

        return res

    def generate_item_parse(
        self, property: Property, source: str, target: str, is_required: bool, handle_nullable: bool = True
    ) -> List[str]:
        res = []
        prefix = ''
        if handle_nullable and property.nullable is not None:
            prefix = '} else '
            if property.nullable == True:
                res.extend(
                    [
                        f'if ({source}.IsNull()) {{',
                        '//! do nothing, property is explicitly nullable',
                    ]
                )
            else:
                res.extend(
                    [
                        f'if ({source}.IsNull()) {{',
                        f'''return "{self.name} property '{target}' is not nullable, but is 'null'";''',
                    ]
                )

        if property.type == Property.Type.SCHEMA_REFERENCE:
            print(f"Unrecognized property type {property.type}, {source}")
            exit(1)
        if property.type == Property.Type.ARRAY:
            return self.generate_array_loop(
                source, target, cast(ArrayProperty, property), handle_nullable=handle_nullable
            )
        elif property.type == Property.Type.PRIMITIVE:
            # Validate the JSON type before extracting the value.
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
                        f'{target} = {specific_mapping.conversion}({source});',
                    ]
                )
                res.extend(
                    [
                        f'}} else if (json_utils::IsUnsignedInteger({source})) {{',
                        f'{target} = json_utils::GetUnsignedInteger({source});',
                    ]
                )
            else:
                res.extend(
                    [
                        f'{prefix}if ({generic_mapping.type_check}({source})) {{',
                        f'{target} = {generic_mapping.conversion}({source});',
                    ]
                )

            res.extend(
                [
                    '} else {',
                    f"""return StringUtil::Format("{self.name} property '{target}' is not of type '{item_type}', found %s instead", json_utils::GetTypeDescription({source}).c_str());""",
                    '}',
                ]
            )
            if primitive_property.const is not None:
                if isinstance(primitive_property.const, str):
                    const_literal = json.dumps(primitive_property.const)
                elif isinstance(primitive_property.const, bool):
                    const_literal = 'true' if primitive_property.const else 'false'
                else:
                    const_literal = str(primitive_property.const)
                res.extend(
                    [
                        f'if (!{source}.IsNull() && {target} != {const_literal}) {{',
                        f'''return "{self.name} property '{target}' does not match its required const value";''',
                        '}',
                    ]
                )
        elif property.type == Property.Type.OBJECT and property.is_raw_object():
            res.extend(
                [
                    f'{prefix}if ({source}.IsObject()) {{',
                    f'{target} = {source};',
                    '} else {',
                    f"""return "{self.name} property '{target}' is not of type 'object'";""",
                    '}',
                ]
            )
        elif property.type == Property.Type.OBJECT and property.additional_properties:
            object_property = cast(ObjectProperty, property)
            additional_properties = property.additional_properties

            res.append(f'{prefix}if ({source}.IsObject()) {{')
            res.append(f'{source}.IterateObject([&](const string &key_str, JSONValue val) {{')
            res.append('if (!error.empty()) {')
            res.append('return;')
            res.append('}')
            res.append(f'{self.generate_variable_type(additional_properties)} tmp;')

            if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
                item_definition = self.make_callback_safe(
                    self.generate_item_parse(additional_properties, 'val', 'tmp', True)
                )
                res.extend(item_definition)
            else:
                schema_property = cast(SchemaReferenceProperty, additional_properties)
                self.referenced_schemas.add(schema_property.ref)
                if schema_property.ref in self.parse_info.recursive_schemas:
                    print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                    exit(1)
                res.append(f'{schema_property.ref} tmp;')
                res.extend(
                    self.make_callback_safe(
                        [
                            'error = tmp.TryFromJSON(val);',
                            'if (!error.empty()) {',
                            'return error;',
                            '}',
                        ]
                    )
                )
            res.extend(
                [
                    f'{target}.emplace(key_str, std::move(tmp));',
                    '});',
                    'if (!error.empty()) {',
                    'return error;',
                    '}',
                ]
            )
            res.extend(['} else {', f"""return "{self.name} property '{target}' is not of type 'object'";""", '}'])
        else:
            print(f"Unrecognized type in 'generate_item_parse', {property.type}")
            exit(1)
        return res

    def generate_assignment(
        self, schema: Property, target: str, source: str, is_required: bool, handle_nullable: bool = True
    ) -> List[str]:
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            return self.generate_array_loop(source, target, array_property, handle_nullable=handle_nullable)
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
            return self.generate_item_parse(schema, source, target, is_required, handle_nullable=handle_nullable)

    def generate_optional_properties(self, name: str, properties: Dict[str, Property]):
        if not properties:
            return
        res = []
        for item, optional_property in properties.items():
            variable_name = safe_cpp_name(item)
            uses_optional_wrapper = self.uses_optional_wrapper(optional_property)
            body = self.generate_optional_assignment(optional_property, variable_name, f'{variable_name}_val')
            self.optional_properties[item] = OptionalProperty(
                property_name=item,
                variable_name=variable_name,
                body=body,
                schema=optional_property,
                nullable=optional_property.nullable,
                uses_optional_wrapper=uses_optional_wrapper,
            )
            variable_type = self.optional_member_type(optional_property)
            self.add_member(
                variable_name,
                variable_type,
                optional_property,
                copy_guard=self.presence_condition(variable_name, uses_optional_wrapper),
                uses_optional_wrapper=uses_optional_wrapper,
            )

    def generate_required_properties(self, name: str, properties: Dict[str, Property]):
        if not properties:
            return
        res = []
        for item, required_property in properties.items():
            variable_name = safe_cpp_name(item)
            is_nullable = required_property.nullable is True
            uses_optional_wrapper = is_nullable and self.uses_optional_wrapper(required_property)
            if is_nullable:
                body = self.generate_nullable_assignment(required_property, variable_name, f'{variable_name}_val')
            else:
                body = self.generate_assignment(required_property, variable_name, f'{variable_name}_val', True)
            if required_property.default is not None:
                default = [f'{variable_name} = "{str(required_property.default)}";']
            else:
                default = None
            self.required_properties[item] = RequiredProperty(
                property_name=item, variable_name=variable_name, body=body, default=default, schema=required_property
            )
            variable_type = (
                self.optional_member_type(required_property)
                if is_nullable
                else self.generate_variable_type(required_property)
            )
            self.add_member(
                variable_name,
                variable_type,
                required_property,
                copy_guard=(self.presence_condition(variable_name, uses_optional_wrapper) if is_nullable else None),
                uses_optional_wrapper=uses_optional_wrapper,
            )

    def generate_additional_properties(self, properties: List[str], additional_properties: Property):
        if not additional_properties:
            return

        skip_if_excluded = []
        exclude_list = []
        if properties:
            exclude_list = [
                'case_insensitive_set_t handled_properties {',
                f"""{', '.join(f'"{x}"' for x in properties)} }};""",
            ]
            skip_if_excluded = [
                'if (handled_properties.count(key_str)) {',
                'continue;',
                '}',
            ]

        body = []
        if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
            body.append(f'{self.generate_variable_type(additional_properties)} tmp;')
            body.extend(self.generate_item_parse(additional_properties, 'val', 'tmp', True))
        else:
            schema_property = cast(SchemaReferenceProperty, additional_properties)
            self.referenced_schemas.add(schema_property.ref)
            if schema_property.ref in self.parse_info.recursive_schemas:
                print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                exit(1)
            body.append(f'{schema_property.ref} tmp;')
            body.extend(
                [
                    'error = tmp.TryFromJSON(val);',
                    'if (!error.empty()) {',
                    'return error;',
                    '}',
                ]
            )
        self.additional_properties = AdditionalProperty(
            body=body, exclude_list=exclude_list, skip_if_excluded=skip_if_excluded, schema=additional_properties
        )
        variable_type = self.generate_variable_type(additional_properties)
        member_schema = ObjectProperty()
        member_schema.additional_properties = additional_properties
        if self.name in CASE_SENSITIVE_MAP_SCHEMAS:
            map_type = f'unordered_map<string, {variable_type}>'
        else:
            map_type = f'case_insensitive_map_t<{variable_type}>'
        self.add_member('additional_properties', map_type, member_schema)

    def generate_variable_type(self, schema: Property) -> str:
        if schema.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, schema)
            assert not object_property.properties
            if object_property.additional_properties:
                variable_type = self.generate_variable_type(object_property.additional_properties)
                return f'case_insensitive_map_t<{variable_type}>'
            return 'JSONValue'
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

    def schema_supports_json_object_population(
        self, schema: Optional[Property], visited: Optional[Set[str]] = None
    ) -> bool:
        if schema is None:
            return False

        if visited is None:
            visited = set()

        if schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            if schema_property.ref in visited:
                return True
            next_visited = set(visited)
            next_visited.add(schema_property.ref)
            return self.schema_supports_json_object_population(
                self.parse_info.parsed_schemas[schema_property.ref], next_visited
            )

        if schema.type != Property.Type.OBJECT:
            return False

        object_schema = cast(ObjectProperty, schema)
        has_object_content = (
            bool(object_schema.all_of)
            or bool(object_schema.properties)
            or object_schema.additional_properties is not None
        )

        if object_schema.one_of:
            return all(self.schema_supports_json_object_population(item, visited) for item in object_schema.one_of)
        if object_schema.any_of and not has_object_content:
            return all(self.schema_supports_json_object_population(item, visited) for item in object_schema.any_of)
        return True

    def supports_json_object_population(self) -> bool:
        return self.schema_supports_json_object_population(self.parse_info.parsed_schemas.get(self.name))

    def class_supports_json_object_population(self, class_name: str) -> bool:
        return self.schema_supports_json_object_population(self.parse_info.parsed_schemas[class_name])

    def variant_uses_optional_wrapper(self, class_name: str) -> bool:
        return class_name not in self.parse_info.recursive_schemas

    def variant_presence_condition(self, variant_name: str, class_name: str) -> str:
        return self.presence_condition(variant_name, self.variant_uses_optional_wrapper(class_name))

    def has_serializable_properties(self) -> bool:
        return bool(
            self.all_of
            or self.required_properties
            or self.optional_properties
            or (self.additional_properties and self.additional_properties.schema)
        )

    def generate_variant_chain(self, variants, body: Callable) -> List[str]:
        lines = []
        for index, variant in enumerate(variants):
            keyword = "if" if index == 0 else "else if"
            condition = self.variant_presence_condition(variant.name, variant.class_name)
            lines.append(f"{keyword} ({condition}) {{")
            lines.extend(body(variant))
            lines.append("}")
        return lines

    @staticmethod
    def generate_json_object_merge(source_expr: str) -> List[str]:
        return [
            f"(void){source_expr};",
            'throw InternalException("PopulateJSON requires an object-like JSON value");',
        ]

    def generate_populate_json_method(self, qualified_name: str) -> List[str]:
        lines = [f"void {qualified_name}::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {{"]

        variants = self.one_of or self.any_of
        if variants:

            def populate_variant(variant) -> List[str]:
                if self.class_supports_json_object_population(variant.class_name):
                    return [f"{variant.name}->PopulateJSON(writer, obj);"]
                return self.generate_json_object_merge(f"{variant.name}->ToJSON(writer)")

            lines.extend(self.generate_variant_chain(variants, populate_variant))
            if self.one_of or not self.has_serializable_properties():
                lines.append("}")
                return lines
            lines.append("")

        for base in self.all_of:
            lines.append(f"// Serialize base class: {base.class_name}")
            if self.class_supports_json_object_population(base.class_name):
                lines.append(f"{base.name}.PopulateJSON(writer, obj);")
            else:
                lines.extend(self.generate_json_object_merge(f"{base.name}.ToJSON(writer)"))
            lines.append("")

        for prop in self.required_properties.values():
            lines.extend(self.generate_property_serialization(prop, required=True))

        for prop in self.optional_properties.values():
            lines.extend(self.generate_property_serialization(prop, required=False))

        if self.additional_properties and self.additional_properties.schema:
            lines.extend(self.generate_additional_properties_serialization())

        lines.append("}")
        return lines

    def generate_to_json_method(self, qualified_name: str) -> List[str]:
        root_schema = self.parse_info.parsed_schemas[self.name]
        lines = [f"JSONMutableValue {qualified_name}::ToJSON(JSONWriter &writer) const {{"]

        if root_schema.type in (Property.Type.PRIMITIVE, Property.Type.ARRAY):
            lines.extend(self.serialize_json_value(root_schema, "value", "result"))
            lines.extend(["return result;", "}"])
            return lines

        if self.supports_json_object_population():
            lines.extend(
                [
                    "auto obj = writer.CreateObject();",
                    "PopulateJSON(writer, obj);",
                    "return obj;",
                    "}",
                ]
            )
            return lines

        if self.one_of:
            lines.extend(
                self.generate_variant_chain(
                    self.one_of,
                    lambda variant: [f"return {variant.name}->ToJSON(writer);"],
                )
            )
            lines.extend(
                [
                    "// No variant is active - return empty object",
                    "return writer.CreateObject();",
                    "}",
                ]
            )
            return lines

        variants = self.any_of
        any_of_is_primitive = variants and all(
            self.parse_info.parsed_schemas[variant.class_name].type == Property.Type.PRIMITIVE for variant in variants
        )
        if any_of_is_primitive:
            variants = sorted(variants, key=self.primitive_variant_priority, reverse=True)

        if variants and not self.has_serializable_properties():
            lines.extend(
                self.generate_variant_chain(
                    variants,
                    lambda variant: [f"return {variant.name}->ToJSON(writer);"],
                )
            )
            lines.append(
                "// No variant is active - return null"
                if any_of_is_primitive
                else "// No variant is active - return empty object"
            )
            lines.append("return writer.CreateNull();" if any_of_is_primitive else "return writer.CreateObject();")
            lines.append("}")
            return lines

        lines.extend(
            [
                'throw InternalException("ToJSON should use PopulateJSON for object-like schemas");',
                "}",
            ]
        )
        return lines

    def primitive_variant_priority(self, variant: AnyOf) -> int:
        schema = cast(PrimitiveProperty, self.parse_info.parsed_schemas[variant.class_name])
        if schema.primitive_type == "integer":
            return 2 if schema.format == "int64" else 1
        if schema.primitive_type == "number":
            return 2 if schema.format == "double" else 1
        return 0

    @staticmethod
    def primitive_json_expression(prop: PrimitiveProperty, value_expr: str) -> str:
        constructors = {
            None: "CreateNull",
            "string": "CreateString",
            "integer": "CreateSignedInteger",
            "boolean": "CreateBoolean",
            "number": "CreateDouble",
        }
        constructor = constructors.get(prop.primitive_type)
        if constructor is None:
            raise ValueError(f"Unsupported primitive serialization type: {prop.primitive_type}")
        argument = "" if prop.primitive_type is None else value_expr
        return f"writer.{constructor}({argument})"

    def schema_reference_json_expression(self, prop: SchemaReferenceProperty, value_expr: str) -> str:
        operator = "->" if prop.ref in self.parse_info.recursive_schemas else "."
        return f"{value_expr}{operator}ToJSON(writer)"

    def serialize_json_value(
        self,
        schema: Property,
        value_expr: str,
        result_name: str,
    ) -> List[str]:
        if schema.type == Property.Type.PRIMITIVE:
            expression = self.primitive_json_expression(cast(PrimitiveProperty, schema), value_expr)
            return [f"auto {result_name} = {expression};"]

        if schema.type == Property.Type.SCHEMA_REFERENCE:
            expression = self.schema_reference_json_expression(cast(SchemaReferenceProperty, schema), value_expr)
            return [f"auto {result_name} = {expression};"]

        if schema.type == Property.Type.ARRAY:
            array_schema = cast(ArrayProperty, schema)
            item_name = f"{result_name}_item"
            item_json_name = f"{item_name}_json"
            lines = [
                f"auto {result_name} = writer.CreateArray();",
                f"for (const auto &{item_name} : {value_expr}) {{",
            ]
            lines.extend(self.serialize_json_value(array_schema.item_type, item_name, item_json_name))
            lines.extend([f"{result_name}.Append({item_json_name});", "}"])
            return lines

        if schema.type == Property.Type.OBJECT:
            object_schema = cast(ObjectProperty, schema)
            if object_schema.is_raw_object():
                return [f"auto {result_name} = writer.CreateCopy({value_expr});"]

            lines = [f"auto {result_name} = writer.CreateObject();"]
            if object_schema.additional_properties:
                key_name = f"{result_name}_key"
                value_name = f"{result_name}_value"
                value_json_name = f"{value_name}_json"
                lines.append(f"for (const auto &[{key_name}, {value_name}] : {value_expr}) {{")
                lines.extend(
                    self.serialize_json_value(
                        object_schema.additional_properties,
                        value_name,
                        value_json_name,
                    )
                )
                lines.extend([f"{result_name}.Add({key_name}, {value_json_name});", "}"])
                return lines

            for json_name, property_schema in object_schema.properties.items():
                member_name = safe_cpp_name(json_name)
                property_json_name = f"{result_name}_{member_name}"
                lines.extend(
                    self.serialize_json_value(
                        property_schema,
                        f"{value_expr}.{member_name}",
                        property_json_name,
                    )
                )
                lines.append(f'{result_name}.Add("{json_name}", {property_json_name});')
            return lines

        raise ValueError(f"Unsupported serialization type: {schema.type}")

    def generate_property_serialization(self, prop, required: bool) -> List[str]:
        var_name = prop.variable_name
        json_name = prop.property_name
        schema = prop.schema
        lines = [f"// Serialize: {json_name}"]

        needs_presence_check = not required or schema.nullable is True
        if needs_presence_check:
            uses_optional_wrapper = self.uses_optional_wrapper(schema)
            condition = self.presence_condition(var_name, uses_optional_wrapper)
            lines.append(f"if ({condition}) {{")
            value_expr = var_name
            if uses_optional_wrapper:
                value_expr = f"{var_name}_value"
                lines.append(f"auto &{value_expr} = *{var_name};")
            result_name = f"{var_name}_json"
            lines.extend(self.serialize_json_value(schema, value_expr, result_name))
            lines.extend([f'obj.Add("{json_name}", {result_name});', "}"])
            if required:
                lines.extend(
                    [
                        "else {",
                        f'obj.Add("{json_name}", writer.CreateNull());',
                        "}",
                    ]
                )
        else:
            result_name = f"{var_name}_json"
            lines.extend(self.serialize_json_value(schema, var_name, result_name))
            lines.append(f'obj.Add("{json_name}", {result_name});')

        lines.append("")
        return lines

    def generate_additional_properties_serialization(self) -> List[str]:
        schema = self.additional_properties.schema
        lines = [
            "// Serialize additional properties",
            "for (const auto &[key, value] : additional_properties) {",
        ]
        lines.extend(self.serialize_json_value(schema, "value", "value_json"))
        lines.extend(["obj.Add(key, value_json);", "}", ""])
        return lines


if __name__ == '__main__':
    openapi_parser = ResponseObjectsGenerator(API_SPEC_PATH)
    openapi_parser.parse_all_schemas()

    # Create directory if it doesn't exist
    os.makedirs(OUTPUT_HEADER_DIR, exist_ok=True)
    os.makedirs(OUTPUT_SOURCE_DIR, exist_ok=True)

    with open(os.path.join(OUTPUT_HEADER_DIR, 'json_utils.hpp'), 'w') as f:
        f.write(JSON_UTILS_HEADER_FORMAT.format())

    with open(os.path.join(OUTPUT_HEADER_DIR, 'list.hpp'), 'w') as f:
        lines = ["", "// This file is automatically generated and contains all REST API object headers", ""]
        # Add includes for all generated headers
        for name in openapi_parser.schemas:
            lines.append(f'#include "rest_catalog/objects/{to_snake_case(name)}.hpp"')
        f.write('\n'.join(lines))

    with open(os.path.join(OUTPUT_SOURCE_DIR, 'CMakeLists.txt'), 'w') as f:
        file_paths = []
        for name in openapi_parser.schemas:
            file_paths.append(f'{to_snake_case(name)}.cpp')
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
            if name in CASE_SENSITIVE_MAP_SCHEMAS:
                additional_headers.insert(0, '#include "duckdb/common/unordered_map.hpp"')
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
