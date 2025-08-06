import yaml
import os
from typing import Dict, List, Set, Optional, cast
from enum import Enum, auto

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
API_SPEC_PATH = os.path.join(SCRIPT_PATH, 'api.yaml')

PRIMITIVE_TYPES = ['string', 'number', 'integer', 'boolean']


class Property:
    class Type(Enum):
        PRIMITIVE = auto()
        ARRAY = auto()
        OBJECT = auto()
        SCHEMA_REFERENCE = auto()

    def __init__(self, type: "Property.Type"):
        self.type = type
        self.all_of: List[Property] = []
        self.any_of: List[Property] = []
        self.one_of: List[Property] = []
        self.nullable: Optional[bool] = None

    def is_string(self):
        if self.type != Property.Type.PRIMITIVE:
            return False
        primitive_property = cast(PrimitiveProperty, self)
        return primitive_property.primitive_type == 'string'


class SchemaReferenceProperty(Property):
    def __init__(self, name):
        super().__init__(Property.Type.SCHEMA_REFERENCE)
        self.ref = name


class ArrayProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.ARRAY)
        self.item_type: Optional[Property] = None


class PrimitiveProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.PRIMITIVE)
        self.primitive_type: Optional[str] = None
        self.format = None
        # TODO: if 'enum' is present, we should verify that the value of the property is one of the accepted values
        self.enum: Optional[List[str]] = None
        # TODO: same for this, this property *has* to have this value
        self.const: Optional[str] = None


class ObjectProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.OBJECT)
        self.required = []
        self.properties: Dict[str, Property] = {}
        self.additional_properties: Optional[Property] = None
        # TODO: do we need this? the schema validation shouldn't need it
        self.discriminator = None

    def is_object_of_strings(self):
        if self.properties:
            return False
        if not self.additional_properties:
            return False
        if self.additional_properties.type != Property.Type.PRIMITIVE:
            return False
        primitive_property = cast(PrimitiveProperty, self.additional_properties)
        return primitive_property.primitive_type == 'string'

    def is_raw_object(self):
        if self.properties:
            return False
        if self.additional_properties:
            return False
        if self.any_of:
            return False
        if self.all_of:
            return False
        if self.one_of:
            return False
        return True


class ResponseObjectsGenerator:
    def __init__(self, path: str):
        self.path = path
        self.parsed_schemas: Dict[str, Property] = {}
        self.parsed_responses: Dict[str, Response] = {}
        # Since schemas reference other schemas and are potentially recursive
        # We want to keep track of the schemas that are currently being parsed
        self.schemas_being_parsed: Set[str] = set()
        # Whenever this schema is referenced, the instance has to be wrapped in a unique_ptr
        # otherwise the constructor will either be an infinite recursion
        # or it won't compile (hopefully this)
        self.recursive_schemas: Set[str] = set()
        self.object_schema_count = 0

        # Load OpenAPI spec
        with open(API_SPEC_PATH) as f:
            spec = yaml.safe_load(f)

        self.schemas = spec['components']['schemas']
        self.responses = spec['components']['responses']

    def parse_object_property(self, spec: dict, result: Property) -> None:
        # For polymorphic types, this defines a mapping based on the content of a property
        discriminator = spec.get('discriminator')
        # Get the required properties of the schema
        required = spec.get('required')
        # Get the defined properties of the schema
        properties = spec.get('properties', {})
        # Get the type for any additional undefined properties
        additional_properties = spec.get('additionalProperties')

        assert result.type == Property.Type.OBJECT
        object_result = cast(ObjectProperty, result)

        if additional_properties:
            object_result.additional_properties = self.parse_property(additional_properties)

        object_result.required = required
        for name in properties:
            property_spec = properties[name]
            object_result.properties[name] = self.parse_property(property_spec)

    def parse_primitive_property(self, spec: dict, result: Property) -> None:
        primitive_type = spec['type']
        format = spec.get('format')
        assert primitive_type in PRIMITIVE_TYPES
        assert result.type == Property.Type.PRIMITIVE
        primitive_result = cast(PrimitiveProperty, result)
        primitive_result.format = format
        primitive_result.primitive_type = primitive_type

    def parse_array_property(self, spec: dict, result: Property) -> None:
        item_type = spec['items']
        assert result.type == Property.Type.ARRAY
        array_result = cast(ArrayProperty, result)
        array_result.item_type = self.parse_property(item_type)

    def parse_property(self, spec: dict, reference: Optional[str] = None) -> Property:
        ref = spec.get('$ref')
        if not reference:
            if ref:
                parts = ref.split('/')
                assert parts[-2] == 'schemas'
                reference = parts[-1]
                self.parse_schema(reference)
                return SchemaReferenceProperty(reference)
        elif ref:
            print(f"Schema {reference} spec contains '$ref' ???")
            exit(1)

        # default to 'object' (see 'AssertViewUUID')
        property_type = spec.get('type', 'object')
        nullable = spec.get('nullable', None)

        one_of = spec.get('oneOf')
        all_of = spec.get('allOf')
        any_of = spec.get('anyOf')

        if property_type == 'object':
            result = ObjectProperty()
            self.parse_object_property(spec, result)
            # this can be removed when https://github.com/apache/iceberg/pull/13624 is resolved
            if reference == 'LoadTableResult':
                result.properties['metadata-location'].nullable = True
        elif property_type == 'array' or property_type == 'list':
            result = ArrayProperty()
            self.parse_array_property(spec, result)
        elif property_type in PRIMITIVE_TYPES:
            result = PrimitiveProperty()
            self.parse_primitive_property(spec, result)
        else:
            print(f"Property has unrecognized type: '{property_type}'!")
            exit(1)

        result.nullable = nullable

        if one_of:
            if property_type != 'object':
                print(f"Property contains both 'oneOf' and a non-object 'type' ({property_type})")
                exit(1)
            assert 'allOf' not in spec
            assert 'anyOf' not in spec
            for item in one_of:
                res = self.parse_property(item)
                result.one_of.append(res)
        if all_of:
            if property_type != 'object':
                print(f"Property contains both 'allOf' and a non-object 'type' ({property_type})")
                exit(1)
            assert 'oneOf' not in spec
            assert 'anyOf' not in spec
            for item in all_of:
                res = self.parse_property(item)
                result.all_of.append(res)
        if any_of:
            if property_type != 'object':
                print(f"Property contains both 'allOf' and a non-object 'type' ({property_type})")
                exit(1)
            assert 'allOf' not in spec
            assert 'oneOf' not in spec
            for item in any_of:
                res = self.parse_property(item)
                result.any_of.append(res)

        if (
            not reference
            and result.type == Property.Type.OBJECT
            and not result.is_object_of_strings()
            and not result.is_raw_object()
        ):
            if (
                not result.one_of
                and not result.any_of
                and len(result.all_of) == 1
                and result.all_of[0].type == Property.Type.SCHEMA_REFERENCE
            ):
                # Optimizer: object that consists of a single 'allOf' can be simplified to just that single reference
                return SchemaReferenceProperty(result.all_of[0].ref)
            self.object_schema_count += 1
            new_name = f'Object{self.object_schema_count}'
            self.parsed_schemas[new_name] = result
            # print("CUSTOM SCHEMA", new_name, spec)
            return SchemaReferenceProperty(new_name)
        return result

    def parse_schema(self, name: str) -> None:
        if name in self.parsed_schemas:
            return
        if name in self.schemas_being_parsed:
            self.recursive_schemas.add(name)
            return
        if name not in self.schemas:
            print(f"{name} is not a schema in the spec!")
            exit(1)

        self.schemas_being_parsed.add(name)
        schema = self.schemas[name]

        result = self.parse_property(schema, name)
        result.reference = name

        self.schemas_being_parsed.remove(name)
        self.parsed_schemas[name] = result

    def parse_all_schemas(self):
        for name in self.schemas:
            self.parse_schema(name)


if __name__ == '__main__':
    generator = ResponseObjectsGenerator(API_SPEC_PATH)
    generator.parse_all_schemas()

    schema = generator.parsed_schemas['DataFile']
    exit(1)
