# Local changes to `api.yaml`

`api.yaml` is vendored from the Apache Iceberg REST Catalog OpenAPI
specification. After replacing it with a newer upstream version, check whether
upstream contains the corrections below and reinstate any that are still
missing before regenerating the C++ REST objects.

## Primitive null values

Iceberg primitive values can be JSON `null`. Add a schema for that value:

```yaml
NullTypeValue:
  type: null
  nullable: true
  example: null
```

Add it as the first variant of `PrimitiveTypeValue.oneOf`:

```yaml
- $ref: '#/components/schemas/NullTypeValue'
```

Without this variant, generated expression and metrics objects cannot parse a
null primitive value.

## Boolean constant expressions

Iceberg's canonical JSON expression representation encodes `alwaysTrue` and
`alwaysFalse` as the JSON booleans `true` and `false`. Add a schema for this
representation:

```yaml
BooleanExpression:
  type: boolean
```

Add it as the first variant of `Expression.oneOf`:

```yaml
- $ref: '#/components/schemas/BooleanExpression'
```

Keep the vendored `TrueExpression` and `FalseExpression` object variants as
well, so the generated parser remains compatible with both representations.
This correction is required for scan responses such as
`"residual-filter": true`.

## Regeneration

After refreshing and correcting `api.yaml`, regenerate and format the REST
objects, then run the generator tests:

```shell
python3 scripts/generate_cpp_code.py
make format-fix
python3 -m pytest -q test/scripts/test_openapi_codegen.py
```
