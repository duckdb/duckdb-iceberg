# Local changes to `api.yaml`

After updating `api.yaml` from upstream, reapply any changes below that are
still missing.

## Primitive null values

Add `NullTypeValue`:

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

## Deprecated V1 table metadata

Keep the deprecated `TableMetadata.schema`, `TableMetadata.partition-spec`,
and `Snapshot.manifests` fields. Make `Snapshot.manifest-list` optional.

## Regeneration

```shell
make generate-rest-catalog-code
python3 -m pytest -q test/scripts/test_openapi_codegen.py
```
