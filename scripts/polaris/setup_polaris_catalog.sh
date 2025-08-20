# TODO: use the python module to execute these steps.
# Seems like the python module is not yet publicly available/installable, so unsure what to do about this.

./polaris \
  --client-id root \
  --client-secret secret \
  catalogs \
  create \
  --storage-type FILE \
  --default-base-location file://${PWD}/storage_files \
  quickstart_catalog

./polaris \
  --client-id root \
  --client-secret secret \
  principals \
  create \
  quickstart_user

./polaris \
  --client-id root \
  --client-secret secret \
  principal-roles \
  create \
  quickstart_user_role

./polaris \
  --client-id root \
  --client-secret secret \
  catalog-roles \
  create \
  --catalog quickstart_catalog \
  quickstart_catalog_role

./polaris \
  --client-id root \
  --client-secret secret \
  principal-roles \
  grant \
  --principal quickstart_user \
  quickstart_user_role

./polaris \
  --client-id root \
  --client-secret secret \
  catalog-roles \
  grant \
  --catalog quickstart_catalog \
  --principal-role quickstart_user_role \
  quickstart_catalog_role

./polaris \
  --client-id root \
  --client-secret secret \
  privileges \
  catalog \
  grant \
  --catalog quickstart_catalog \
  --catalog-role quickstart_catalog_role \
  CATALOG_MANAGE_CONTENT
