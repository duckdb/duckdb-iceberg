from scripts.data_generators.tests.base import IcebergTest
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    """Bucket-partitioned UUID table.

    Spark SQL has no UUID type, so the table cannot be created with `CREATE TABLE ... (val UUID)`.
    Instead the table is created through the Iceberg Java API (reached through the Spark catalog),
    after which Spark can insert into it using the string representation of the UUIDs.
    """

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(__file__)

    def setup(self, con):
        spark = con.con
        jvm = spark.sparkContext._jvm
        gateway = spark.sparkContext._gateway

        types = jvm.org.apache.iceberg.types.Types
        fields = gateway.new_array(types.NestedField, 2)
        fields[0] = types.NestedField.required(1, "id", types.IntegerType.get())
        fields[1] = types.NestedField.optional(2, "val", types.UUIDType.get())
        schema = jvm.org.apache.iceberg.Schema(fields)
        spec = jvm.org.apache.iceberg.PartitionSpec.builderFor(schema).bucket("val", 32).build()

        spark_catalog = spark._jsparkSession.sessionState().catalogManager().catalog(con.catalog)
        iceberg_catalog = spark_catalog.icebergCatalog()
        identifier = jvm.org.apache.iceberg.catalog.TableIdentifier.parse(self.qualified_name)

        properties = jvm.java.util.HashMap()
        properties.put("format-version", "2")
        iceberg_catalog.createTable(identifier, schema, spec, properties)
