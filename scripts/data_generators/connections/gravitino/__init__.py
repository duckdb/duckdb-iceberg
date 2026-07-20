from scripts.data_generators.connections.base import IcebergConnection
from scripts.data_generators.connections.spark_rest import IcebergSparkRest


CONNECTION_KEY = "gravitino"


@IcebergConnection.register(CONNECTION_KEY)
class IcebergSparkGravitino(IcebergSparkRest):
    connection_key = CONNECTION_KEY
    default_endpoint = "http://127.0.0.1:9001/iceberg"
    default_warehouse = ""
