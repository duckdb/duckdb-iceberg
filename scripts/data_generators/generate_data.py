from generate_spark_local.generate_iceberg_spark_local import IcebergSparkLocal
from generate_spark_rest.generate_iceberg_spark_rest import IcebergSparkRest
from generate_polaris_rest.generate_iceberg_polaris_rest import IcebergPolarisRest
from generate_lakekeeper_rest.generate_iceberg_lakekeeper_rest import IcebergLakekeeperRest
import sys


def GenerateSparkRest():
    db2 = IcebergSparkRest()
    conn2 = db2.GetConnection()
    db2.GenerateTables(conn2)
    db2.CloseConnection(conn2)
    del db2
    del conn2


def GenerateSparkLocal():
    db = IcebergSparkLocal()
    conn = db.GetConnection()
    db.GenerateTables(conn)
    db.CloseConnection(conn)
    del db
    del conn


def GeneratePolarisData():
    db = IcebergPolarisRest()
    conn = db.GetConnection()
    db.GenerateTables(conn)
    db.CloseConnection(conn)
    del db
    del conn


def GenerateLakekeeperData():
    db = IcebergLakekeeperRest()
    conn = db.GetConnection()
    db.GenerateTables(conn)
    db.CloseConnection(conn)
    del db
    del conn


if __name__ == "__main__":
    argv = sys.argv
    for i in range(1, len(argv)):
        if argv[i] == "polaris":
            print("generating polaris data")
            GeneratePolarisData()
        elif argv[i] == "lakekeeper":
            print("generating lakekeeper data")
            GenerateLakekeeperData()
        elif argv[i] == "local":
            print("generating local iceberg data")
            GenerateSparkLocal()
        elif argv[i] == "spark-rest":
            print("generating local iceberg REST data")
            GenerateSparkRest()
        else:
            print(f"{argv[i]} not recognized, skipping")
