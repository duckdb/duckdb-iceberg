from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext

import sys
import os
import glob

DATA_GENERATION_DIR = 'data/persistent'
SCRIPT_DIR = os.path.dirname(__file__)
SPARK_RUNTIME_PATH = os.path.join(SCRIPT_DIR, '..', 'data_generators', 'iceberg-spark-runtime-3.5_2.12-1.9.0.jar')

conf = pyspark.SparkConf()
conf.setMaster('local[*]')
conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DATA_GENERATION_DIR)
conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
conf.set('spark.driver.memory', '10g')
conf.set('spark.jars', SPARK_RUNTIME_PATH)
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.sql("USE iceberg_catalog")

# Create bucket partitioned table
spark.sql(
    """
CREATE OR REPLACE TABLE partition_bucket_test (
    id BIGINT,
    name STRING,
    partition_col STRING
)
USING iceberg
PARTITIONED BY (bucket(10, partition_col))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read'
);
"""
)

values_list = [f"({i}, 'user_{i}', 'partition_{i % 10}')" for i in range(100)]
values_str = ",\n  ".join(values_list)

spark.sql(f"""
INSERT INTO partition_bucket_test VALUES
  {values_str}
""")

print("✅ Bucket partitioned table created successfully!")
print(f"📁 Data location: {DATA_GENERATION_DIR}/partition_bucket_test")
print(f"📊 Total rows: 100")
print(f"🗂️  Buckets: 10")
print(f"📝 Each bucket should have ~10 rows")

# Verify the data distribution
print("\n📈 Data distribution verification:")
result = spark.sql("""
    SELECT partition_col, COUNT(*) as count
    FROM partition_bucket_test
    GROUP BY partition_col
    ORDER BY partition_col
""")
result.show()

# Show sample data from each partition
print("\n🔍 Sample data from partition_0:")
spark.sql("""
    SELECT * FROM partition_bucket_test
    WHERE partition_col = 'partition_0'
    LIMIT 5
""").show()
