from pathlib import Path

import pytest

from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


SEED_TABLE = SparkSeedTable(
    "default.filtering_on_partition_bounds",
    """
CREATE or REPLACE TABLE default.filtering_on_partition_bounds (
	seq integer,
	col1 integer
)
USING ICEBERG
PARTITIONED BY (seq)
TBLPROPERTIES (
    'format-version'='2',
    'write.update.mode'='merge-on-read'
);

INSERT INTO default.filtering_on_partition_bounds
SELECT 1 as seq, id AS col1 FROM range(0, 1000);

INSERT INTO default.filtering_on_partition_bounds
SELECT 2 as seq, id AS col1 FROM range(1000, 2000);

INSERT INTO default.filtering_on_partition_bounds
SELECT 3 as seq, id AS col1 FROM range(2000, 3000);

INSERT INTO default.filtering_on_partition_bounds
SELECT 4 as seq, id AS col1 FROM range(3000, 4000);

INSERT INTO default.filtering_on_partition_bounds
SELECT 5 as seq, id AS col1 FROM range(4000, 5000);
""",
)


@pytest.mark.spark_seed_tables(SEED_TABLE)
@pytest.mark.generator_catalog("local")
def test_filtering_on_partition_bounds(
    unittest_binary,
    print_unittest_stdin,
):
    with DuckDBUnittestRunner(
        unittest_binary,
        print_stdin=print_unittest_stdin,
        initialize=False,
    ) as runner:
        runner.run_sqllogic_file(Path(__file__).with_name("filtering_on_partition_bounds.test"))
