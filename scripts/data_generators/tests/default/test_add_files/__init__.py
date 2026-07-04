from scripts.data_generators.tests.base import IcebergTest
import pathlib
import duckdb

SCRIPTS_DIR = pathlib.Path(__file__).resolve().parents[4]
FILES_DIR = SCRIPTS_DIR / "data" / "generated" / "files"

def generate_files():
    n_rows = 100_000

    FILES_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("select setseed(0.42)")

    # Generate synthetic data inside DuckDB
    con.execute(f"""
        CREATE OR REPLACE TABLE fake_data AS
        SELECT
            i AS id,
            'item_' || i AS name,
            CASE floor(random() * 3)
                WHEN 0 THEN 'A'
                WHEN 1 THEN 'B'
                ELSE 'C'
            END AS category,
            100 + 15 * random() AS value,
            CAST(floor(random() * 99) + 1 AS INTEGER) AS count,
            random() < 0.5 AS active,
            substr(md5(random()::VARCHAR), 1, 24) AS description
        FROM range(1, {n_rows + 1}) t(i)
    """)

    # Get categories
    categories = [row[0] for row in con.execute(
        "SELECT DISTINCT category FROM fake_data"
    ).fetchall()]

    # Write one parquet file per category
    for i, category in enumerate(categories):
        file_path = FILES_DIR / f"file_{i}.parquet"
        con.execute(f"""
            COPY (
                SELECT * FROM fake_data
                WHERE category = '{category}'
            )
            TO '{file_path}'
            (FORMAT PARQUET)
        """)

@IcebergTest.register()
class Test(IcebergTest):
    supported_catalogs = {"local"}

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(__file__)

    def generate(self, con):
        generate_files()

        con.con.sql("CREATE NAMESPACE IF NOT EXISTS test_add_files")
        try:
            con.con.sql("DROP TABLE test_add_files.fake_data_test")
        except Exception:
            pass

        table_schema = """
        id bigint,
        name string,
        category string,
        value double,
        count bigint,
        active boolean,
        description string
        """

        # ----------------------------------------------------
        # add_files
        # ----------------------------------------------------

        con.con.sql(f"""
        CREATE OR REPLACE TABLE test_add_files.fake_data_test ({table_schema})
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """)

        con.con.sql(f"""
        CALL iceberg_catalog.system.add_files(
        table => 'test_add_files.fake_data_test',
        source_table => 'parquet.`{FILES_DIR}`'
        )
        """)
