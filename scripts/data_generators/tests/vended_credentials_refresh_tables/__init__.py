from scripts.data_generators.tests.base import IcebergTest
from typing import Optional


REFRESH_METADATA_TABLES = [
    "vended_init_refresh",
    "vended_range_refresh",
    "vended_head_refresh",
    "vended_get_refresh",
    "vended_put_refresh",
    "vended_post_refresh",
    "vended_list_refresh",
    "vended_delete_refresh",
    "vended_mixed_delete_refresh_a",
    "vended_mixed_delete_refresh_b",
    "vended_same_bad_refresh",
    "vended_range_fail_refresh",
]


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        super().__init__("vended_credentials_refresh_tables", write_intermediates=False)

    def generate(self, catalog: str, *, target: Optional[str] = None, connection_kwargs: Optional[dict] = None):
        con = self.get_connection(catalog, target=target, **(connection_kwargs or {}))
        try:
            con.con.sql(
                """
                CREATE OR REPLACE TABLE default.vended_refresh_table (
                    dt date,
                    number integer,
                    letter string
                )
                USING iceberg
                """
            )
            con.con.sql(
                """
                INSERT INTO default.vended_refresh_table
                VALUES
                    (CAST('2023-03-01' AS date), 1, 'a'),
                    (CAST('2023-03-02' AS date), 2, 'b'),
                    (CAST('2023-03-03' AS date), 3, 'c'),
                    (CAST('2023-03-04' AS date), 4, 'd'),
                    (CAST('2023-03-05' AS date), 5, 'e'),
                    (CAST('2023-03-06' AS date), 6, 'f'),
                    (CAST('2023-03-07' AS date), 7, 'g'),
                    (CAST('2023-03-08' AS date), 8, 'h'),
                    (CAST('2023-03-09' AS date), 9, 'i'),
                    (CAST('2023-03-10' AS date), 10, 'j'),
                    (CAST('2023-03-11' AS date), 11, 'k'),
                    (CAST('2023-03-12' AS date), 12, 'l')
                """
            )

            for table in REFRESH_METADATA_TABLES:
                con.con.sql(
                    f"""
                    CREATE OR REPLACE TABLE default.{table} (
                        id integer
                    )
                    USING iceberg
                    """
                )
        finally:
            self.close_connection(con)
