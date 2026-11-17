from scripts.data_generators.tests.base import IcebergTest


@IcebergTest.register()
class Test(IcebergTest):
    expected_failures = {
        "nessie": "Nessie creates the table as V2, which has no _row_id"
    }

    def __init__(self):
        super().__init__(__file__)
