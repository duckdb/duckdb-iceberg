from scripts.data_generators.tests.base import IcebergTest


@IcebergTest.register()
class Test(IcebergTest):
    expected_failures = {
        "nessie": "format-version update not supported yet"
    }

    def __init__(self):
        super().__init__(__file__)
