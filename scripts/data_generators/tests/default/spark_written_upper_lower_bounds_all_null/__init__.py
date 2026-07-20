from scripts.data_generators.tests.base import IcebergTest
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    catalog_mapping = {
        "fixture": "fixture-single-thread",
        "fixture-latest": "fixture-single-thread",
        "gravitino": "fixture-single-thread",
        "nessie": "fixture-single-thread"
    }

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(__file__)
