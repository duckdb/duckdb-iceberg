from scripts.data_generators.tests.base import IcebergTest
import pathlib


POLARIS_SKIP_REASON = (
    "Polaris requires partition and sort updates to be applied as delete-and-insert operations, "
    "which Spark does not support for this generator"
)


@IcebergTest.register()
class Test(IcebergTest):
    catalog_mapping = {
        "fixture": "fixture-single-thread",
        "fixture-latest": "fixture-single-thread",
        "gravitino": "fixture-single-thread",
        "nessie": "fixture-single-thread"
    }
    skips = {"polaris": POLARIS_SKIP_REASON}

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(__file__)
