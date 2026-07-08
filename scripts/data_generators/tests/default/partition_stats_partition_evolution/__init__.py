from scripts.data_generators.tests.base import IcebergTest


POLARIS_SKIP_REASON = (
    "Polaris requires partition and sort updates to be applied as delete-and-insert operations, "
    "which Spark does not support for this generator"
)


@IcebergTest.register()
class Test(IcebergTest):
    catalog_mapping = {
        "fixture": "fixture-single-thread",
        "nessie": "fixture-single-thread",
    }
    skips = {"polaris": POLARIS_SKIP_REASON}
    required_runtime_capabilities = {"compute_partition_stats"}

    def __init__(self):
        super().__init__(__file__)
