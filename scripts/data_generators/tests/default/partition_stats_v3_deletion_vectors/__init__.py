from scripts.data_generators.tests.base import IcebergTest


@IcebergTest.register()
class Test(IcebergTest):
    required_runtime_capabilities = {"compute_partition_stats", "format_v3"}
    supported_catalogs = {"local", "fixture", "lakekeeper", "polaris"}

    def __init__(self):
        super().__init__(__file__)
