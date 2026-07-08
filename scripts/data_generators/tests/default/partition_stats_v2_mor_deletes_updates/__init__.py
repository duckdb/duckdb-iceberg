from scripts.data_generators.tests.base import IcebergTest


@IcebergTest.register()
class Test(IcebergTest):
    required_runtime_capabilities = {"compute_partition_stats"}

    def __init__(self):
        super().__init__(__file__)
