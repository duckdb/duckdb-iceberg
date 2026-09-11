from scripts.data_generators.tests.base import IcebergTest


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        super().__init__(__file__)
