from scripts.data_generators.tests.base import IcebergTest
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(table='nested_namespaces', namespace=['level1', 'level2', 'level3'], write_intermediates=False)
