from scripts.data_generators.tests.base import IcebergTest
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    catalog_mapping = {"spark-rest": "spark-rest-single-thread"}

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(__file__)
