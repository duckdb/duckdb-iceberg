from scripts.data_generators.tests.base import IcebergTest
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)

    def get_connection(self, catalog: str, *, target: str | None = None, **kwargs):
        # Example: tweak behavior for a specific catalog
        target = "spark-rest-single-thread"
        if catalog == "spark-rest":
            from scripts.data_generators.connections import IcebergConnection
            # Use base mapping unless explicit target is provided
            registry_key = self.resolve_target_for_catalog(catalog, target)
            cls = IcebergConnection.get_class(registry_key)
            con = cls(**kwargs) if kwargs else cls()
            return con
        # Fallback to default behavior for other catalogs
        return super().get_connection(catalog, target=target, **kwargs)
