from uuid import uuid4

import pytest

from conftest import is_active_catalog

pa = pytest.importorskip("pyarrow")


@pytest.mark.skipif(not is_active_catalog("lakekeeper"), reason="Lakekeeper snapshot-reference regression")
def test_removing_last_snapshot_ref_survives_reload(rest_catalog):
    rest_catalog.create_namespace_if_not_exists("default")
    identifier = ("default", "remove_last_ref_" + uuid4().hex)
    data = pa.table({"id": [42]})
    table = rest_catalog.create_table(identifier, schema=data.schema, properties={"format-version": "2"})
    try:
        table.append(data)
        snapshot_id = table.current_snapshot().snapshot_id
        assert set(table.refs()) == {"main"}

        with table.manage_snapshots() as snapshots:
            snapshots.remove_branch("main")

        reloaded = rest_catalog.load_table(identifier)
        assert reloaded.refs() == {}
        assert reloaded.current_snapshot() is None
        assert [snapshot.snapshot_id for snapshot in reloaded.snapshots()] == [snapshot_id]
        assert reloaded.scan().to_arrow().num_rows == 0
        assert reloaded.scan(snapshot_id=snapshot_id).to_arrow().to_pylist() == [{"id": 42}]
    finally:
        rest_catalog.drop_table(identifier)
