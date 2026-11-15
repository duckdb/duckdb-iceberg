"""Regression tests for metadata-only snapshot removal in the REST mock."""

import copy
import tempfile
import unittest
from pathlib import Path

from scripts.mock_rest_catalog.model import Catalog, CatalogError


class SnapshotRemovalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.catalog = Catalog(Path(self.directory.name) / "warehouse")
        self.catalog.create_namespace({"namespace": ["default"]})
        self.key = (("default",), "snapshots")
        self.catalog.create(
            self.key[0],
            {"name": self.key[1], "schema": {"type": "struct", "schema-id": 0, "fields": []}},
        )
        updates = []
        for identifier in (10, 20, 30):
            updates.extend(
                [
                    {
                        "action": "add-snapshot",
                        "snapshot": {
                            "snapshot-id": identifier,
                            "sequence-number": identifier // 10,
                            "schema-id": 0,
                            "timestamp-ms": identifier,
                            "manifest-list": f"{identifier}.avro",
                            "summary": {"operation": "append"},
                        },
                    },
                    self.ref(identifier),
                ]
            )
        self.commit(updates)

    @staticmethod
    def ref(identifier):
        return {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": identifier}

    @staticmethod
    def removal(identifiers):
        return {"action": "remove-snapshots", "snapshot-ids": identifiers}

    def commit(self, updates):
        return self.catalog.commit(self.key, {"requirements": [], "updates": updates})

    def test_removal_preserves_files_and_trims_history_through_last_gap(self):
        self.commit([self.ref(10), self.ref(30)])
        before = copy.deepcopy(self.catalog.tables[self.key])
        metadata_files = list(Path(self.directory.name).rglob("*.metadata.json"))
        result = self.commit([self.removal([10, 10, 999])])["metadata"]
        self.assertEqual([s["snapshot-id"] for s in result["snapshots"]], [20, 30])
        self.assertEqual([s["snapshot-id"] for s in result["snapshot-log"]], [30])
        self.assertEqual(result["current-snapshot-id"], 30)
        self.assertEqual(result["refs"], before["metadata"]["refs"])
        self.assertEqual(result["last-sequence-number"], 3)
        self.assertTrue(all(path.is_file() for path in metadata_files))
        self.assertEqual(len(list(Path(self.directory.name).rglob("*.metadata.json"))), len(metadata_files) + 1)
        self.assertEqual(self.commit([self.removal([10, 999])])["metadata"]["snapshots"], result["snapshots"])

    def test_removing_current_snapshot_clears_main(self):
        result = self.commit([self.removal([30])])["metadata"]
        self.assertEqual([s["snapshot-id"] for s in result["snapshots"]], [10, 20])
        self.assertFalse(result["refs"])
        self.assertIsNone(result.get("current-snapshot-id"))
        self.assertEqual(result["snapshot-log"], [])

    def test_later_invalid_update_does_not_publish_removal(self):
        before = copy.deepcopy(self.catalog.tables[self.key])
        with self.assertRaises(CatalogError) as error:
            self.commit([self.removal([10]), self.ref(999)])
        self.assertEqual(error.exception.code, 400)
        self.assertIn("Unknown snapshot reference", str(error.exception))
        self.assertEqual(self.catalog.tables[self.key], before)

    def test_malformed_snapshot_ids_do_not_publish(self):
        before = copy.deepcopy(self.catalog.tables[self.key])
        for identifiers in (None, 10, [True], ["10"], [10.5], [None], [1 << 63]):
            with self.subTest(identifiers=identifiers):
                with self.assertRaises(CatalogError) as error:
                    self.commit([self.removal(identifiers)])
                self.assertEqual(error.exception.code, 400)
                self.assertIn("snapshot-ids", str(error.exception))
                self.assertEqual(self.catalog.tables[self.key], before)


if __name__ == "__main__":
    unittest.main()
