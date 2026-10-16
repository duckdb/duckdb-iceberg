#!/usr/bin/env python3
"""
An append-only table whose appends merge older manifests into the new one.

With 'commit.manifest-merge.enabled' and a 'commit.manifest.min-count-to-merge' of 2, every append
after the first rewrites the previous manifest into its own. The manifest written by an append then
holds the files it ADDED alongside EXISTING files from earlier snapshots, so its manifest-level row
counts are larger than what that append added. Incremental reads must not answer count(*) from them.

Run from the repository root with pyiceberg[sql-sqlite,pyarrow]==0.11.1:

    python3 scripts/persistent/incremental_merged_manifests.py
"""

import os
import sys

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField

warehouse_path = "data/persistent/incremental_merged_manifests"
if os.path.exists(warehouse_path):
    sys.exit(f"'{warehouse_path}' already exists, remove it first to regenerate")
os.makedirs(warehouse_path)

catalog_db = f"{warehouse_path}/pyiceberg_catalog.db"
catalog = SqlCatalog("local", uri=f"sqlite:///{catalog_db}", warehouse=warehouse_path)
catalog.create_namespace("default")

table = catalog.create_table(
    "default.merged",
    schema=Schema(NestedField(1, "id", IntegerType(), required=False)),
    properties={
        "commit.manifest-merge.enabled": "true",
        "commit.manifest.min-count-to-merge": "2",
    },
)

# Three appends of three rows each: snapshot i appends ids [3 * i + 1, 3 * i + 3]
for i in range(3):
    table.append(pa.table({"id": pa.array(range(3 * i + 1, 3 * i + 4), type=pa.int32())}))

for snapshot in table.snapshots():
    manifests = snapshot.manifests(table.io)
    print(
        f"snapshot {snapshot.snapshot_id}: {len(manifests)} manifest(s), "
        f"added_rows={sum(m.added_rows_count for m in manifests)}, "
        f"existing_rows={sum(m.existing_rows_count for m in manifests)}"
    )
print(f"metadata: {table.metadata_location}")

# The catalog is only needed to write the table, scans read the metadata file directly
os.remove(catalog_db)
