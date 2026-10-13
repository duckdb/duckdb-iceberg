#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
struct IcebergTableMetadata;

//! The files an Iceberg table owns according to its metadata, grouped the way Iceberg's CatalogUtil.dropTableData
//! deletes them.
struct IcebergTableFiles {
public:
	//! Data and delete files: every entry of every manifest. DELETED entries are included, because the file they name
	//! exists until the snapshot that removed it is expired.
	vector<string> data_files;
	//! Every manifest reachable from a snapshot, once.
	vector<string> manifests;
	//! The manifest list of every snapshot (V1 snapshots that embed their manifests have none).
	vector<string> manifest_lists;
	//! Table statistics and partition statistics (Puffin) files.
	vector<string> statistics_files;
	//! The metadata-log entries, then the current metadata file.
	vector<string> metadata_files;
	//! Whether the data files were left out because the table has 'gc.enabled' = false.
	bool data_files_retained = false;
};

//! Client-side purge of a dropped table, for catalogs that only unregister the table on DELETE (purgeRequested=false),
//! see IcebergPurgeMode::CLIENT. Follows Iceberg's GlueCatalog.dropTable and CatalogUtil.dropTableData: once the table
//! metadata is loaded, nothing about the files can fail the drop. Metadata that cannot be read and files that cannot
//! be deleted are logged as warnings, under 'operation' ("Purge of Iceberg table <name>"), and what they would have
//! named stays behind. Only a cancelled statement and fatal errors propagate.
class IcebergTablePurge {
public:
	//! Collect the table's files from its metadata: the manifest list of every snapshot and, unless the table has
	//! 'gc.enabled' = false (its data files may be shared with another table), every manifest once. A manifest that
	//! cannot be read is logged and skipped, like dropTableData does ("this may cause orphaned data files"). A manifest
	//! list that cannot be read is logged and its snapshot skipped; dropTableData itself fails there, after the catalog
	//! deleted the table and before deleting any file, so this is a deliberate deviation towards its stated intent.
	//! The unreadable file itself is still collected for deletion in both cases.
	static IcebergTableFiles CollectFiles(ClientContext &context, const IcebergTableMetadata &metadata,
	                                      const string &operation);
	//! Delete the collected files. Every group is attempted, and a group that cannot be deleted is logged with its
	//! paths: the catalog has already accepted the drop.
	static void DeleteFiles(ClientContext &context, const IcebergTableFiles &files, const string &operation);
};

} // namespace duckdb
