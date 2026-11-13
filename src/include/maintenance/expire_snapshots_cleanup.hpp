#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
class IcebergSnapshot;
struct IcebergTableMetadata;

struct ExpiredSnapshotFiles {
	//! Data and delete files that no retained manifest lists as a live entry
	vector<string> data_files;
	//! Manifests that no retained snapshot lists
	vector<string> manifests;
	//! Manifest lists of the expired snapshots
	vector<string> manifest_lists;
	//! Statistics and partition statistics files of the expired snapshots
	vector<string> statistics_files;
};

//! The files only the expired snapshots reach, given the metadata the catalog holds after the commit.
//! 'before' supplies the partition specs for reading the expired snapshots' manifests and the
//! statistics entries that existed before the commit. Reads happen through 'context'.
ExpiredSnapshotFiles PlanExpiredSnapshotFiles(ClientContext &context, const IcebergTableMetadata &before,
                                              const vector<IcebergSnapshot> &expired_snapshots,
                                              const IcebergTableMetadata &after);

} // namespace duckdb
