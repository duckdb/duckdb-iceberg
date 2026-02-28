#pragma once

#include "storage/iceberg_table_update.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "storage/iceberg_transaction_metadata.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergManifest;
struct IcebergManifestList;

struct IcebergAddSnapshot : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SNAPSHOT;

public:
	IcebergAddSnapshot(IcebergTableInformation &table_info, const string &manifest_list_path,
	                   IcebergSnapshot &&snapshot);

public:
	IcebergManifestList ConstructManifestList(CopyFunction &avro_copy, DatabaseInstance &db,
	                                          IcebergCommitState &commit_state) const;
	IcebergManifestFile ConstructManifest(CopyFunction &avro_copy, DatabaseInstance &db,
	                                      IcebergCommitState &commit_state, const IcebergManifestFile &manifest_file,
	                                      const IcebergManifestDeletes &deletes) const;
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;

public:
	case_insensitive_map_t<IcebergManifestDeletes> altered_manifests;
	IcebergManifestList manifest_list;

	IcebergSnapshot snapshot;
};

} // namespace duckdb
