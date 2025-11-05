#pragma once

#include "storage/iceberg_table_update.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergManifestFile;
struct IcebergManifestList;

struct IcebergAddSnapshot : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SNAPSHOT;

public:
	IcebergAddSnapshot(IcebergTableInformation &table_info, const string &manifest_list_path,
	                   IcebergSnapshot &&snapshot);

public:
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) override;

public:
	IcebergManifestList manifest_list;

	IcebergSnapshot snapshot;
};

} // namespace duckdb
