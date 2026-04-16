#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

#include "catalog/rest/api/iceberg_table_update.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergManifestList;

struct IcebergAddSnapshot : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SNAPSHOT;

public:
	IcebergAddSnapshot(const IcebergTableInformation &table_info);

public:
	void ConstructManifestList(IcebergManifestList &manifest_list, CopyFunction &avro_copy, DatabaseInstance &db,
	                           IcebergCommitState &commit_state) const;
	void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const override;
	const vector<IcebergManifestListEntry> &GetManifestFiles() const;
	void AddManifestFile(IcebergManifestListEntry &&manifest_file);

public:
	IcebergManifestDeletes altered_manifests;

private:
	vector<IcebergManifestListEntry> manifest_files;
};

} // namespace duckdb
