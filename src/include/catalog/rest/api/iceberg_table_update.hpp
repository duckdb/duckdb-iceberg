#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "rest_catalog/objects/list.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {

struct IcebergTableInformation;
struct IcebergTransactionData;

enum class IcebergTableUpdateType : uint8_t {
	ASSIGN_UUID,
	UPGRADE_FORMAT_VERSION,
	ADD_SCHEMA,
	SET_CURRENT_SCHEMA,
	ADD_PARTITION_SPEC,
	SET_DEFAULT_SPEC,
	ADD_SORT_ORDER,
	SET_DEFAULT_SORT_ORDER,
	ADD_SNAPSHOT,
	SET_SNAPSHOT_REF,
	REMOVE_SNAPSHOTS,
	REMOVE_SNAPSHOT_REF,
	SET_LOCATION,
	SET_PROPERTIES,
	REMOVE_PROPERTIES,
	SET_STATISTICS,
	REMOVE_STATISTICS,
	REMOVE_PARTITION_SPECS,
	REMOVE_SCHEMAS,
	ENABLE_ROW_LINEAGE
};

struct IcebergCommitState {
public:
	IcebergCommitState(const IcebergTableInformation &table_info, ClientContext &context);

public:
	const IcebergTableInformation &table_info;
	optional_ptr<const IcebergSnapshot> latest_snapshot;
	//! Snapshot(s) created in this commit
	vector<IcebergSnapshot> created_snapshots;
	int64_t next_sequence_number;
	int64_t next_row_id = 0;

	ClientContext &context;

	//! Resolved Avro codec for manifests written during this commit ("null"/"deflate"), decided once
	//! per apply from write.manifest.compression-codec AND the catalog's delete capability. All
	//! manifest/manifest-list writes in the apply path use it.
	string manifest_avro_codec = "null";

	//! All the 'manifest_file' entries we will write to the new manifest list
	vector<IcebergManifestListEntry> manifests;
	//! Paths of manifest / manifest-list files written while applying this commit. Tracked so that a
	//! failed transaction (or a discarded retry attempt) can delete the metadata files it wrote,
	//! instead of leaking them (the data files are cleaned separately). Never deleted on success --
	//! they are referenced by the committed snapshot.
	vector<string> written_metadata_paths;
	rest_api_objects::CommitTableRequest table_change;
};

struct IcebergTableUpdate {
public:
	IcebergTableUpdate(IcebergTableUpdateType type, const IcebergTableInformation &table_info);
	virtual ~IcebergTableUpdate() {
	}

public:
	virtual void CreateUpdate(DatabaseInstance &db, ClientContext &context, IcebergCommitState &commit_state) const = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast IcebergTableUpdate to type - type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

public:
	IcebergTableUpdateType type;
	const IcebergTableInformation &table_info;
};

} // namespace duckdb
