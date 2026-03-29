#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/file_system.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "iceberg_options.hpp"
#include "planning/iceberg_manifest_read_state.hpp"

namespace duckdb {

enum class AvroScanInfoType : uint8_t { MANIFEST_LIST, MANIFEST_FILE };

class IcebergAvroScanInfo : public TableFunctionInfo {
public:
	IcebergAvroScanInfo(AvroScanInfoType type, const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot);
	virtual ~IcebergAvroScanInfo();

public:
	idx_t IcebergVersion() const {
		return metadata.iceberg_version;
	}

public:
	AvroScanInfoType type;
	const IcebergTableMetadata &metadata;
	const IcebergSnapshot &snapshot;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast AvroScanInfo to type - AvroScanInfo type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast AvroScanInfo to type - AvroScanInfo type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class IcebergManifestListScanInfo : public IcebergAvroScanInfo {
public:
	static constexpr const AvroScanInfoType TYPE = AvroScanInfoType::MANIFEST_LIST;

public:
	IcebergManifestListScanInfo(const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot,
	                            vector<IcebergManifestListEntry> &result);
	virtual ~IcebergManifestListScanInfo();

public:
	vector<IcebergManifestListEntry> &result;
};

struct IcebergManifestFileScan {
public:
	IcebergManifestFileScan(idx_t index, IcebergManifestListEntry &entry)
	    : manifest_list_entry_idx(index), manifest_list_entry(entry) {
	}

public:
	idx_t manifest_list_entry_idx;
	IcebergManifestListEntry &manifest_list_entry;
};

class IcebergManifestFileScanInfo : public IcebergAvroScanInfo {
public:
	static constexpr const AvroScanInfoType TYPE = AvroScanInfoType::MANIFEST_FILE;

public:
	IcebergManifestFileScanInfo(ClientContext &context, const IcebergTableMetadata &metadata,
	                            const IcebergSnapshot &snapshot, vector<IcebergManifestListEntry> &manifest_files,
	                            const IcebergOptions &options, const string &iceberg_path,
	                            optional_ptr<ManifestEntryReadState> read_state);
	virtual ~IcebergManifestFileScanInfo();

public:
	ClientContext &context;
	vector<IcebergManifestFileScan> files_to_scan;
	const IcebergOptions &options;
	string iceberg_path;
	//! partition_field_id -> semantic column type (e.g. INTEGER for DAY)
	map<idx_t, LogicalType> partition_field_id_to_type;
	optional_ptr<ManifestEntryReadState> read_state;
};

class IcebergAvroMultiFileList : public SimpleMultiFileList {
public:
	IcebergAvroMultiFileList(shared_ptr<IcebergAvroScanInfo> info, vector<OpenFileInfo> paths);
	virtual ~IcebergAvroMultiFileList();

public:
	shared_ptr<IcebergAvroScanInfo> info;
};

} // namespace duckdb
