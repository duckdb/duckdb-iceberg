#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_options.hpp"

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
	IcebergManifestListScanInfo(const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot);
	virtual ~IcebergManifestListScanInfo();
};

class IcebergManifestFileScanInfo : public IcebergAvroScanInfo {
public:
	static constexpr const AvroScanInfoType TYPE = AvroScanInfoType::MANIFEST_FILE;

public:
	IcebergManifestFileScanInfo(const IcebergTableMetadata &metadata, const IcebergSnapshot &snapshot,
	                            const vector<IcebergManifestFile> &manifest_files, const IcebergOptions &options,
	                            FileSystem &fs, const string &iceberg_path);
	virtual ~IcebergManifestFileScanInfo();

public:
	// Helper to get manifest metadata by file index
	const IcebergManifestFile &GetManifestFile(idx_t file_idx) const;

public:
	const vector<IcebergManifestFile> &manifest_files;
	const IcebergOptions &options;
	FileSystem &fs;
	string iceberg_path;
	//! partition_field_id -> column type;
	map<idx_t, LogicalType> partition_field_id_to_type;
};

class IcebergAvroMultiFileList : public SimpleMultiFileList {
public:
	IcebergAvroMultiFileList(shared_ptr<IcebergAvroScanInfo> info, vector<OpenFileInfo> paths);
	virtual ~IcebergAvroMultiFileList();

public:
	shared_ptr<IcebergAvroScanInfo> info;
};

} // namespace duckdb
