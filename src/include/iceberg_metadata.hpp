//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"
#include "iceberg_types.hpp"
#include "iceberg_options.hpp"
#include "duckdb/common/open_file_info.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

struct IcebergColumnDefinition {
public:
	static IcebergColumnDefinition ParseFromJson(yyjson_val *val);

	LogicalType ToDuckDBType() {
		return type;
	}

	int32_t id;
	string name;
	LogicalType type;
	Value default_value;
	bool required;
};

struct IcebergFieldMapping {
public:
	//! field-id can be omitted for the root of a struct
	int32_t field_id = NumericLimits<int32_t>::Maximum();
	case_insensitive_map_t<idx_t> field_mapping_indexes;

public:
public:
	void Verify() {
		if (field_id != NumericLimits<int32_t>::Maximum()) {
			return;
		}
		if (field_mapping_indexes.empty()) {
			throw InvalidInputException(
			    "Parsed 'schema.name-mapping.default' field mapping is invalid, has no 'field-id' and no 'fields'");
		}
	}
};

struct IcebergMetadata {
private:
	IcebergMetadata() = default;

public:
	static unique_ptr<IcebergMetadata> Parse(const string &path, FileSystem &fs,
	                                         const string &metadata_compression_codec);
	~IcebergMetadata() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}

public:
	// Ownership of parse data
	yyjson_doc *doc = nullptr;
	string document;

	//! Parsed info
	yyjson_val *snapshots;
	vector<yyjson_val *> schemas;
	uint64_t iceberg_version;
	uint64_t schema_id;

	IcebergFieldMapping root_field_mapping;
	vector<IcebergFieldMapping> mappings;
};

//! An Iceberg snapshot https://iceberg.apache.org/spec/#snapshots
class IcebergSnapshot {
public:
	//! Snapshot metadata
	uint64_t snapshot_id;
	uint64_t sequence_number;
	string manifest_list;
	timestamp_t timestamp_ms;
	idx_t iceberg_format_version;
	uint64_t schema_id;
	vector<IcebergColumnDefinition> schema;
	string metadata_compression_codec = "none";

public:
	static IcebergSnapshot GetLatestSnapshot(IcebergMetadata &info, const IcebergOptions &options);
	static IcebergSnapshot GetSnapshotById(IcebergMetadata &info, idx_t snapshot_id, const IcebergOptions &options);
	static IcebergSnapshot GetSnapshotByTimestamp(IcebergMetadata &info, timestamp_t timestamp,
	                                              const IcebergOptions &options);

	static IcebergSnapshot ParseSnapShot(yyjson_val *snapshot, idx_t iceberg_format_version, idx_t schema_id,
	                                     vector<yyjson_val *> &schemas, const IcebergOptions &options);
	static string GetMetaDataPath(ClientContext &context, const string &path, FileSystem &fs,
	                              const IcebergOptions &options);

protected:
	//! Version extraction and identification
	static bool UnsafeVersionGuessingEnabled(ClientContext &context);
	static string GetTableVersionFromHint(const string &path, FileSystem &fs, string version_format);
	static string GuessTableVersion(const string &meta_path, FileSystem &fs, const IcebergOptions &options);
	static string PickTableVersion(vector<OpenFileInfo> &found_metadata, string &version_pattern, string &glob);

	//! Internal JSON parsing functions
	static yyjson_val *FindLatestSnapshotInternal(yyjson_val *snapshots);
	static yyjson_val *FindSnapshotByIdInternal(yyjson_val *snapshots, idx_t target_id);
	static yyjson_val *FindSnapshotByIdTimestampInternal(yyjson_val *snapshots, timestamp_t timestamp);
	static vector<IcebergColumnDefinition> ParseSchema(vector<yyjson_val *> &schemas, idx_t schema_id);
};

//! Represents the iceberg table at a specific IcebergSnapshot. Corresponds to a single Manifest List.
struct IcebergTable {
public:
	//! Loads all(!) metadata of into IcebergTable object
	static IcebergTable Load(const string &iceberg_path, IcebergSnapshot &snapshot, ClientContext &context,
	                         const IcebergOptions &options);

public:
	//! Returns all paths to be scanned for the IcebergManifestContentType
	template <IcebergManifestContentType TYPE>
	vector<string> GetPaths() {
		vector<string> ret;
		for (auto &entry : entries) {
			if (entry.manifest.content != TYPE) {
				continue;
			}
			for (auto &manifest_entry : entry.manifest_entries) {
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				ret.push_back(manifest_entry.file_path);
			}
		}
		return ret;
	}
	vector<IcebergManifestEntry> GetAllPaths() {
		vector<IcebergManifestEntry> ret;
		for (auto &entry : entries) {
			for (auto &manifest_entry : entry.manifest_entries) {
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				ret.push_back(manifest_entry);
			}
		}
		return ret;
	}

	void Print() {
		Printer::Print("Iceberg table (" + path + ")");
		for (auto &entry : entries) {
			entry.Print();
		}
	}

	//! The snapshot of this table
	IcebergSnapshot snapshot;
	//! The entries (manifests) of this table
	vector<IcebergTableEntry> entries;

protected:
	string path;
};

} // namespace duckdb
