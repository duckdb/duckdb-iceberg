#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

struct IcebergPartitionSpec;

using sequence_number_t = int64_t;

struct FieldSummary {
public:
	bool contains_null = false;
	//! Optional
	bool contains_nan = false;
	//! Optional
	Value lower_bound;
	//! Optional
	Value upper_bound;
};

struct ManifestPartitions {
public:
	void Create(const IcebergTableMetadata &metadata, const IcebergPartitionSpec &partition_spec,
	            const vector<IcebergManifestEntry> &entries);

public:
	bool has_partitions = false;
	vector<FieldSummary> field_summary;
};

struct IcebergManifestCounts {
public:
	static IcebergManifestCounts Zero();

	bool FilesComplete() const {
		return added_files_count && existing_files_count && deleted_files_count;
	}
	bool RowsComplete() const {
		return added_rows_count && existing_rows_count && deleted_rows_count;
	}
	bool Complete() const {
		return FilesComplete() && RowsComplete();
	}

public:
	optional<idx_t> added_files_count;
	optional<idx_t> existing_files_count;
	optional<idx_t> deleted_files_count;
	optional<idx_t> added_rows_count;
	optional<idx_t> existing_rows_count;
	optional<idx_t> deleted_rows_count;
};

enum class IcebergManifestContentType : uint8_t {
	DATA = 0,
	DELETE = 1,
};

string IcebergManifestContentTypeToString(IcebergManifestContentType type);

struct IcebergManifestMetadata {
public:
	IcebergManifestMetadata(int32_t schema_id, int32_t partition_spec_id, int32_t format_version,
	                        IcebergManifestContentType content)
	    : schema_id(schema_id), partition_spec_id(partition_spec_id), format_version(format_version), content(content) {
	}

	static IcebergManifestMetadata FromTableMetadata(const IcebergTableMetadata &table_metadata,
	                                                 IcebergManifestContentType content,
	                                                 optional<int32_t> partition_spec_id = nullopt);

public:
	const int32_t schema_id;
	const int32_t partition_spec_id;
	const int32_t format_version;
	const IcebergManifestContentType content;
};

unordered_map<string, string> GetManifestMetadataMap(const IcebergTableMetadata &table_metadata,
                                                     const IcebergManifestMetadata &manifest_metadata);

struct IcebergManifestFile {
public:
	//! Path to the manifest AVRO file
	string manifest_path;
	//! Length of the manifest file in bytes
	int64_t manifest_length;
	//! The id of the partition spec referenced by this manifest (and the data files that are part of it)
	int32_t partition_spec_id;
	optional<sequence_number_t> first_row_id;
	//! either data or deletes
	IcebergManifestContentType content;
	//! sequence_number when manifest was added to table (0 for Iceberg v1)
	optional<sequence_number_t> sequence_number;
	optional<sequence_number_t> min_sequence_number;
	optional<int64_t> added_snapshot_id;
	//! The count fields were optional in V1 manifest lists. A missing count means unknown/non-zero, not zero.
	optional<IcebergManifestCounts> counts;
	//! The field summaries of the partition (if present)
	ManifestPartitions partitions;

public:
	IcebergManifestFile(const string &manifest_path) : manifest_path(manifest_path) {
	}

	void SetCountsFromEntries(const vector<IcebergManifestEntry> &entries);
};

//! Snapshot metrics gathered for a manifest
struct IcebergManifestMetrics {
public:
	//! DELETE metrics
	int64_t added_position_deletes = 0; // added-position-deletes
	int64_t added_deletion_vectors = 0; // added-dvs
	int64_t added_equality_deletes = 0; // added-equality-deletes

	int64_t removed_position_deletes = 0; // removed-position-deletes
	int64_t removed_deletion_vectors = 0; // removed-dvs
	int64_t removed_equality_deletes = 0; // removed-equality-deletes

	int64_t added_position_delete_files = 0; // added-position-delete-files
	int64_t added_equality_delete_files = 0; // added-equality-delete-files
	int64_t added_delete_files = 0;          // added-delete-files

	int64_t removed_position_delete_files = 0; // removed-position-delete-files
	int64_t removed_equality_delete_files = 0; // removed-equality-delete-files
	int64_t removed_delete_files = 0;          // removed-delete-files

public:
	//! DATA metrics
	int64_t added_data_files = 0; // added-data-files
	int64_t added_records = 0;    // added-records

	int64_t deleted_data_files = 0; // deleted-data-files
	int64_t deleted_records = 0;    // deleted-records

public:
	//! Shared metrics
	int64_t added_files_size = 0;
	int64_t removed_files_size = 0;
};

struct IcebergManifestListEntry {
public:
	IcebergManifestListEntry(IcebergManifestFile file) : file(std::move(file)) {
	}
	IcebergManifestListEntry(IcebergManifestFile file, IcebergManifestMetadata manifest_metadata)
	    : file(std::move(file)), manifest_metadata(std::move(manifest_metadata)) {
	}
	IcebergManifestListEntry(const IcebergManifestListEntry &) = default;
	IcebergManifestListEntry(IcebergManifestListEntry &&) = default;
	IcebergManifestListEntry &operator=(const IcebergManifestListEntry &other) {
		if (this != &other) {
			file = other.file;
			manifest_entries = other.manifest_entries;
			metrics = other.metrics;
			if (other.manifest_metadata) {
				manifest_metadata.reset();
				manifest_metadata.emplace(*other.manifest_metadata);
			} else {
				manifest_metadata.reset();
			}
		}
		return *this;
	}
	IcebergManifestListEntry &operator=(IcebergManifestListEntry &&other) {
		if (this != &other) {
			file = std::move(other.file);
			manifest_entries = std::move(other.manifest_entries);
			metrics = std::move(other.metrics);
			if (other.manifest_metadata) {
				manifest_metadata.reset();
				manifest_metadata.emplace(*other.manifest_metadata);
			} else {
				manifest_metadata.reset();
			}
		}
		return *this;
	}

public:
	static IcebergManifestListEntry CreateFromEntries(FileSystem &fs, sequence_number_t sequence_number,
	                                                  const IcebergTableMetadata &table_metadata,
	                                                  const IcebergManifestMetadata &manifest_metadata,
	                                                  vector<IcebergManifestEntry> &&manifest_entries,
	                                                  int64_t &next_row_id);
	bool HasManifestEntries() const {
		return manifest_entries.has_value();
	}
	vector<IcebergManifestEntry> &GetManifestEntries() {
		D_ASSERT(manifest_entries);
		return *manifest_entries;
	}
	const vector<IcebergManifestEntry> &GetManifestEntries() const {
		D_ASSERT(manifest_entries);
		return *manifest_entries;
	}
	vector<IcebergManifestEntry> &GetOrCreateManifestEntries() {
		if (!manifest_entries) {
			manifest_entries.emplace();
		}
		return *manifest_entries;
	}

public:
	IcebergManifestFile file;
	optional<IcebergManifestMetadata> manifest_metadata;
	optional<vector<IcebergManifestEntry>> manifest_entries;
	//! Metrics gathered during 'CreateFromEntries'
	optional<IcebergManifestMetrics> metrics;
};

struct IcebergManifestList {
public:
	IcebergManifestList(int64_t snapshot_id, sequence_number_t sequence_number, const string &path)
	    : path(path), snapshot_id(snapshot_id), sequence_number(sequence_number) {
	}

public:
	vector<IcebergManifestListEntry> &GetManifestFilesMutable();
	const vector<IcebergManifestListEntry> &GetManifestFilesConst() const;
	const string &GetPath() const {
		return path;
	}
	sequence_number_t GetSequenceNumber() const {
		return sequence_number;
	}

	void AddNewManifestFile(IcebergManifestListEntry &&manifest_list_entry) {
		auto &manifest_file = manifest_list_entry.file;
		manifest_file.sequence_number = sequence_number;
		manifest_file.added_snapshot_id = snapshot_id;

		if (!manifest_file.min_sequence_number || *manifest_file.min_sequence_number > sequence_number) {
			manifest_file.min_sequence_number = sequence_number;
		}
		manifest_entries.push_back(std::move(manifest_list_entry));
	}
	void AddExistingManifestFile(IcebergManifestListEntry &&manifest_file) {
		manifest_entries.push_back(std::move(manifest_file));
	}
	idx_t GetManifestListEntriesCount() const;

	void AddToManifestEntries(vector<IcebergManifestListEntry> &manifest_list_entries);
	vector<IcebergManifestListEntry> GetManifestListEntries();

public:
	static LogicalType FieldSummaryType();
	static Value FieldSummaryFieldIds();
	static void LoadManifestFiles(const IcebergSnapshotScanInfo &snapshot_info, const IcebergTableMetadata &metadata,
	                              ClientContext &context, vector<IcebergManifestListEntry> &result);
	static unique_ptr<IcebergManifestList> Load(const string &iceberg_path, const IcebergTableMetadata &metadata,
	                                            const IcebergSnapshotScanInfo &snapshot_info, ClientContext &context,
	                                            const IcebergOptions &options);

private:
	string path;
	int64_t snapshot_id;
	sequence_number_t sequence_number;
	vector<IcebergManifestListEntry> manifest_entries;
};

namespace manifest_list {

static constexpr const int32_t MANIFEST_PATH = 500;
static constexpr const int32_t MANIFEST_LENGTH = 501;
static constexpr const int32_t PARTITION_SPEC_ID = 502;
static constexpr const int32_t CONTENT = 517;
static constexpr const int32_t SEQUENCE_NUMBER = 515;
static constexpr const int32_t MIN_SEQUENCE_NUMBER = 516;
static constexpr const int32_t ADDED_SNAPSHOT_ID = 503;
static constexpr const int32_t ADDED_FILES_COUNT = 504;
static constexpr const int32_t EXISTING_FILES_COUNT = 505;
static constexpr const int32_t DELETED_FILES_COUNT = 506;
static constexpr const int32_t ADDED_ROWS_COUNT = 512;
static constexpr const int32_t EXISTING_ROWS_COUNT = 513;
static constexpr const int32_t DELETED_ROWS_COUNT = 514;
static constexpr const int32_t PARTITIONS = 507;
static constexpr const int32_t PARTITIONS_ELEMENT = 508;
static constexpr const int32_t FIELD_SUMMARY_CONTAINS_NULL = 509;
static constexpr const int32_t FIELD_SUMMARY_CONTAINS_NAN = 518;
static constexpr const int32_t FIELD_SUMMARY_LOWER_BOUND = 510;
static constexpr const int32_t FIELD_SUMMARY_UPPER_BOUND = 511;
static constexpr const int32_t KEY_METADATA = 519;
static constexpr const int32_t FIRST_ROW_ID = 520;

void WriteToFile(const IcebergTableMetadata &table_metadata, const IcebergManifestList &manifest_list,
                 CopyFunction &copy_function, DatabaseInstance &db, ClientContext &context);

} // namespace manifest_list

} // namespace duckdb
