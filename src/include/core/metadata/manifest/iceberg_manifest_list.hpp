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

#include <variant>

namespace duckdb {

struct IcebergPartitionSpec;

using sequence_number_t = int64_t;

struct FieldSummary {
public:
	bool contains_null = false;
	//! Optional
	optional<bool> contains_nan = false;
	//! Optional
	Value lower_bound;
	//! Optional
	Value upper_bound;
};

struct ManifestPartitions {
public:
	void Create(const IcebergTableMetadata &metadata, const IcebergTableSchema &target_schema,
	            const IcebergPartitionSpec &partition_spec, const vector<IcebergManifestEntry> &entries);

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

//! Manifest attributes shared by in-memory content and Avro files.
struct IcebergManifest {
public:
	IcebergManifest(int32_t partition_spec_id, IcebergManifestContentType content, sequence_number_t sequence_number)
	    : partition_spec_id(partition_spec_id), content(content), sequence_number(sequence_number) {
	}

	//! Compute manifest attributes from content without taking ownership of its entries or assigning a file path.
	static IcebergManifest CreateFromEntries(sequence_number_t sequence_number,
	                                         const IcebergTableMetadata &table_metadata,
	                                         const IcebergManifestMetadata &manifest_metadata,
	                                         const vector<IcebergManifestEntry> &entries,
	                                         optional<int64_t> first_row_id);

public:
	//! The id of the partition spec referenced by this manifest (and the data files that are part of it)
	int32_t partition_spec_id;
	optional<sequence_number_t> first_row_id;
	//! either data or deletes
	IcebergManifestContentType content;
	//! Sequence number used for entry inheritance: transaction-local or persisted (0 for Iceberg v1).
	sequence_number_t sequence_number;
	optional<sequence_number_t> min_sequence_number;
	//! The count fields were optional in V1 manifest lists. A missing count means unknown/non-zero, not zero.
	optional<IcebergManifestCounts> counts;
	//! The field summaries of the partition (if present)
	ManifestPartitions partitions;

public:
	void SetCountsFromEntries(const vector<IcebergManifestEntry> &entries);
};

//! A descriptor for an existing Avro manifest, including its snapshot identity.
struct IcebergManifestFile : public IcebergManifest {
	IcebergManifestFile(string path, int64_t length, int64_t snapshot_id, IcebergManifest manifest)
	    : IcebergManifest(std::move(manifest)), manifest_path(std::move(path)), manifest_length(length),
	      added_snapshot_id(snapshot_id) {
	}

	//! Resolve inherited identities against this file before moving entries into a replacement.
	//! ADDED entries become EXISTING; explicit identities and previously DELETED entries are preserved.
	vector<IcebergManifestEntry> PrepareEntriesForRewrite(vector<IcebergManifestEntry> entries) const;

	string manifest_path;
	int64_t manifest_length;
	int64_t added_snapshot_id;
};

struct IcebergManifestListEntry {
public:
	IcebergManifestListEntry(IcebergManifestFile file) : manifest(std::move(file)) {
	}
	IcebergManifestListEntry(IcebergManifestFile file, IcebergManifestMetadata metadata,
	                         vector<IcebergManifestEntry> entries)
	    : manifest_metadata(std::move(metadata)), manifest_entries(std::move(entries)), manifest(std::move(file)) {
	}
	IcebergManifestListEntry(const IcebergManifestListEntry &) = default;
	IcebergManifestListEntry(IcebergManifestListEntry &&) = default;
	IcebergManifestListEntry &operator=(const IcebergManifestListEntry &other) {
		if (this != &other) {
			manifest = other.manifest;
			manifest_entries = other.manifest_entries;
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
			manifest = std::move(other.manifest);
			manifest_entries = std::move(other.manifest_entries);
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
	//! Adapt materialized entries to the scanner without a file path or manifest-level row-ID inheritance.
	static IcebergManifestListEntry CreateForScan(sequence_number_t sequence_number,
	                                              const IcebergTableMetadata &table_metadata,
	                                              const IcebergManifestMetadata &manifest_metadata,
	                                              vector<IcebergManifestEntry> entries);
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
	bool HasFile() const {
		return std::holds_alternative<IcebergManifestFile>(manifest);
	}
	const IcebergManifestFile &GetFile() const {
		if (!HasFile()) {
			throw InternalException("In-memory manifest content has no Avro file");
		}
		return std::get<IcebergManifestFile>(manifest);
	}
	IcebergManifest &GetManifest() {
		return HasFile() ? std::get<IcebergManifestFile>(manifest) : std::get<IcebergManifest>(manifest);
	}
	const IcebergManifest &GetManifest() const {
		return HasFile() ? std::get<IcebergManifestFile>(manifest) : std::get<IcebergManifest>(manifest);
	}
	optional<IcebergManifestMetadata> manifest_metadata;
	optional<vector<IcebergManifestEntry>> manifest_entries;

private:
	IcebergManifestListEntry(IcebergManifest manifest, IcebergManifestMetadata metadata,
	                         vector<IcebergManifestEntry> entries)
	    : manifest_metadata(std::move(metadata)), manifest_entries(std::move(entries)), manifest(std::move(manifest)) {
	}

	std::variant<IcebergManifest, IcebergManifestFile> manifest;
};

//! Contains only descriptors for written Avro files; in-memory scan entries cannot be added to this list.
struct IcebergManifestList {
public:
	explicit IcebergManifestList(const string &path) : path(path) {
	}

public:
	const vector<IcebergManifestListEntry> &GetManifestFilesConst() const;
	const string &GetPath() const {
		return path;
	}
	void AddExistingManifestFile(IcebergManifestListEntry &&manifest_file) {
		if (!manifest_file.HasFile()) {
			throw InternalException("Cannot add unwritten manifest content to a manifest list");
		}
		manifest_entries.push_back(std::move(manifest_file));
	}
	idx_t GetManifestListEntriesCount() const;

	vector<IcebergManifestListEntry> TakeManifestListEntries();

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
