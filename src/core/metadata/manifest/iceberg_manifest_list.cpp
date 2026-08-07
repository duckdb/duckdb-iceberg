#include "core/metadata/manifest/iceberg_manifest_list.hpp"

#include "core/metadata/manifest/iceberg_avro_codec.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "core/expression/iceberg_value.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "core/expression/iceberg_transform.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "re2/re2.h"

#include <optional>

namespace duckdb {

string IcebergManifestContentTypeToString(IcebergManifestContentType type) {
	switch (type) {
	case IcebergManifestContentType::DATA:
		return "DATA";
	case IcebergManifestContentType::DELETE:
		return "DELETE";
	default:
		throw InvalidConfigurationException("Invalid Manifest Content Type");
	}
}

IcebergManifestMetadata IcebergManifestMetadata::FromTableMetadata(const IcebergTableMetadata &table_metadata,
                                                                   IcebergManifestContentType content,
                                                                   optional<int32_t> partition_spec_id) {
	return IcebergManifestMetadata(table_metadata.GetCurrentSchemaId(),
	                               partition_spec_id ? *partition_spec_id
	                                                 : NumericCast<int32_t>(table_metadata.default_spec_id),
	                               NumericCast<int32_t>(table_metadata.iceberg_version), content);
}

unordered_map<string, string> GetManifestMetadataMap(const IcebergTableMetadata &table_metadata,
                                                     const IcebergManifestMetadata &manifest_metadata) {
	unordered_map<string, string> result;
	result.reserve(6);
	auto schema_id = manifest_metadata.schema_id;
	auto partition_spec_id = manifest_metadata.partition_spec_id;
	auto format_version = manifest_metadata.format_version;
	auto content = manifest_metadata.content;

	JSONWriter writer;
	auto schema_root_obj = writer.CreateObject();
	writer.SetRoot(schema_root_obj);
	IcebergCreateTableRequest::PopulateSchema(writer, schema_root_obj, *table_metadata.GetSchemaFromId(schema_id));
	result.emplace("schema", writer.ToString(JSONWriteFlags::ALLOW_INF_AND_NAN));
	result.emplace("schema-id", std::to_string(schema_id));

	auto partition_spec = table_metadata.FindPartitionSpecById(partition_spec_id);
	if (!partition_spec) {
		throw InternalException("Cannot find partition spec with id " + std::to_string(partition_spec_id));
	}
	result.emplace("partition-spec", partition_spec->FieldsToJSONString());
	result.emplace("partition-spec-id", std::to_string(partition_spec_id));
	result.emplace("format-version", std::to_string(format_version));
	result.emplace("content", content == IcebergManifestContentType::DATA ? "data" : "deletes");
	return result;
}

IcebergManifestCounts IcebergManifestCounts::Zero() {
	IcebergManifestCounts result;
	result.added_files_count = 0;
	result.existing_files_count = 0;
	result.deleted_files_count = 0;
	result.added_rows_count = 0;
	result.existing_rows_count = 0;
	result.deleted_rows_count = 0;
	return result;
}

void IcebergManifestFile::SetCountsFromEntries(const vector<IcebergManifestEntry> &entries) {
	counts = IcebergManifestCounts::Zero();
	auto &manifest_counts = *counts;
	for (const auto &entry : entries) {
		switch (entry.status) {
		case IcebergManifestEntryStatusType::ADDED:
			(*manifest_counts.added_files_count)++;
			*manifest_counts.added_rows_count += entry.data_file.record_count;
			break;
		case IcebergManifestEntryStatusType::EXISTING:
			(*manifest_counts.existing_files_count)++;
			*manifest_counts.existing_rows_count += entry.data_file.record_count;
			break;
		case IcebergManifestEntryStatusType::DELETED:
			(*manifest_counts.deleted_files_count)++;
			*manifest_counts.deleted_rows_count += entry.data_file.record_count;
			break;
		default:
			throw InvalidConfigurationException("Invalid manifest entry status");
		}
	}
}

namespace {

struct IcebergManifestEntryMetrics {
public:
	IcebergManifestEntryMetrics(int64_t &position_deletes, int64_t &deletion_vectors, int64_t &equality_deletes,
	                            int64_t &position_delete_files, int64_t &equality_delete_files, int64_t &delete_files,
	                            int64_t &data_files, int64_t &records, int64_t &files_size, idx_t &files_count,
	                            idx_t &rows_count)
	    : position_deletes(position_deletes), deletion_vectors(deletion_vectors), equality_deletes(equality_deletes),
	      position_delete_files(position_delete_files), equality_delete_files(equality_delete_files),
	      delete_files(delete_files), data_files(data_files), records(records), files_size(files_size),
	      files_count(files_count), rows_count(rows_count) {
	}

public:
	int64_t &position_deletes;      //! added|removed-position-deletes
	int64_t &deletion_vectors;      //! added|removed-dvs
	int64_t &equality_deletes;      //! added|removed-equality-deletes
	int64_t &position_delete_files; //! added|removed-position-delete-files
	int64_t &equality_delete_files; //! added|removed-equality-delete-files
	int64_t &delete_files;          //! added|removed-delete-files
	int64_t &data_files;            //! added|deleted-data-files
	int64_t &records;               //! added|deleted-records
	int64_t &files_size;            //! added|removed-files-size

	idx_t &files_count;
	idx_t &rows_count;
};

static IcebergManifestEntryMetrics GetManifestEntryMetrics(IcebergManifestMetrics &metrics,
                                                           IcebergManifestFile &manifest_file,
                                                           IcebergManifestEntryStatusType direction) {
	D_ASSERT(direction != IcebergManifestEntryStatusType::EXISTING);
	D_ASSERT(manifest_file.counts && manifest_file.counts->Complete());
	auto &counts = *manifest_file.counts;
	if (direction == IcebergManifestEntryStatusType::ADDED) {
		return IcebergManifestEntryMetrics(metrics.added_position_deletes, metrics.added_deletion_vectors,
		                                   metrics.added_equality_deletes, metrics.added_position_delete_files,
		                                   metrics.added_equality_delete_files, metrics.added_delete_files,
		                                   metrics.added_data_files, metrics.added_records, metrics.added_files_size,
		                                   *counts.added_files_count, *counts.added_rows_count);
	} else {
		return IcebergManifestEntryMetrics(
		    metrics.removed_position_deletes, metrics.removed_deletion_vectors, metrics.removed_equality_deletes,
		    metrics.removed_position_delete_files, metrics.removed_equality_delete_files, metrics.removed_delete_files,
		    metrics.deleted_data_files, metrics.deleted_records, metrics.removed_files_size,
		    *counts.deleted_files_count, *counts.deleted_rows_count);
	}
}

} // namespace

static void CollectDeleteManifestMetrics(const IcebergManifestEntry &manifest_entry,
                                         IcebergManifestEntryMetrics &metrics) {
	auto &data_file = manifest_entry.data_file;
	metrics.files_count++;
	metrics.rows_count += data_file.record_count;
	metrics.delete_files++;
	switch (data_file.content) {
	case IcebergManifestEntryContentType::EQUALITY_DELETES: {
		metrics.equality_delete_files++;
		metrics.equality_deletes += data_file.record_count;
		break;
	}
	case IcebergManifestEntryContentType::POSITION_DELETES: {
		metrics.position_deletes += data_file.record_count;
		if (data_file.IsDeletionVector()) {
			metrics.deletion_vectors++;
		} else {
			metrics.position_delete_files++;
		}
		break;
	}
	case IcebergManifestEntryContentType::DATA: {
		throw InvalidConfigurationException("Encountered data_file.content == DATA in DELETE manifest");
	}
	}
}

static void CollectDataManifestMetrics(const IcebergManifestEntry &manifest_entry,
                                       IcebergManifestEntryMetrics &metrics) {
	auto &data_file = manifest_entry.data_file;
	if (data_file.content != IcebergManifestEntryContentType::DATA) {
		throw InvalidConfigurationException("Encountered data_file.content != DATA in DATA manifest");
	}
	metrics.files_count++;
	metrics.rows_count += data_file.record_count;

	metrics.data_files++;
	metrics.records += data_file.record_count;
}

IcebergManifestListEntry IcebergManifestListEntry::CreateFromEntries(FileSystem &fs, sequence_number_t sequence_number,
                                                                     const IcebergTableMetadata &table_metadata,
                                                                     const IcebergManifestMetadata &manifest_metadata,
                                                                     vector<IcebergManifestEntry> &&manifest_entries,
                                                                     int64_t &next_row_id) {
	//! create manifest file path
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = fs.JoinPath(table_metadata.GetMetadataPath(fs), manifest_file_uuid + "-m0.avro");

	// Add a manifest list entry for the entries
	IcebergManifestListEntry manifest_list_entry(IcebergManifestFile {manifest_file_path}, manifest_metadata);
	auto manifest_content = manifest_metadata.content;
	auto manifest_partition_spec_id = manifest_metadata.partition_spec_id;
	auto &manifest_file = manifest_list_entry.file;
	manifest_file.manifest_path = manifest_file_path;
	if (table_metadata.iceberg_version >= 3 && manifest_content == IcebergManifestContentType::DATA) {
		//! 'first_row_id' is only assigned to data manifests (row lineage), deletes manifests leave it null
		manifest_file.first_row_id = next_row_id;
	}

	manifest_file.manifest_path = manifest_file_path;
	manifest_file.content = manifest_content;
	//! NOTE: this gets overwritten on commit
	manifest_file.sequence_number = sequence_number;
	manifest_file.counts = IcebergManifestCounts::Zero();
	manifest_file.partition_spec_id = manifest_partition_spec_id;

	manifest_list_entry.metrics.emplace();
	auto &metrics = *manifest_list_entry.metrics;

	//! Add the files to the manifest
	for (auto &manifest_entry : manifest_entries) {
		auto &data_file = manifest_entry.data_file;

		if (data_file.content == IcebergManifestEntryContentType::DATA) {
			next_row_id += data_file.record_count;
		}

		do {
			if (manifest_entry.status == IcebergManifestEntryStatusType::EXISTING) {
				auto &counts = *manifest_file.counts;
				(*counts.existing_files_count)++;
				*counts.existing_rows_count += data_file.record_count;
				break;
			}
			auto entry_metrics = GetManifestEntryMetrics(metrics, manifest_file, manifest_entry.status);

			//! Gather 'added-files-size' and 'removed-files-size' metrics
			auto new_files_size =
			    IcebergUtils::AddFileSizeChecked(entry_metrics.files_size, data_file.GetContentSizeInBytes());
			entry_metrics.files_size = new_files_size;

			if (manifest_metadata.content == IcebergManifestContentType::DATA) {
				CollectDataManifestMetrics(manifest_entry, entry_metrics);
			} else {
				CollectDeleteManifestMetrics(manifest_entry, entry_metrics);
			}
		} while (false);

		//! NOTE: this gets overwritten on commit
		auto entry_data_seq = manifest_entry.GetSequenceNumber(manifest_file);
		if (!manifest_file.min_sequence_number || entry_data_seq < *manifest_file.min_sequence_number) {
			manifest_file.min_sequence_number = entry_data_seq;
		}
	}
	//! NOTE: this gets assigned when the manifest is added to a manifest list
	manifest_file.added_snapshot_id = nullopt;

	// Compute partition field summaries (upper/lower bounds) for the manifest list entry
	if (table_metadata.HasPartitionSpec() && table_metadata.GetLatestPartitionSpec().IsPartitioned()) {
		auto partition_spec_it = table_metadata.partition_specs.find(manifest_partition_spec_id);
		if (partition_spec_it == table_metadata.partition_specs.end()) {
			throw InternalException("Cannot find partition spec with id " + std::to_string(manifest_partition_spec_id));
		}
		auto &partition_spec = partition_spec_it->second;
		manifest_file.partitions.Create(table_metadata, partition_spec, manifest_entries);
	}

	auto &stored_entries = manifest_list_entry.GetOrCreateManifestEntries();
	stored_entries.insert(stored_entries.end(), std::make_move_iterator(manifest_entries.begin()),
	                      std::make_move_iterator(manifest_entries.end()));
	return manifest_list_entry;
}

void ManifestPartitions::Create(const IcebergTableMetadata &metadata, const IcebergPartitionSpec &partition_spec,
                                const vector<IcebergManifestEntry> &manifest_entries) {
	if (manifest_entries.empty() || partition_spec.fields.empty()) {
		return;
	}

	// Check if any entry has partition info
	for (auto &entry : manifest_entries) {
		if (entry.data_file.partition_info.empty()) {
			throw InvalidInputException(
			    "Manifest file contains entries without partition info even though there is a partition spec");
		}
	}

	has_partitions = true;

	auto num_fields = partition_spec.fields.size();
	field_summary.resize(num_fields);
	vector<Value> min_values(num_fields);
	vector<Value> max_values(num_fields);
	vector<bool> initialized(num_fields, false);

	for (auto &entry : manifest_entries) {
		auto &data_file = entry.data_file;
		auto data_extended_partition_info = data_file.GetExtendedPartitionInfo(metadata);
		for (idx_t i = 0; i < num_fields; i++) {
			auto &spec_field = partition_spec.fields[i];

			// Find the partition info entry matching this field's partition_field_id
			IcebergExtendedPartitionInfo extended_partition_info;
			bool partition_info_exists = false;
			for (auto &pi : data_extended_partition_info) {
				if (pi.field_id == spec_field.partition_field_id) {
					extended_partition_info = pi;
					partition_info_exists = true;
					break;
				}
			}

			if (!partition_info_exists || extended_partition_info.value.IsNull()) {
				field_summary[i].contains_null = true;
				continue;
			}

			// Get them serialized type from the DataFilePartitionInfo's transform and source_type
			auto serialized_type =
			    extended_partition_info.transform.GetSerializedType(extended_partition_info.source_type);

			// Cast the partition value (stored as VARCHAR) to the correct serialized type so we can compare typed
			// values
			auto typed_value = extended_partition_info.value.DefaultCastAs(serialized_type);

			if (!initialized[i]) {
				min_values[i] = typed_value;
				max_values[i] = typed_value;
				initialized[i] = true;
			} else {
				if (typed_value < min_values[i]) {
					min_values[i] = typed_value;
				}
				if (typed_value > max_values[i]) {
					max_values[i] = typed_value;
				}
			}
		}
	}

	// Serialize the min/max values as bounds
	for (idx_t i = 0; i < num_fields; i++) {
		if (!initialized[i]) {
			// All values for this field are null - set bounds to null BLOBs
			field_summary[i].lower_bound = Value(LogicalType::BLOB);
			field_summary[i].upper_bound = Value(LogicalType::BLOB);
			continue;
		}
		auto &spec_field = partition_spec.fields[i];
		// Find one IcebergPartitionInfo entry to get the type info
		IcebergExtendedPartitionInfo extended_partition_info;
		bool have_extended_partition_info = false;
		for (auto &entry : manifest_entries) {
			auto &data_file = entry.data_file;
			auto data_extended_partition_info = data_file.GetExtendedPartitionInfo(metadata);
			for (auto &pi : data_extended_partition_info) {
				if (pi.field_id == spec_field.partition_field_id && !pi.value.IsNull()) {
					extended_partition_info = pi;
					have_extended_partition_info = true;
					break;
				}
			}
			if (have_extended_partition_info) {
				break;
			}
		}
		D_ASSERT(have_extended_partition_info);
		auto serialized_type = extended_partition_info.transform.GetSerializedType(extended_partition_info.source_type);
		// min/max_values already in their partition result value types. We cast those to varchar to serialize them
		// again unless they are blob, in which case we do not cast and serialize
		SerializeResult lower_result = SerializeResult(min_values[i].type(), min_values[i]);
		SerializeResult upper_result = SerializeResult(max_values[i].type(), max_values[i]);
		if (min_values[i].type() != LogicalType::BLOB && max_values[i].type() != LogicalType::BLOB) {
			lower_result = IcebergValue::SerializeValue(min_values[i].DefaultCastAs(LogicalType::VARCHAR),
			                                            min_values[i].type(), SerializeBound::LOWER_BOUND);
			upper_result = IcebergValue::SerializeValue(max_values[i].DefaultCastAs(LogicalType::VARCHAR),
			                                            max_values[i].type(), SerializeBound::UPPER_BOUND);
		}

		if (lower_result.HasValue()) {
			field_summary[i].lower_bound = lower_result.GetValue();
		} else {
			field_summary[i].lower_bound = Value(LogicalType::BLOB);
		}
		if (upper_result.HasValue()) {
			field_summary[i].upper_bound = upper_result.GetValue();
		} else {
			field_summary[i].upper_bound = Value(LogicalType::BLOB);
		}
	}
}

vector<IcebergManifestListEntry> &IcebergManifestList::GetManifestFilesMutable() {
	return manifest_entries;
}

const vector<IcebergManifestListEntry> &IcebergManifestList::GetManifestFilesConst() const {
	return manifest_entries;
}

idx_t IcebergManifestList::GetManifestListEntriesCount() const {
	return manifest_entries.size();
}

void IcebergManifestList::AddToManifestEntries(vector<IcebergManifestListEntry> &manifest_list_entries) {
	manifest_entries.insert(manifest_entries.begin(), std::make_move_iterator(manifest_list_entries.begin()),
	                        std::make_move_iterator(manifest_list_entries.end()));
}

vector<IcebergManifestListEntry> IcebergManifestList::GetManifestListEntries() {
	return std::move(manifest_entries);
}

LogicalType IcebergManifestList::FieldSummaryType() {
	child_list_t<LogicalType> children;
	children.emplace_back("contains_null", LogicalType::BOOLEAN);
	children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	children.emplace_back("lower_bound", LogicalType::BLOB);
	children.emplace_back("upper_bound", LogicalType::BLOB);
	auto field_summary = LogicalType::STRUCT(children);

	return LogicalType::LIST(field_summary);
}

namespace manifest_list {

namespace {

using std::optional;

struct AvroBindSchemaMetadata {
	child_list_t<Value> field_ids;
	vector<Identifier> names;
	vector<LogicalType> types;
};

using FieldSummaryListWriter = VectorWriter<VectorListType<VectorStructType<bool, bool, string_t, string_t>>>;

static Value CreateFieldID(int32_t field_id, bool nullable) {
	child_list_t<Value> fields;
	fields.emplace_back("__duckdb_field_id", Value::INTEGER(field_id));
	fields.emplace_back("__duckdb_nullable", Value::BOOLEAN(nullable));
	return Value::STRUCT(fields);
}

static void AddSimpleColumn(AvroBindSchemaMetadata &metadata, const string &name, const LogicalType &type,
                            int32_t field_id, bool nullable) {
	metadata.names.push_back(Identifier(name));
	metadata.types.push_back(type);
	metadata.field_ids.emplace_back(name, CreateFieldID(field_id, nullable));
}

template <class WRITER>
static void WriteBlobField(WRITER &writer, const Value &value) {
	if (value.IsNull()) {
		writer.WriteNull();
		return;
	}
	writer.WriteValue(value.GetValueUnsafe<string_t>());
}

static void WritePartitions(FieldSummaryListWriter &writer, const ManifestPartitions &partitions) {
	if (!partitions.has_partitions) {
		writer.WriteNull();
		return;
	}
	auto summaries = writer.WriteList(partitions.field_summary.size());
	auto it = partitions.field_summary.begin();
	for (auto &summary_writer : summaries) {
		auto &summary = *it++;
		summary_writer.WriteValue([&](auto &contains_null_writer, auto &contains_nan_writer, auto &lower_bound_writer,
		                              auto &upper_bound_writer) {
			contains_null_writer.WriteValue(summary.contains_null);
			contains_nan_writer.WriteValue(summary.contains_nan);
			WriteBlobField(lower_bound_writer, summary.lower_bound);
			WriteBlobField(upper_bound_writer, summary.upper_bound);
		});
	}
}

template <class T>
static void WriteManifestCount(VectorWriter<T> &writer, const optional<idx_t> &count, bool required, const char *name) {
	if (!count) {
		if (required) {
			throw InvalidConfigurationException("manifest_file.%s is not set", name);
		}
		writer.WriteNull();
		return;
	}
	writer.WriteValue(static_cast<T>(*count));
}

struct ManifestListVectorWriters {
	explicit ManifestListVectorWriters(DataChunk &data, idx_t row_count, bool counts_required_p)
	    : manifest_path(data.data[MANIFEST_PATH_INDEX], row_count, 0),
	      manifest_length(data.data[MANIFEST_LENGTH_INDEX], row_count, 0),
	      partition_spec_id(data.data[PARTITION_SPEC_ID_INDEX], row_count, 0),
	      added_snapshot_id(data.data[ADDED_SNAPSHOT_ID_INDEX], row_count, 0),
	      added_files_count(data.data[ADDED_FILES_COUNT_INDEX], row_count, 0),
	      existing_files_count(data.data[EXISTING_FILES_COUNT_INDEX], row_count, 0),
	      deleted_files_count(data.data[DELETED_FILES_COUNT_INDEX], row_count, 0),
	      added_rows_count(data.data[ADDED_ROWS_COUNT_INDEX], row_count, 0),
	      existing_rows_count(data.data[EXISTING_ROWS_COUNT_INDEX], row_count, 0),
	      deleted_rows_count(data.data[DELETED_ROWS_COUNT_INDEX], row_count, 0),
	      partitions(data.data[PARTITIONS_INDEX], row_count, 0), counts_required(counts_required_p) {
		if (data.ColumnCount() > CONTENT_INDEX) {
			content.emplace(data.data[CONTENT_INDEX], row_count, 0);
			sequence_number.emplace(data.data[SEQUENCE_NUMBER_INDEX], row_count, 0);
			min_sequence_number.emplace(data.data[MIN_SEQUENCE_NUMBER_INDEX], row_count, 0);
		}
		if (data.ColumnCount() > FIRST_ROW_ID_INDEX) {
			first_row_id.emplace(data.data[FIRST_ROW_ID_INDEX], row_count, 0);
		}
	}

	void WriteRow(const IcebergManifestFile &manifest, idx_t *next_row_id = nullptr) {
		manifest_path.WriteValue(string_t(manifest.manifest_path));
		manifest_length.WriteValue(manifest.manifest_length);
		partition_spec_id.WriteValue(manifest.partition_spec_id);
		if (!manifest.added_snapshot_id) {
			throw InvalidConfigurationException("manifest_file.added_snapshot_id is not set");
		}
		added_snapshot_id.WriteValue(*manifest.added_snapshot_id);
		IcebergManifestCounts empty_counts;
		auto &counts = manifest.counts ? *manifest.counts : empty_counts;
		WriteManifestCount(added_files_count, counts.added_files_count, counts_required, "added_files_count");
		WriteManifestCount(existing_files_count, counts.existing_files_count, counts_required, "existing_files_count");
		WriteManifestCount(deleted_files_count, counts.deleted_files_count, counts_required, "deleted_files_count");
		WriteManifestCount(added_rows_count, counts.added_rows_count, counts_required, "added_rows_count");
		WriteManifestCount(existing_rows_count, counts.existing_rows_count, counts_required, "existing_rows_count");
		WriteManifestCount(deleted_rows_count, counts.deleted_rows_count, counts_required, "deleted_rows_count");
		WritePartitions(partitions, manifest.partitions);

		if (content) {
			content->WriteValue(static_cast<int32_t>(manifest.content));
			if (!manifest.sequence_number) {
				throw InvalidConfigurationException("manifest_file.sequence_number is not set");
			}
			sequence_number->WriteValue(*manifest.sequence_number);
			if (!manifest.min_sequence_number) {
				min_sequence_number->WriteValue(int64_t(-1));
			} else {
				min_sequence_number->WriteValue(*manifest.min_sequence_number);
			}
		}

		if (!first_row_id) {
			return;
		}
		auto row_id = manifest.first_row_id;
		if (!row_id && manifest.content == IcebergManifestContentType::DATA) {
			D_ASSERT(next_row_id);
			if (!manifest.counts || !manifest.counts->added_rows_count || !manifest.counts->existing_rows_count) {
				throw InvalidConfigurationException("manifest_file row counts are not set");
			}
			row_id = static_cast<int64_t>(*next_row_id);
			*next_row_id += *manifest.counts->added_rows_count;
			*next_row_id += *manifest.counts->existing_rows_count;
		}
		if (row_id) {
			first_row_id->WriteValue(*row_id);
		} else {
			first_row_id->WriteNull();
		}
	}

private:
	static constexpr idx_t MANIFEST_PATH_INDEX = 0;
	static constexpr idx_t MANIFEST_LENGTH_INDEX = 1;
	static constexpr idx_t PARTITION_SPEC_ID_INDEX = 2;
	static constexpr idx_t ADDED_SNAPSHOT_ID_INDEX = 3;
	static constexpr idx_t ADDED_FILES_COUNT_INDEX = 4;
	static constexpr idx_t EXISTING_FILES_COUNT_INDEX = 5;
	static constexpr idx_t DELETED_FILES_COUNT_INDEX = 6;
	static constexpr idx_t ADDED_ROWS_COUNT_INDEX = 7;
	static constexpr idx_t EXISTING_ROWS_COUNT_INDEX = 8;
	static constexpr idx_t DELETED_ROWS_COUNT_INDEX = 9;
	static constexpr idx_t PARTITIONS_INDEX = 10;
	static constexpr idx_t CONTENT_INDEX = 11;
	static constexpr idx_t SEQUENCE_NUMBER_INDEX = 12;
	static constexpr idx_t MIN_SEQUENCE_NUMBER_INDEX = 13;
	static constexpr idx_t FIRST_ROW_ID_INDEX = 14;

	VectorWriter<string_t> manifest_path;
	VectorWriter<int64_t> manifest_length;
	VectorWriter<int32_t> partition_spec_id;
	VectorWriter<int64_t> added_snapshot_id;
	VectorWriter<int32_t> added_files_count;
	VectorWriter<int32_t> existing_files_count;
	VectorWriter<int32_t> deleted_files_count;
	VectorWriter<int64_t> added_rows_count;
	VectorWriter<int64_t> existing_rows_count;
	VectorWriter<int64_t> deleted_rows_count;
	FieldSummaryListWriter partitions;
	optional<VectorWriter<int32_t>> content;
	optional<VectorWriter<int64_t>> sequence_number;
	optional<VectorWriter<int64_t>> min_sequence_number;
	optional<VectorWriter<int64_t>> first_row_id;
	bool counts_required;
};

} // namespace

static Value FieldSummaryFieldIds() {
	child_list_t<Value> children;
	children.emplace_back("contains_null", CreateFieldID(FIELD_SUMMARY_CONTAINS_NULL, false));
	children.emplace_back("contains_nan", CreateFieldID(FIELD_SUMMARY_CONTAINS_NAN, true));
	children.emplace_back("lower_bound", CreateFieldID(FIELD_SUMMARY_LOWER_BOUND, true));
	children.emplace_back("upper_bound", CreateFieldID(FIELD_SUMMARY_UPPER_BOUND, true));
	children.emplace_back("__duckdb_field_id", Value::INTEGER(PARTITIONS_ELEMENT));
	children.emplace_back("__duckdb_nullable", Value::BOOLEAN(false));
	auto field_summary = Value::STRUCT(children);

	child_list_t<Value> list_children;
	list_children.emplace_back("list", field_summary);
	list_children.emplace_back("__duckdb_field_id", Value::INTEGER(PARTITIONS));
	return Value::STRUCT(list_children);
}

void WriteToFile(const IcebergTableMetadata &table_metadata, const IcebergManifestList &manifest_list,
                 CopyFunction &copy, DatabaseInstance &db, ClientContext &context) {
	auto &allocator = db.GetBufferManager().GetBufferAllocator();

	//! Create the types for the DataChunk

	AvroBindSchemaMetadata metadata;

	// manifest_path: string - 500
	AddSimpleColumn(metadata, "manifest_path", LogicalType::VARCHAR, MANIFEST_PATH, false);

	// manifest_length: long - 501
	AddSimpleColumn(metadata, "manifest_length", LogicalType::BIGINT, MANIFEST_LENGTH, false);

	// partition_spec_id: long - 502
	AddSimpleColumn(metadata, "partition_spec_id", LogicalType::INTEGER, PARTITION_SPEC_ID, false);

	// added_snapshot_id: long - 503
	AddSimpleColumn(metadata, "added_snapshot_id", LogicalType::BIGINT, ADDED_SNAPSHOT_ID, false);

	const bool counts_nullable = table_metadata.iceberg_version == 1;

	// added_files_count: int - 504
	AddSimpleColumn(metadata, "added_files_count", LogicalType::INTEGER, ADDED_FILES_COUNT, counts_nullable);

	// existing_files_count: int - 505
	AddSimpleColumn(metadata, "existing_files_count", LogicalType::INTEGER, EXISTING_FILES_COUNT, counts_nullable);

	// deleted_files_count: int - 506
	AddSimpleColumn(metadata, "deleted_files_count", LogicalType::INTEGER, DELETED_FILES_COUNT, counts_nullable);

	// added_rows_count: long - 512
	AddSimpleColumn(metadata, "added_rows_count", LogicalType::BIGINT, ADDED_ROWS_COUNT, counts_nullable);

	// existing_rows_count: long - 513
	AddSimpleColumn(metadata, "existing_rows_count", LogicalType::BIGINT, EXISTING_ROWS_COUNT, counts_nullable);

	// deleted_rows_count: long - 514
	AddSimpleColumn(metadata, "deleted_rows_count", LogicalType::BIGINT, DELETED_ROWS_COUNT, counts_nullable);

	// partitions: list<508: field_summary> - 507
	metadata.names.push_back("partitions");
	metadata.types.push_back(IcebergManifestList::FieldSummaryType());
	metadata.field_ids.emplace_back("partitions", FieldSummaryFieldIds());

	if (table_metadata.iceberg_version >= 2) {
		// content: int - 517
		AddSimpleColumn(metadata, "content", LogicalType::INTEGER, CONTENT, false);

		// sequence_number: long - 515
		AddSimpleColumn(metadata, "sequence_number", LogicalType::BIGINT, SEQUENCE_NUMBER, false);

		// min_sequence_number: long - 516
		AddSimpleColumn(metadata, "min_sequence_number", LogicalType::BIGINT, MIN_SEQUENCE_NUMBER, false);
	}

	if (table_metadata.iceberg_version >= 3) {
		//! first_row_id: long - 520
		AddSimpleColumn(metadata, "first_row_id", LogicalType::BIGINT, FIRST_ROW_ID, true);
	}

	//! Populate the DataChunk with the manifests
	auto &manifest_files = manifest_list.GetManifestFilesConst();
	DataChunk data;
	data.Initialize(allocator, metadata.types, STANDARD_VECTOR_SIZE);

	idx_t next_row_id;
	if (table_metadata.next_row_id) {
		next_row_id = *table_metadata.next_row_id;
	} else {
		next_row_id = 0;
	}

	CopyInfo copy_info;
	copy_info.is_from = false;
	copy_info.options["root_name"].push_back(Value("manifest_file"));
	copy_info.options["field_ids"].push_back(Value::STRUCT(metadata.field_ids));

	//! write.manifest.compression-codec: let the Avro COPY writer emit the codec natively.
	//! "null" is the COPY default (uncompressed), so only set the option for a compressing codec.
	auto avro_codec =
	    iceberg_avro_codec::ResolveAvroCodec(table_metadata.GetTableProperty("write.manifest.compression-codec"));
	if (!StringUtil::CIEquals(avro_codec, "null")) {
		copy_info.options["codec"].push_back(Value(avro_codec));
	}

	CopyFunctionBindInput input(copy_info);
	input.file_extension = "avro";

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto bind_data = copy.copy_to_bind(context, input, metadata.names, metadata.types);

	auto global_state = copy.copy_to_initialize_global(context, *bind_data, manifest_list.GetPath());
	auto local_state = copy.copy_to_initialize_local(execution_context, *bind_data);

	for (idx_t offset = 0; offset < manifest_files.size(); offset += STANDARD_VECTOR_SIZE) {
		const auto chunk_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, manifest_files.size() - offset);
		if (offset > 0) {
			data.Reset();
		}

		ManifestListVectorWriters writers(data, chunk_count, table_metadata.iceberg_version >= 2);
		for (idx_t i = 0; i < chunk_count; i++) {
			const auto &manifest_entry = manifest_files[offset + i];
			const auto &manifest = manifest_entry.file;
			writers.WriteRow(manifest, table_metadata.iceberg_version >= 3 ? &next_row_id : nullptr);
		}

		data.SetChildCardinality(chunk_count);
		copy.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, data);
	}
	copy.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
	copy.copy_to_finalize(context, *bind_data, *global_state);
}

} // namespace manifest_list

Value IcebergManifestList::FieldSummaryFieldIds() {
	return manifest_list::FieldSummaryFieldIds();
}

void IcebergManifestList::LoadManifestFiles(const IcebergSnapshotScanInfo &snapshot_info,
                                            const IcebergTableMetadata &metadata, ClientContext &context,
                                            vector<IcebergManifestListEntry> &result) {
	auto &snapshot = *snapshot_info.snapshot;
	if (!snapshot.manifests.empty()) {
		result.reserve(result.size() + snapshot.manifests.size());
		for (auto &manifest_path : snapshot.manifests) {
			IcebergManifestFile manifest_file(manifest_path);
			manifest_file.manifest_length = 0;
			manifest_file.partition_spec_id = metadata.default_spec_id;
			manifest_file.content = IcebergManifestContentType::DATA;
			manifest_file.sequence_number = 0;
			manifest_file.min_sequence_number = 0;
			manifest_file.added_snapshot_id = snapshot.snapshot_id;
			result.emplace_back(std::move(manifest_file));
		}
		return;
	}
	if (snapshot.manifest_list.empty()) {
		throw InvalidConfigurationException("Snapshot must contain either 'manifest-list' or 'manifests'");
	}

	auto scan = AvroScan::ScanManifestList(snapshot_info, metadata, context, snapshot.manifest_list, result);
	auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);
	while (!manifest_list_reader->Finished()) {
		manifest_list_reader->Read();
	}
}

unique_ptr<IcebergManifestList> IcebergManifestList::Load(const string &iceberg_path,
                                                          const IcebergTableMetadata &metadata,
                                                          const IcebergSnapshotScanInfo &snapshot_info,
                                                          ClientContext &context, const IcebergOptions &options) {
	auto &snapshot = *snapshot_info.snapshot;
	if (!snapshot.snapshot_id) {
		throw InvalidConfigurationException("snapshot.snapshot_id is not set");
	}
	if (!snapshot.sequence_number) {
		throw InvalidConfigurationException("snapshot.sequence_number is not set");
	}
	auto ret = make_uniq<IcebergManifestList>(*snapshot.snapshot_id, *snapshot.sequence_number, snapshot.manifest_list);

	auto &fs = FileSystem::GetFileSystem(context);
	if (!snapshot.manifests.empty()) {
		LoadManifestFiles(snapshot_info, metadata, context, ret->manifest_entries);
	} else {
		auto manifest_list_full_path = options.allow_moved_paths
		                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
		                                   : snapshot.manifest_list;
		auto scan = AvroScan::ScanManifestList(snapshot_info, metadata, context, manifest_list_full_path,
		                                       ret->manifest_entries);
		auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);
		while (!manifest_list_reader->Finished()) {
			manifest_list_reader->Read();
		}
	}

	//! Read all manifest files, producing 'manifest_entry' items
	auto manifest_scan =
	    AvroScan::ScanManifest(snapshot_info, ret->manifest_entries, options, fs, iceberg_path, metadata, context);
	auto manifest_file_reader = make_uniq<manifest_file::ManifestReader>(*manifest_scan);

	while (!manifest_file_reader->Finished()) {
		manifest_file_reader->Read();
	}
	return ret;
}

} // namespace duckdb
