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
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"
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

IcebergManifestListEntry IcebergManifestListEntry::CreateFromEntries(FileSystem &fs, int64_t snapshot_id,
                                                                     sequence_number_t sequence_number,
                                                                     const IcebergTableMetadata &table_metadata,
                                                                     IcebergManifestContentType manifest_content_type,
                                                                     vector<IcebergManifestEntry> &&manifest_entries,
                                                                     int64_t &next_row_id) {
	//! create manifest file path
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = fs.JoinPath(table_metadata.GetMetadataPath(fs), manifest_file_uuid + "-m0.avro");

	// Add a manifest list entry for the entries
	IcebergManifestListEntry manifest_list_entry(manifest_file_path);
	auto &manifest_file = manifest_list_entry.file;
	manifest_file.manifest_path = manifest_file_path;
	if (table_metadata.iceberg_version >= 3 && manifest_content_type == IcebergManifestContentType::DATA) {
		//! 'first_row_id' is only assigned to data manifests (row lineage), deletes manifests leave it null
		manifest_file.has_first_row_id = true;
		manifest_file.first_row_id = next_row_id;
	}

	manifest_file.manifest_path = manifest_file_path;
	manifest_file.content = manifest_content_type;
	//! NOTE: this gets overwritten on commit
	manifest_file.sequence_number = sequence_number;
	manifest_file.added_files_count = 0;
	manifest_file.deleted_files_count = 0;
	manifest_file.existing_files_count = 0;
	manifest_file.added_rows_count = 0;
	manifest_file.existing_rows_count = 0;
	manifest_file.deleted_rows_count = 0;
	manifest_file.partition_spec_id = table_metadata.default_spec_id;

	//! Add the files to the manifest
	for (auto &manifest_entry : manifest_entries) {
		auto &data_file = manifest_entry.data_file;
		if (data_file.content == IcebergManifestEntryContentType::DATA) {
			next_row_id += data_file.record_count;
		}
		switch (manifest_entry.status) {
		case IcebergManifestEntryStatusType::ADDED: {
			manifest_file.added_files_count++;
			manifest_file.added_rows_count += data_file.record_count;
			break;
		}
		case IcebergManifestEntryStatusType::DELETED: {
			manifest_file.deleted_files_count++;
			manifest_file.deleted_rows_count += data_file.record_count;
			break;
		}
		case IcebergManifestEntryStatusType::EXISTING: {
			manifest_file.existing_files_count++;
			manifest_file.existing_rows_count += data_file.record_count;
			break;
		}
		}

		//! NOTE: this gets overwritten on commit
		if (!manifest_file.has_min_sequence_number ||
		    manifest_file.sequence_number < manifest_file.min_sequence_number) {
			manifest_file.min_sequence_number = manifest_file.sequence_number;
		}
		manifest_file.has_min_sequence_number = true;
	}
	//! NOTE: this gets overwritten on commit
	manifest_file.added_snapshot_id = snapshot_id;

	// Compute partition field summaries (upper/lower bounds) for the manifest list entry
	if (table_metadata.HasPartitionSpec() && table_metadata.GetLatestPartitionSpec().IsPartitioned()) {
		auto partition_spec_it = table_metadata.partition_specs.find(table_metadata.default_spec_id);
		if (partition_spec_it == table_metadata.partition_specs.end()) {
			throw InternalException("Cannot find partition spec with id " +
			                        std::to_string(table_metadata.default_spec_id));
		}
		auto &partition_spec = partition_spec_it->second;
		manifest_file.partitions.Create(table_metadata, partition_spec, manifest_entries);
	}

	manifest_list_entry.manifest_entries.insert(manifest_list_entry.manifest_entries.end(),
	                                            std::make_move_iterator(manifest_entries.begin()),
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

struct ManifestListVectorWriters {
	explicit ManifestListVectorWriters(DataChunk &data, idx_t row_count)
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
	      partitions(data.data[PARTITIONS_INDEX], row_count, 0) {
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
		added_snapshot_id.WriteValue(manifest.added_snapshot_id);
		added_files_count.WriteValue(static_cast<int32_t>(manifest.added_files_count));
		existing_files_count.WriteValue(static_cast<int32_t>(manifest.existing_files_count));
		deleted_files_count.WriteValue(static_cast<int32_t>(manifest.deleted_files_count));
		added_rows_count.WriteValue(static_cast<int64_t>(manifest.added_rows_count));
		existing_rows_count.WriteValue(static_cast<int64_t>(manifest.existing_rows_count));
		deleted_rows_count.WriteValue(static_cast<int64_t>(manifest.deleted_rows_count));
		WritePartitions(partitions, manifest.partitions);

		if (content) {
			content->WriteValue(static_cast<int32_t>(manifest.content));
			sequence_number->WriteValue(manifest.sequence_number);
			if (!manifest.has_min_sequence_number) {
				min_sequence_number->WriteValue(int64_t(-1));
			} else {
				min_sequence_number->WriteValue(manifest.min_sequence_number);
			}
		}

		if (!first_row_id) {
			return;
		}
		bool has_first_row_id = manifest.has_first_row_id;
		int64_t row_id = manifest.first_row_id;
		if (!has_first_row_id && manifest.content == IcebergManifestContentType::DATA) {
			D_ASSERT(next_row_id);
			row_id = static_cast<int64_t>(*next_row_id);
			has_first_row_id = true;
			*next_row_id += manifest.added_rows_count;
			*next_row_id += manifest.existing_rows_count;
		}
		if (has_first_row_id) {
			first_row_id->WriteValue(row_id);
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

	// added_files_count: int - 504
	AddSimpleColumn(metadata, "added_files_count", LogicalType::INTEGER, ADDED_FILES_COUNT, false);

	// existing_files_count: int - 505
	AddSimpleColumn(metadata, "existing_files_count", LogicalType::INTEGER, EXISTING_FILES_COUNT, false);

	// deleted_files_count: int - 506
	AddSimpleColumn(metadata, "deleted_files_count", LogicalType::INTEGER, DELETED_FILES_COUNT, false);

	// added_rows_count: long - 512
	AddSimpleColumn(metadata, "added_rows_count", LogicalType::BIGINT, ADDED_ROWS_COUNT, false);

	// existing_rows_count: long - 513
	AddSimpleColumn(metadata, "existing_rows_count", LogicalType::BIGINT, EXISTING_ROWS_COUNT, false);

	// deleted_rows_count: long - 514
	AddSimpleColumn(metadata, "deleted_rows_count", LogicalType::BIGINT, DELETED_ROWS_COUNT, false);

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
	if (table_metadata.has_next_row_id) {
		next_row_id = table_metadata.next_row_id;
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

		ManifestListVectorWriters writers(data, chunk_count);
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

unique_ptr<IcebergManifestList> IcebergManifestList::Load(const string &iceberg_path,
                                                          const IcebergTableMetadata &metadata,
                                                          const IcebergSnapshotScanInfo &snapshot_info,
                                                          ClientContext &context, const IcebergOptions &options) {
	auto &snapshot = *snapshot_info.snapshot;
	auto ret = make_uniq<IcebergManifestList>(snapshot.snapshot_id, snapshot.sequence_number, snapshot.manifest_list);

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;

	//! Read the entire manifest list, producing 'manifest_file' items
	auto scan =
	    AvroScan::ScanManifestList(snapshot_info, metadata, context, manifest_list_full_path, ret->manifest_entries);
	auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);

	while (!manifest_list_reader->Finished()) {
		manifest_list_reader->Read();
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
