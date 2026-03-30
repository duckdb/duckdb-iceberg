#include "core/metadata/manifest/iceberg_manifest_list.hpp"

#include "duckdb/common/exception/conversion_exception.hpp"
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

	// Add a manifest list entry for the delete files
	IcebergManifestListEntry manifest_list_entry(manifest_file_path);
	auto &manifest_file = manifest_list_entry.file;
	manifest_file.manifest_path = manifest_file_path;
	if (table_metadata.iceberg_version >= 3) {
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
		manifest_file.partitions.Create(partition_spec, manifest_entries);
	}

	manifest_list_entry.manifest_entries.insert(manifest_list_entry.manifest_entries.end(),
	                                            std::make_move_iterator(manifest_entries.begin()),
	                                            std::make_move_iterator(manifest_entries.end()));
	return manifest_list_entry;
}

void ManifestPartitions::Create(const IcebergPartitionSpec &partition_spec,
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
		for (idx_t i = 0; i < num_fields; i++) {
			auto &spec_field = partition_spec.fields[i];

			// Find the partition info entry matching this field's partition_field_id
			optional_ptr<const DataFilePartitionInfo> info_ptr;
			for (auto &pi : data_file.partition_info) {
				if (pi.field_id == spec_field.partition_field_id) {
					info_ptr = &pi;
					break;
				}
			}

			if (!info_ptr || info_ptr->value.IsNull()) {
				field_summary[i].contains_null = true;
				continue;
			}

			// Get the serialized type from the DataFilePartitionInfo's transform and source_type
			auto serialized_type = info_ptr->transform.GetSerializedType(info_ptr->source_type);

			// Cast the partition value (stored as VARCHAR) to the correct serialized type
			auto typed_value = info_ptr->value.DefaultCastAs(serialized_type);

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
		// Find one DataFilePartitionInfo entry to get the type info
		optional_ptr<const DataFilePartitionInfo> info_ptr;
		for (auto &entry : manifest_entries) {
			for (auto &pi : entry.data_file.partition_info) {
				if (pi.field_id == spec_field.partition_field_id && !pi.value.IsNull()) {
					info_ptr = &pi;
					break;
				}
			}
			if (info_ptr) {
				break;
			}
		}
		D_ASSERT(info_ptr);
		auto serialized_type = info_ptr->transform.GetSerializedType(info_ptr->source_type);
		auto lower_result = IcebergValue::SerializeValue(min_values[i], serialized_type, SerializeBound::LOWER_BOUND);
		auto upper_result = IcebergValue::SerializeValue(max_values[i], serialized_type, SerializeBound::UPPER_BOUND);

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

struct AvroBindSchemaMetadata {
	child_list_t<Value> field_ids;
	vector<string> names;
	vector<LogicalType> types;
};

static Value CreateFieldID(int32_t field_id, bool nullable) {
	child_list_t<Value> fields;
	fields.emplace_back("__duckdb_field_id", Value::INTEGER(field_id));
	fields.emplace_back("__duckdb_nullable", Value::BOOLEAN(nullable));
	return Value::STRUCT(fields);
}

static void AddSimpleColumn(AvroBindSchemaMetadata &metadata, const string &name, const LogicalType &type,
                            int32_t field_id, bool nullable) {
	metadata.names.push_back(name);
	metadata.types.push_back(type);
	metadata.field_ids.emplace_back(name, CreateFieldID(field_id, nullable));
}

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

	// content: int - 517
	AddSimpleColumn(metadata, "content", LogicalType::INTEGER, CONTENT, false);

	// sequence_number: long - 515
	AddSimpleColumn(metadata, "sequence_number", LogicalType::BIGINT, SEQUENCE_NUMBER, false);

	// min_sequence_number: long - 516
	AddSimpleColumn(metadata, "min_sequence_number", LogicalType::BIGINT, MIN_SEQUENCE_NUMBER, false);

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

	if (table_metadata.iceberg_version >= 3) {
		//! first_row_id: long - 520
		metadata.names.push_back("first_row_id");
		metadata.types.push_back(LogicalType::BIGINT);
		metadata.field_ids.emplace_back("first_row_id", Value::INTEGER(FIRST_ROW_ID));
	}

	//! Populate the DataChunk with the manifests
	auto &manifest_files = manifest_list.GetManifestFilesConst();
	DataChunk data;
	data.Initialize(allocator, metadata.types, manifest_files.size());

	idx_t next_row_id;
	if (table_metadata.has_next_row_id) {
		next_row_id = table_metadata.next_row_id;
	} else {
		next_row_id = 0;
	}

	for (idx_t i = 0; i < manifest_files.size(); i++) {
		const auto &manifest_entry = manifest_files[i];
		const auto &manifest = manifest_entry.file;
		idx_t col_idx = 0;

		// manifest_path: string - 500
		data.SetValue(col_idx++, i, Value(manifest.manifest_path));

		// manifest_length: long - 501
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.manifest_length));

		// partition_spec_id: long - 502
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.partition_spec_id));

		// content: int - 517
		data.SetValue(col_idx++, i, Value::INTEGER(static_cast<int32_t>(manifest.content)));

		// sequence_number: long - 515
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.sequence_number));

		// min_sequence_number: long - 516
		if (!manifest.has_min_sequence_number) {
			//! Behavior copied from pyiceberg
			data.SetValue(col_idx++, i, Value::BIGINT(-1));
		} else {
			data.SetValue(col_idx++, i, Value::BIGINT(manifest.min_sequence_number));
		}

		// added_snapshot_id: long - 503
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.added_snapshot_id));

		// added_files_count: int - 504
		data.SetValue(col_idx++, i, Value::INTEGER(manifest.added_files_count));

		// existing_files_count: int - 505
		data.SetValue(col_idx++, i, Value::INTEGER(manifest.existing_files_count));

		// deleted_files_count: int - 506
		data.SetValue(col_idx++, i, Value::INTEGER(manifest.deleted_files_count));

		// added_rows_count: long - 512
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(manifest.added_rows_count)));

		// existing_rows_count: long - 513
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(manifest.existing_rows_count)));

		// deleted_rows_count: long - 514
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(manifest.deleted_rows_count)));

		// partitions: list<508: field_summary> - 507
		data.SetValue(col_idx++, i, manifest.partitions.ToValue());

		if (table_metadata.iceberg_version < 3) {
			continue;
		}

		bool has_first_row_id = manifest.has_first_row_id;
		int64_t first_row_id = manifest.first_row_id;
		if (!has_first_row_id && manifest.content == IcebergManifestContentType::DATA) {
			//! Assign first_row_id to old manifest_file entries
			first_row_id = next_row_id;
			has_first_row_id = true;
			next_row_id += manifest.added_rows_count;
			next_row_id += manifest.existing_rows_count;
		}

		if (has_first_row_id) {
			data.SetValue(col_idx++, i, first_row_id);
		}
	}
	data.SetCardinality(manifest_files.size());

	CopyInfo copy_info;
	copy_info.is_from = false;
	copy_info.options["root_name"].push_back(Value("manifest_file"));
	copy_info.options["field_ids"].push_back(Value::STRUCT(metadata.field_ids));

	CopyFunctionBindInput input(copy_info);
	input.file_extension = "avro";

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto bind_data = copy.copy_to_bind(context, input, metadata.names, metadata.types);

	auto global_state = copy.copy_to_initialize_global(context, *bind_data, manifest_list.GetPath());
	auto local_state = copy.copy_to_initialize_local(execution_context, *bind_data);

	copy.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, data);
	copy.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
	copy.copy_to_finalize(context, *bind_data, *global_state);
}

} // namespace manifest_list

Value IcebergManifestList::FieldSummaryFieldIds() {
	return manifest_list::FieldSummaryFieldIds();
}

unique_ptr<IcebergManifestList> IcebergManifestList::Load(const string &iceberg_path,
                                                          const IcebergTableMetadata &metadata,
                                                          const IcebergSnapshot &snapshot, ClientContext &context,
                                                          const IcebergOptions &options) {
	auto ret = make_uniq<IcebergManifestList>(snapshot.snapshot_id, snapshot.sequence_number, snapshot.manifest_list);

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;

	//! Read the entire manifest list, producing 'manifest_file' items
	auto scan = AvroScan::ScanManifestList(snapshot, metadata, context, manifest_list_full_path, ret->manifest_entries);
	auto manifest_list_reader = make_uniq<manifest_list::ManifestListReader>(*scan);

	while (!manifest_list_reader->Finished()) {
		manifest_list_reader->Read();
	}

	//! Read all manifest files, producing 'manifest_entry' items
	auto manifest_scan =
	    AvroScan::ScanManifest(snapshot, ret->manifest_entries, options, fs, iceberg_path, metadata, context);
	auto manifest_file_reader = make_uniq<manifest_file::ManifestReader>(*manifest_scan);

	while (!manifest_file_reader->Finished()) {
		manifest_file_reader->Read();
	}
	return ret;
}

} // namespace duckdb
