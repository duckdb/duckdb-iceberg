#include "manifest_reader.hpp"
#include "iceberg_manifest.hpp"
#include "duckdb/main/database.hpp"

//! Iceberg Manifest scan routines

namespace duckdb {

static void ManifestNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	name_to_vec[name] = ColumnIndex(column_id);
}

idx_t IcebergManifestV1::ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input,
                                        vector<entry_type> &result) {
	auto &name_to_vec = input.name_to_vec;
	auto manifest_path = FlatVector::GetData<string_t>(chunk.data[name_to_vec.at("manifest_path").GetPrimaryIndex()]);

	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifest manifest;
		manifest.manifest_path = manifest_path[index].GetString();
		manifest.content = IcebergManifestContentType::DATA;
		manifest.sequence_number = 0;

		result.push_back(manifest);
	}
	return count;
}

bool IcebergManifestV1::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!name_to_vec.count("manifest_path")) {
		return false;
	}
	return true;
}

void IcebergManifestV1::PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                            case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	ManifestNameMapping(column_id, type, name, name_to_vec);
}

idx_t IcebergManifestV2::ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input,
                                        vector<entry_type> &result) {
	auto &name_to_vec = input.name_to_vec;
	auto manifest_path = FlatVector::GetData<string_t>(chunk.data[name_to_vec.at("manifest_path").GetPrimaryIndex()]);
	auto content = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("content").GetPrimaryIndex()]);
	auto sequence_number =
	    FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("sequence_number").GetPrimaryIndex()]);

	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifest manifest;
		manifest.manifest_path = manifest_path[index].GetString();
		manifest.content = IcebergManifestContentType(content[index]);
		manifest.sequence_number = sequence_number[index];

		result.push_back(manifest);
	}
	return count;
}

bool IcebergManifestV2::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!IcebergManifestV1::VerifySchema(name_to_vec)) {
		return false;
	}
	if (!name_to_vec.count("content")) {
		return false;
	}
	return true;
}

void IcebergManifestV2::PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                            case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	ManifestNameMapping(column_id, type, name, name_to_vec);
}

//! Iceberg Manifest Entry scan routines

static void EntryNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                             case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	auto lname = StringUtil::Lower(name);
	if (lname != "data_file") {
		name_to_vec[lname] = ColumnIndex(column_id);
		return;
	}

	if (type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("The 'data_file' of the manifest should be a STRUCT");
	}
	auto &children = StructType::GetChildTypes(type);
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];
		auto child_name = StringUtil::Lower(child.first);

		name_to_vec[child_name] = ColumnIndex(column_id, {ColumnIndex(child_idx)});
	}
}

idx_t IcebergManifestEntryV1::ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count,
                                             const ManifestReaderInput &input, vector<entry_type> &result) {
	auto &name_to_vec = input.name_to_vec;
	auto status = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("status").GetPrimaryIndex()]);

	auto file_path_idx = name_to_vec.at("file_path");
	auto data_file_idx = file_path_idx.GetPrimaryIndex();
	auto &child_entries = StructVector::GetEntries(chunk.data[data_file_idx]);
	D_ASSERT(name_to_vec.at("file_format").GetPrimaryIndex());
	D_ASSERT(name_to_vec.at("record_count").GetPrimaryIndex());

	auto file_path = FlatVector::GetData<string_t>(*child_entries[file_path_idx.GetChildIndex(0).GetPrimaryIndex()]);
	auto file_format =
	    FlatVector::GetData<string_t>(*child_entries[name_to_vec.at("file_format").GetChildIndex(0).GetPrimaryIndex()]);
	auto record_count =
	    FlatVector::GetData<int64_t>(*child_entries[name_to_vec.at("record_count").GetChildIndex(0).GetPrimaryIndex()]);

	idx_t produced = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status[index];
		entry.content = IcebergManifestEntryContentType::DATA;
		entry.file_path = file_path[index].GetString();
		entry.file_format = file_format[index].GetString();
		entry.record_count = record_count[index];

		if (input.skip_deleted && entry.status == IcebergManifestEntryStatusType::DELETED) {
			//! Skip this entry, we don't care about deleted entries
			continue;
		}
		produced++;
		result.push_back(entry);
	}
	return produced;
}

bool IcebergManifestEntryV1::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!name_to_vec.count("status")) {
		return false;
	}
	if (!name_to_vec.count("file_path")) {
		return false;
	}
	if (!name_to_vec.count("file_format")) {
		return false;
	}
	if (!name_to_vec.count("record_count")) {
		return false;
	}
	return true;
}

void IcebergManifestEntryV1::PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                                 case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	EntryNameMapping(column_id, type, name, name_to_vec);
}

idx_t IcebergManifestEntryV2::ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count,
                                             const ManifestReaderInput &input, vector<entry_type> &result) {
	auto &name_to_vec = input.name_to_vec;
	auto status = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("status").GetPrimaryIndex()]);

	auto file_path_idx = name_to_vec.at("file_path");
	auto data_file_idx = file_path_idx.GetPrimaryIndex();
	auto &child_entries = StructVector::GetEntries(chunk.data[data_file_idx]);
	D_ASSERT(name_to_vec.at("file_format").GetPrimaryIndex() == data_file_idx);
	D_ASSERT(name_to_vec.at("record_count").GetPrimaryIndex() == data_file_idx);
	D_ASSERT(name_to_vec.at("content").GetPrimaryIndex() == data_file_idx);

	auto content =
	    FlatVector::GetData<int32_t>(*child_entries[name_to_vec.at("content").GetChildIndex(0).GetPrimaryIndex()]);
	auto file_path = FlatVector::GetData<string_t>(*child_entries[file_path_idx.GetChildIndex(0).GetPrimaryIndex()]);
	auto file_format =
	    FlatVector::GetData<string_t>(*child_entries[name_to_vec.at("file_format").GetChildIndex(0).GetPrimaryIndex()]);
	auto record_count =
	    FlatVector::GetData<int64_t>(*child_entries[name_to_vec.at("record_count").GetChildIndex(0).GetPrimaryIndex()]);

	idx_t produced = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status[index];
		entry.content = (IcebergManifestEntryContentType)content[index];
		entry.file_path = file_path[index].GetString();
		entry.file_format = file_format[index].GetString();
		entry.record_count = record_count[index];

		if (input.skip_deleted && entry.status == IcebergManifestEntryStatusType::DELETED) {
			//! Skip this entry, we don't care about deleted entries
			continue;
		}
		produced++;
		result.push_back(entry);
	}
	return produced;
}

bool IcebergManifestEntryV2::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!IcebergManifestEntryV1::VerifySchema(name_to_vec)) {
		return false;
	}
	if (!name_to_vec.count("content")) {
		return false;
	}
	return true;
}

void IcebergManifestEntryV2::PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name,
                                                 case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	EntryNameMapping(column_id, type, name, name_to_vec);
}

AvroScan::AvroScan(const string &scan_name, ClientContext &context, const string &path) : context(context) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	auto &avro_scan_entry = ExtensionUtil::GetTableFunction(instance, "read_avro");
	avro_scan = avro_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = scan_name;
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	bind_data = avro_scan->bind(context, bind_input, return_types, return_names);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	global_state = avro_scan->init_global(context, input);
	local_state = avro_scan->init_local(execution_context, input, global_state.get());
}

bool AvroScan::GetNext(DataChunk &result) {
	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	avro_scan->function(context, function_input, result);

	idx_t count = result.size();
	for (auto &vec : result.data) {
		vec.Flatten(count);
	}
	if (count == 0) {
		finished = true;
		return false;
	}
	return true;
}

void AvroScan::InitializeChunk(DataChunk &chunk) {
	chunk.Initialize(context, return_types, STANDARD_VECTOR_SIZE);
}

bool AvroScan::Finished() const {
	return finished;
}

} // namespace duckdb
