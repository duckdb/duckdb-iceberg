#include "iceberg_manifest.hpp"

//! Iceberg Manifest scan routines

namespace duckdb {

static void ManifestNameMapping(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	name_to_vec[name] = ColumnIndex(column_id);
}

void IcebergManifestV1::ProduceEntries(DataChunk &input, const case_insensitive_map_t<ColumnIndex> &name_to_vec,
											vector<entry_type> &result) {
	auto manifest_path = FlatVector::GetData<string_t>(input.data[name_to_vec.at("manifest_path").GetPrimaryIndex()]);

	for (idx_t i = 0; i < input.size(); i++) {
		IcebergManifest manifest;
		manifest.manifest_path = manifest_path[i].GetString();
		manifest.content = IcebergManifestContentType::DATA;
		manifest.sequence_number = 0;

		result.push_back(manifest);
	}
}

bool IcebergManifestV1::VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	if (!name_to_vec.count("manifest_path")) {
		return false;
	}
	return true;
}

void IcebergManifestV1::PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	ManifestNameMapping(column_id, type, name, name_to_vec);
}

void IcebergManifestV2::ProduceEntries(DataChunk &input, const case_insensitive_map_t<ColumnIndex> &name_to_vec,
											vector<entry_type> &result) {
	auto manifest_path = FlatVector::GetData<string_t>(input.data[name_to_vec.at("manifest_path").GetPrimaryIndex()]);
	auto content = FlatVector::GetData<int32_t>(input.data[name_to_vec.at("content").GetPrimaryIndex()]);
	auto sequence_number = FlatVector::GetData<int64_t>(input.data[name_to_vec.at("sequence_number").GetPrimaryIndex()]);

	for (idx_t i = 0; i < input.size(); i++) {
		IcebergManifest manifest;
		manifest.manifest_path = manifest_path[i].GetString();
		manifest.content = IcebergManifestContentType(content[i]);
		manifest.sequence_number = sequence_number[i];

		result.push_back(manifest);
	}
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

void IcebergManifestV2::PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	ManifestNameMapping(column_id, type, name, name_to_vec);
}

//! Iceberg Manifest Entry scan routines

static void EntryNameMapping(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec) {
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

void IcebergManifestEntryV1::ProduceEntries(DataChunk &input, const case_insensitive_map_t<ColumnIndex> &name_to_vec,
													vector<entry_type> &result) {
	auto status = FlatVector::GetData<int32_t>(input.data[name_to_vec.at("status").GetPrimaryIndex()]);

	auto file_path_idx = name_to_vec.at("file_path");
	auto data_file_idx = file_path_idx.GetPrimaryIndex();
	auto &child_entries = StructVector::GetEntries(input.data[data_file_idx]);
	D_ASSERT(name_to_vec.at("file_format").GetPrimaryIndex());
	D_ASSERT(name_to_vec.at("record_count").GetPrimaryIndex());

	auto file_path = FlatVector::GetData<string_t>(*child_entries[file_path_idx.GetChildIndex(0).GetPrimaryIndex()]);
	auto file_format = FlatVector::GetData<string_t>(*child_entries[name_to_vec.at("file_format").GetChildIndex(0).GetPrimaryIndex()]);
	auto record_count = FlatVector::GetData<int64_t>(*child_entries[name_to_vec.at("record_count").GetChildIndex(0).GetPrimaryIndex()]);

	for (idx_t i = 0; i < input.size(); i++) {
		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status[i];
		entry.content = IcebergManifestEntryContentType::DATA;
		entry.file_path = file_path[i].GetString();
		entry.file_format = file_format[i].GetString();
		entry.record_count = record_count[i];

		result.push_back(entry);
	}
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

void IcebergManifestEntryV1::PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	EntryNameMapping(column_id, type, name, name_to_vec);
}

void IcebergManifestEntryV2::ProduceEntries(DataChunk &input, const case_insensitive_map_t<ColumnIndex> &name_to_vec,
													vector<entry_type> &result) {
	auto status = FlatVector::GetData<int32_t>(input.data[name_to_vec.at("status").GetPrimaryIndex()]);

	auto file_path_idx = name_to_vec.at("file_path");
	auto data_file_idx = file_path_idx.GetPrimaryIndex();
	auto &child_entries = StructVector::GetEntries(input.data[data_file_idx]);
	D_ASSERT(name_to_vec.at("file_format").GetPrimaryIndex() == data_file_idx);
	D_ASSERT(name_to_vec.at("record_count").GetPrimaryIndex() == data_file_idx);
	D_ASSERT(name_to_vec.at("content").GetPrimaryIndex() == data_file_idx);

	auto content = FlatVector::GetData<int32_t>(*child_entries[name_to_vec.at("content").GetChildIndex(0).GetPrimaryIndex()]);
	auto file_path = FlatVector::GetData<string_t>(*child_entries[file_path_idx.GetChildIndex(0).GetPrimaryIndex()]);
	auto file_format = FlatVector::GetData<string_t>(*child_entries[name_to_vec.at("file_format").GetChildIndex(0).GetPrimaryIndex()]);
	auto record_count = FlatVector::GetData<int64_t>(*child_entries[name_to_vec.at("record_count").GetChildIndex(0).GetPrimaryIndex()]);

	for (idx_t i = 0; i < input.size(); i++) {
		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status[i];
		entry.content = (IcebergManifestEntryContentType)content[i];
		entry.file_path = file_path[i].GetString();
		entry.file_format = file_format[i].GetString();
		entry.record_count = record_count[i];

		result.push_back(entry);
	}
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

void IcebergManifestEntryV2::PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec) {
	EntryNameMapping(column_id, type, name, name_to_vec);
}

} // namespace duckdb

