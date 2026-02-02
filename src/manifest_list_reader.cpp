#include "manifest_reader.hpp"
#include "include/metadata/iceberg_manifest_list.hpp"

namespace duckdb {

namespace manifest_list {

ManifestListReader::ManifestListReader(idx_t iceberg_version) : BaseManifestReader(iceberg_version) {
}

idx_t ManifestListReader::Read(idx_t count, vector<IcebergManifestFile> &result) {
	if (!scan || finished) {
		return 0;
	}

	idx_t total_read = 0;
	idx_t total_added = 0;
	while (total_read < count && !finished) {
		auto tuples = ScanInternal(count - total_read);
		if (finished) {
			break;
		}
		total_added += ReadChunk(offset, tuples, result);
		offset += tuples;
		total_read += tuples;
	}
	return total_added;
}

idx_t ManifestListReader::ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestFile> &result) {
	D_ASSERT(offset < chunk.size());
	D_ASSERT(offset + count <= chunk.size());

	auto manifest_path = FlatVector::GetData<string_t>(chunk.data[vector_mapping.at(MANIFEST_PATH).GetPrimaryIndex()]);
	auto manifest_length =
	    FlatVector::GetData<int64_t>(chunk.data[vector_mapping.at(MANIFEST_LENGTH).GetPrimaryIndex()]);
	auto added_snapshot_id =
	    FlatVector::GetData<int64_t>(chunk.data[vector_mapping.at(ADDED_SNAPSHOT_ID).GetPrimaryIndex()]);
	auto partition_spec_id =
	    FlatVector::GetData<int32_t>(chunk.data[vector_mapping.at(PARTITION_SPEC_ID).GetPrimaryIndex()]);

	int32_t *content = nullptr;
	int64_t *sequence_number = nullptr;
	int64_t *min_sequence_number = nullptr;
	int32_t *added_files_count = nullptr;
	int32_t *existing_files_count = nullptr;
	int32_t *deleted_files_count = nullptr;
	int64_t *added_rows_count = nullptr;
	int64_t *existing_rows_count = nullptr;
	int64_t *deleted_rows_count = nullptr;

	if (iceberg_version > 1) {
		//! 'content'
		content = FlatVector::GetData<int32_t>(chunk.data[vector_mapping.at(CONTENT).GetPrimaryIndex()]);
		//! 'sequence_number'
		sequence_number =
		    FlatVector::GetData<int64_t>(chunk.data[vector_mapping.at(SEQUENCE_NUMBER).GetPrimaryIndex()]);
		//! 'min_sequence_number'
		min_sequence_number =
		    FlatVector::GetData<int64_t>(chunk.data[vector_mapping.at(MIN_SEQUENCE_NUMBER).GetPrimaryIndex()]);
		//! 'added_files_count'
		added_files_count =
		    FlatVector::GetData<int32_t>(chunk.data[vector_mapping.at(ADDED_FILES_COUNT).GetPrimaryIndex()]);
		//! 'existing_files_count'
		existing_files_count =
		    FlatVector::GetData<int32_t>(chunk.data[vector_mapping.at(EXISTING_FILES_COUNT).GetPrimaryIndex()]);
		//! 'deleted_files_count'
		deleted_files_count =
		    FlatVector::GetData<int32_t>(chunk.data[vector_mapping.at(DELETED_FILES_COUNT).GetPrimaryIndex()]);
		//! 'added_rows_count'
		added_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[vector_mapping.at(ADDED_ROWS_COUNT).GetPrimaryIndex()]);
		//! 'existing_rows_count'
		existing_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[vector_mapping.at(EXISTING_ROWS_COUNT).GetPrimaryIndex()]);
		//! 'deleted_rows_count'
		deleted_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[vector_mapping.at(DELETED_ROWS_COUNT).GetPrimaryIndex()]);
	}

	//! 'partitions'
	optional_ptr<Vector> partitions;
	list_entry_t *field_summary = nullptr;
	optional_ptr<Vector> contains_null = nullptr;
	optional_ptr<Vector> contains_nan = nullptr;
	optional_ptr<Vector> lower_bound = nullptr;
	optional_ptr<Vector> upper_bound = nullptr;

	bool *contains_null_data = nullptr;
	bool *contains_nan_data = nullptr;

	auto partitions_it = vector_mapping.find(PARTITIONS);
	if (partitions_it != vector_mapping.end()) {
		partitions = chunk.data[partitions_it->second.GetPrimaryIndex()];

		auto &field_summary_vec = ListVector::GetEntry(*partitions);
		field_summary = FlatVector::GetData<list_entry_t>(*partitions);
		auto &child_vectors = StructVector::GetEntries(field_summary_vec);

		contains_null =
		    child_vectors[vector_mapping.at(FIELD_SUMMARY_CONTAINS_NULL).GetChildIndex(0).GetPrimaryIndex()].get();
		contains_null_data = FlatVector::GetData<bool>(*contains_null);
		contains_nan =
		    child_vectors[vector_mapping.at(FIELD_SUMMARY_CONTAINS_NAN).GetChildIndex(0).GetPrimaryIndex()].get();
		contains_nan_data = FlatVector::GetData<bool>(*contains_nan);
		lower_bound =
		    child_vectors[vector_mapping.at(FIELD_SUMMARY_LOWER_BOUND).GetChildIndex(0).GetPrimaryIndex()].get();
		upper_bound =
		    child_vectors[vector_mapping.at(FIELD_SUMMARY_UPPER_BOUND).GetChildIndex(0).GetPrimaryIndex()].get();
	}

	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestFile manifest(manifest_path[index].GetString());
		manifest.manifest_length = manifest_length[index];
		manifest.added_snapshot_id = added_snapshot_id[index];
		manifest.partition_spec_id = partition_spec_id[index];
		//! This flag is only used for writing, not for reading
		manifest.has_min_sequence_number = true;

		if (iceberg_version > 1) {
			manifest.content = IcebergManifestContentType(content[index]);
			manifest.sequence_number = sequence_number[index];
			manifest.min_sequence_number = min_sequence_number[index];
			manifest.added_files_count = added_files_count[index];
			manifest.existing_files_count = existing_files_count[index];
			manifest.deleted_files_count = deleted_files_count[index];
			manifest.added_rows_count = added_rows_count[index];
			manifest.existing_rows_count = existing_rows_count[index];
			manifest.deleted_rows_count = deleted_rows_count[index];
		} else {
			manifest.content = IcebergManifestContentType::DATA;
			manifest.sequence_number = 0;
			manifest.min_sequence_number = 0;
			//! NOTE: these are optional in v1, they *could* be written
			manifest.added_files_count = 0;
			manifest.existing_files_count = 0;
			manifest.deleted_files_count = 0;
			manifest.added_rows_count = 0;
			manifest.existing_rows_count = 0;
			manifest.deleted_rows_count = 0;
		}

		if (field_summary && FlatVector::Validity(*partitions).RowIsValid(index)) {
			manifest.partitions.has_partitions = true;
			auto &summaries = manifest.partitions.field_summary;
			auto list_entry = field_summary[index];
			for (idx_t j = 0; j < list_entry.length; j++) {
				FieldSummary summary;
				auto list_idx = list_entry.offset + j;
				if (contains_null && FlatVector::Validity(*contains_null).RowIsValid(list_idx)) {
					summary.contains_null = contains_null_data[list_idx];
				}
				if (contains_nan && FlatVector::Validity(*contains_nan).RowIsValid(list_idx)) {
					summary.contains_nan = contains_nan_data[list_idx];
				}
				if (lower_bound) {
					summary.lower_bound = lower_bound->GetValue(list_idx);
				}
				if (upper_bound) {
					summary.upper_bound = upper_bound->GetValue(list_idx);
				}
				summaries.push_back(summary);
			}
		}
		result.push_back(manifest);
	}
	return count;
}

bool ManifestListReader::ValidateVectorMapping() {
	static const int32_t V1_REQUIRED_FIELDS[] = {MANIFEST_PATH, MANIFEST_LENGTH, ADDED_SNAPSHOT_ID, PARTITION_SPEC_ID};
	static const idx_t V1_REQUIRED_FIELDS_SIZE = sizeof(V1_REQUIRED_FIELDS) / sizeof(int32_t);
	for (idx_t i = 0; i < V1_REQUIRED_FIELDS_SIZE; i++) {
		if (!vector_mapping.count(V1_REQUIRED_FIELDS[i])) {
			return false;
		}
	}

	static const int32_t V2_REQUIRED_FIELDS[] = {CONTENT,           SEQUENCE_NUMBER,      MIN_SEQUENCE_NUMBER,
	                                             ADDED_FILES_COUNT, EXISTING_FILES_COUNT, DELETED_FILES_COUNT,
	                                             ADDED_ROWS_COUNT,  EXISTING_ROWS_COUNT,  DELETED_ROWS_COUNT};
	static const idx_t V2_REQUIRED_FIELDS_SIZE = sizeof(V2_REQUIRED_FIELDS) / sizeof(int32_t);
	if (iceberg_version >= 2) {
		for (idx_t i = 0; i < V2_REQUIRED_FIELDS_SIZE; i++) {
			if (!vector_mapping.count(V2_REQUIRED_FIELDS[i])) {
				return false;
			}
		}
	}
	return true;
}

void ManifestListReader::CreateVectorMapping(idx_t column_id, MultiFileColumnDefinition &column) {
	if (column.identifier.IsNull()) {
		throw InvalidConfigurationException("Column '%s' of the manifest list is missing a field_id!", column.name);
	}
	D_ASSERT(column.identifier.type().id() == LogicalTypeId::INTEGER);

	auto field_id = column.identifier.GetValue<int32_t>();
	vector_mapping.emplace(field_id, ColumnIndex(column_id));
	if (field_id != PARTITIONS) {
		return;
	}

	auto &type = column.type;
	if (type.id() != LogicalTypeId::LIST) {
		throw InvalidInputException("The 'partitions' of the manifest entry should be a STRUCT(...)[]");
	}
	D_ASSERT(column.children.size() == 1);
	auto &field_summary = column.children[0];

	auto &children = field_summary.children;
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];
		D_ASSERT(!child.identifier.IsNull() && child.identifier.type().id() == LogicalTypeId::INTEGER);
		auto child_field_id = child.identifier.GetValue<int32_t>();

		vector_mapping.emplace(child_field_id, ColumnIndex(column_id, {ColumnIndex(child_idx)}));
	}
}

} // namespace manifest_list

} // namespace duckdb
