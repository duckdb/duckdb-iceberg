#include "manifest_reader.hpp"

namespace duckdb {

namespace manifest_file {

ManifestFileReader::ManifestFileReader(idx_t iceberg_version, bool skip_deleted)
    : BaseManifestReader(iceberg_version), skip_deleted(skip_deleted) {
}

void ManifestFileReader::SetSequenceNumber(sequence_number_t sequence_number_p) {
	sequence_number = sequence_number_p;
}

void ManifestFileReader::SetPartitionSpecID(int32_t partition_spec_id_p) {
	partition_spec_id = partition_spec_id_p;
}

idx_t ManifestFileReader::Read(idx_t count, vector<IcebergManifestEntry> &result) {
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

void ManifestFileReader::CreateVectorMapping(idx_t column_id, MultiFileColumnDefinition &column) {
	if (column.identifier.IsNull()) {
		throw InvalidConfigurationException("Column '%s' of the manifest file is missing a field_id!", column.name);
	}
	D_ASSERT(column.identifier.type().id() == LogicalTypeId::INTEGER);

	auto field_id = column.identifier.GetValue<int32_t>();
	if (field_id != DATA_FILE) {
		vector_mapping.emplace(field_id, ColumnIndex(column_id));
		return;
	}

	auto &type = column.type;
	if (type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("The 'data_file' of the manifest should be a STRUCT");
	}
	auto &children = column.children;
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];
		if (child.identifier.IsNull()) {
			throw InvalidConfigurationException("Column '%s.%s' of the manifest file is missing a field_id!",
			                                    column.name, child.name);
		}
		D_ASSERT(child.identifier.type().id() == LogicalTypeId::INTEGER);
		auto child_field_id = child.identifier.GetValue<int32_t>();

		vector<ColumnIndex> child_indexes;
		child_indexes.emplace_back(child_idx);

		vector_mapping.emplace(child_field_id, ColumnIndex(column_id, child_indexes));
		if (child_field_id == PARTITION) {
			for (idx_t partition_idx = 0; partition_idx < child.children.size(); partition_idx++) {
				auto &partition_field = child.children[partition_idx];

				auto partition_field_id = partition_field.identifier.GetValue<int32_t>();
				auto partition_child_indexes = child_indexes;
				partition_child_indexes.emplace_back(partition_idx);

				partition_fields.emplace(partition_field_id, ColumnIndex(column_id, partition_child_indexes));
			}
		}
	}
}

bool ManifestFileReader::ValidateVectorMapping() {
	static const int32_t V1_REQUIRED_FIELDS[] = {STATUS, FILE_PATH, FILE_FORMAT, RECORD_COUNT};
	static const idx_t V1_REQUIRED_FIELDS_SIZE = sizeof(V1_REQUIRED_FIELDS) / sizeof(int32_t);
	for (idx_t i = 0; i < V1_REQUIRED_FIELDS_SIZE; i++) {
		if (!vector_mapping.count(V1_REQUIRED_FIELDS[i])) {
			return false;
		}
	}

	static const int32_t V2_REQUIRED_FIELDS[] = {CONTENT};
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

static unordered_map<int32_t, Value> GetBounds(Vector &bounds, idx_t index) {
	auto &bounds_child = ListVector::GetEntry(bounds);
	auto keys = FlatVector::GetData<int32_t>(*StructVector::GetEntries(bounds_child)[0]);
	auto &values = *StructVector::GetEntries(bounds_child)[1];
	auto bounds_list = FlatVector::GetData<list_entry_t>(bounds);

	unordered_map<int32_t, Value> parsed_bounds;

	auto &validity = FlatVector::Validity(bounds);
	if (!validity.RowIsValid(index)) {
		return parsed_bounds;
	}

	auto list_entry = bounds_list[index];
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		parsed_bounds[keys[list_idx]] = values.GetValue(list_idx);
	}
	return parsed_bounds;
}

static unordered_map<int32_t, int64_t> GetCounts(Vector &counts, idx_t index) {
	auto &counts_child = ListVector::GetEntry(counts);
	auto keys = FlatVector::GetData<int32_t>(*StructVector::GetEntries(counts_child)[0]);
	auto values = FlatVector::GetData<int64_t>(*StructVector::GetEntries(counts_child)[1]);
	auto counts_list = FlatVector::GetData<list_entry_t>(counts);

	unordered_map<int32_t, int64_t> parsed_counts;

	auto &validity = FlatVector::Validity(counts);
	if (!validity.RowIsValid(index)) {
		return parsed_counts;
	}

	auto list_entry = counts_list[index];
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		parsed_counts[keys[list_idx]] = values[list_idx];
	}
	return parsed_counts;
}

static vector<int32_t> GetEqualityIds(Vector &equality_ids, idx_t index) {
	vector<int32_t> result;

	if (!FlatVector::Validity(equality_ids).RowIsValid(index)) {
		return result;
	}
	auto &equality_ids_child = ListVector::GetEntry(equality_ids);
	auto equality_ids_data = FlatVector::GetData<int32_t>(equality_ids_child);
	auto equality_ids_list = FlatVector::GetData<list_entry_t>(equality_ids);
	auto list_entry = equality_ids_list[index];

	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = list_entry.offset + j;
		result.push_back(equality_ids_data[list_idx]);
	}

	return result;
}

idx_t ManifestFileReader::ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result) {
	D_ASSERT(offset < chunk.size());
	D_ASSERT(offset + count <= chunk.size());

	auto status = FlatVector::GetData<int32_t>(chunk.data[vector_mapping.at(STATUS).GetPrimaryIndex()]);

	auto file_path_idx = vector_mapping.at(FILE_PATH);
	auto data_file_idx = file_path_idx.GetPrimaryIndex();
	auto &child_entries = StructVector::GetEntries(chunk.data[data_file_idx]);
	D_ASSERT(vector_mapping.at(FILE_FORMAT).GetPrimaryIndex() == data_file_idx);
	D_ASSERT(vector_mapping.at(RECORD_COUNT).GetPrimaryIndex() == data_file_idx);
	if (iceberg_version > 1) {
		D_ASSERT(vector_mapping.at(CONTENT).GetPrimaryIndex() == data_file_idx);
	}
	optional_ptr<Vector> equality_ids;
	optional_ptr<Vector> sequence_number;
	int32_t *content;

	auto partition_idx = vector_mapping.at(PARTITION);
	if (iceberg_version > 1) {
		auto equality_ids_it = vector_mapping.find(EQUALITY_IDS);
		if (equality_ids_it != vector_mapping.end()) {
			equality_ids = *child_entries[equality_ids_it->second.GetChildIndex(0).GetPrimaryIndex()];
		}
		auto sequence_number_it = vector_mapping.find(SEQUENCE_NUMBER);
		if (sequence_number_it != vector_mapping.end()) {
			sequence_number = chunk.data[sequence_number_it->second.GetPrimaryIndex()];
		}
		content =
		    FlatVector::GetData<int32_t>(*child_entries[vector_mapping.at(CONTENT).GetChildIndex(0).GetPrimaryIndex()]);
	}

	auto file_path = FlatVector::GetData<string_t>(*child_entries[file_path_idx.GetChildIndex(0).GetPrimaryIndex()]);
	auto file_format = FlatVector::GetData<string_t>(
	    *child_entries[vector_mapping.at(FILE_FORMAT).GetChildIndex(0).GetPrimaryIndex()]);
	auto record_count = FlatVector::GetData<int64_t>(
	    *child_entries[vector_mapping.at(RECORD_COUNT).GetChildIndex(0).GetPrimaryIndex()]);
	auto file_size_in_bytes = FlatVector::GetData<int64_t>(
	    *child_entries[vector_mapping.at(FILE_SIZE_IN_BYTES).GetChildIndex(0).GetPrimaryIndex()]);
	optional_ptr<Vector> column_sizes;
	optional_ptr<Vector> lower_bounds;
	optional_ptr<Vector> upper_bounds;
	optional_ptr<Vector> value_counts;
	optional_ptr<Vector> null_value_counts;
	optional_ptr<Vector> nan_value_counts;

	auto column_sizes_it = vector_mapping.find(COLUMN_SIZES);
	if (column_sizes_it != vector_mapping.end()) {
		column_sizes = *child_entries[column_sizes_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto lower_bounds_it = vector_mapping.find(LOWER_BOUNDS);
	if (lower_bounds_it != vector_mapping.end()) {
		lower_bounds = *child_entries[lower_bounds_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto upper_bounds_it = vector_mapping.find(UPPER_BOUNDS);
	if (upper_bounds_it != vector_mapping.end()) {
		upper_bounds = *child_entries[upper_bounds_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto value_counts_it = vector_mapping.find(VALUE_COUNTS);
	if (value_counts_it != vector_mapping.end()) {
		value_counts = *child_entries[value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto null_value_counts_it = vector_mapping.find(NULL_VALUE_COUNTS);
	if (null_value_counts_it != vector_mapping.end()) {
		null_value_counts = *child_entries[null_value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto nan_value_counts_it = vector_mapping.find(NAN_VALUE_COUNTS);
	if (nan_value_counts_it != vector_mapping.end()) {
		nan_value_counts = *child_entries[nan_value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto &partition_vec = child_entries[partition_idx.GetChildIndex(0).GetPrimaryIndex()];
	unordered_map<int32_t, reference<Vector>> partition_vectors;
	if (partition_vec->GetType().id() != LogicalTypeId::SQLNULL) {
		auto &partition_children = StructVector::GetEntries(*partition_vec);
		for (auto &it : partition_fields) {
			auto partition_field_idx = it.second.GetChildIndex(1).GetPrimaryIndex();
			partition_vectors.emplace(it.first, *partition_children[partition_field_idx]);
		}
	}

	optional_ptr<Vector> referenced_data_file;
	optional_ptr<Vector> content_offset;
	optional_ptr<Vector> content_size_in_bytes;

	auto referenced_data_file_it = vector_mapping.find(REFERENCED_DATA_FILE);
	if (referenced_data_file_it != vector_mapping.end()) {
		referenced_data_file = *child_entries[referenced_data_file_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto content_offset_it = vector_mapping.find(CONTENT_OFFSET);
	if (content_offset_it != vector_mapping.end()) {
		content_offset = *child_entries[content_offset_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto content_size_in_bytes_it = vector_mapping.find(CONTENT_SIZE_IN_BYTES);
	if (content_size_in_bytes_it != vector_mapping.end()) {
		content_size_in_bytes = *child_entries[content_size_in_bytes_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}

	idx_t produced = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status[index];
		if (this->skip_deleted && entry.status == IcebergManifestEntryStatusType::DELETED) {
			//! Skip this entry, we don't care about deleted entries
			continue;
		}

		entry.file_path = file_path[index].GetString();
		entry.file_format = file_format[index].GetString();
		entry.record_count = record_count[index];
		entry.file_size_in_bytes = file_size_in_bytes[index];

		if (lower_bounds && upper_bounds) {
			entry.lower_bounds = GetBounds(*lower_bounds, index);
			entry.upper_bounds = GetBounds(*upper_bounds, index);
		}
		if (column_sizes) {
			entry.column_sizes = GetCounts(*column_sizes, index);
		}
		if (value_counts) {
			entry.value_counts = GetCounts(*value_counts, index);
		}
		if (null_value_counts) {
			entry.null_value_counts = GetCounts(*null_value_counts, index);
		}
		if (nan_value_counts) {
			entry.nan_value_counts = GetCounts(*nan_value_counts, index);
		}

		if (referenced_data_file && FlatVector::Validity(*referenced_data_file).RowIsValid(index)) {
			entry.referenced_data_file = FlatVector::GetData<string_t>(*referenced_data_file)[index].GetString();
		}
		if (content_offset && FlatVector::Validity(*content_offset).RowIsValid(index)) {
			entry.content_offset = content_offset->GetValue(index);
		}
		if (content_size_in_bytes && FlatVector::Validity(*content_size_in_bytes).RowIsValid(index)) {
			entry.content_size_in_bytes = content_size_in_bytes->GetValue(index);
		}

		if (iceberg_version > 1) {
			entry.content = (IcebergManifestEntryContentType)content[index];
			if (equality_ids) {
				entry.equality_ids = GetEqualityIds(*equality_ids, index);
			}

			if (sequence_number) {
				auto sequence_numbers = FlatVector::GetData<int64_t>(*sequence_number);
				if (FlatVector::Validity(*sequence_number).RowIsValid(index)) {
					entry.sequence_number = sequence_numbers[index];
				} else {
					//! Value should only be NULL for ADDED manifest entries, to support inheritance
					D_ASSERT(entry.status == IcebergManifestEntryStatusType::ADDED);
					entry.sequence_number = this->sequence_number;
				}
			} else {
				//! Default to sequence number 0
				//! (The 'manifest_file' should also have defaulted to 0)
				D_ASSERT(this->sequence_number == 0);
				entry.sequence_number = 0;
			}
		} else {
			entry.sequence_number = this->sequence_number;
			entry.content = IcebergManifestEntryContentType::DATA;
		}

		entry.partition_spec_id = this->partition_spec_id;
		for (auto &it : partition_vectors) {
			auto field_id = it.first;
			auto &partition_vector = it.second.get();

			entry.partition_values.emplace_back(field_id, partition_vector.GetValue(index));
		}
		produced++;
		result.push_back(entry);
	}
	return produced;
}

} // namespace manifest_file

} // namespace duckdb
