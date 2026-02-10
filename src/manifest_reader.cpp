#include "manifest_reader.hpp"

namespace duckdb {

namespace manifest_file {

ManifestReader::ManifestReader(idx_t iceberg_version, bool skip_deleted)
    : BaseManifestReader(iceberg_version), skip_deleted(skip_deleted) {
}

idx_t ManifestReader::Read(idx_t count, vector<IcebergManifestEntry> &result) {
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

idx_t ManifestReader::ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result) {
	D_ASSERT(offset < chunk.size());
	D_ASSERT(offset + count <= chunk.size());
	auto &scan_info = GetScanInfo().Cast<IcebergManifestFileScanInfo>();

	//! NOTE: the order of these columns is defined by the order that they are produced in BuildManifestSchema
	//! see `iceberg_avro_multi_file_reader.cpp`
	idx_t vector_index = 0;
	auto &status = chunk.data[vector_index++];
	auto &snapshot_id = chunk.data[vector_index++];
	auto &sequence_number = chunk.data[vector_index++];
	auto &file_sequence_number = chunk.data[vector_index++];
	auto &data_file = chunk.data[vector_index++];

	auto &partition_spec_id = chunk.data[vector_index++];
	auto &manifest_file_sequence_number = chunk.data[vector_index++];
	auto &manifest_file_index = chunk.data[vector_index++];

	idx_t entry_index = 0;
	auto &data_file_entries = StructVector::GetEntries(data_file);
	optional_ptr<Vector> content;
	if (iceberg_version >= 2) {
		content = *data_file_entries[entry_index++];
	}
	auto &file_path = *data_file_entries[entry_index++];
	auto &file_format = *data_file_entries[entry_index++];
	auto &partition = *data_file_entries[entry_index++];
	auto &record_count = *data_file_entries[entry_index++];
	auto &file_size_in_bytes = *data_file_entries[entry_index++];
	auto &column_sizes = *data_file_entries[entry_index++];
	auto &value_counts = *data_file_entries[entry_index++];
	auto &null_value_counts = *data_file_entries[entry_index++];
	auto &nan_value_counts = *data_file_entries[entry_index++];
	auto &lower_bounds = *data_file_entries[entry_index++];
	auto &upper_bounds = *data_file_entries[entry_index++];
	auto &split_offsets = *data_file_entries[entry_index++];
	auto &equality_ids = *data_file_entries[entry_index++];
	auto &sort_order_id = *data_file_entries[entry_index++];
	optional_ptr<Vector> first_row_id;
	if (iceberg_version >= 3) {
		first_row_id = *data_file_entries[entry_index++];
	}
	optional_ptr<Vector> referenced_data_file;
	if (iceberg_version >= 2) {
		referenced_data_file = *data_file_entries[entry_index++];
	}
	optional_ptr<Vector> content_offset;
	optional_ptr<Vector> content_size_in_bytes;
	if (iceberg_version >= 3) {
		content_offset = *data_file_entries[entry_index++];
		content_size_in_bytes = *data_file_entries[entry_index++];
	}

	auto status_data = FlatVector::GetData<int32_t>(status);
	auto snapshot_id_data = FlatVector::GetData<int64_t>(snapshot_id);
	auto sequence_number_data = FlatVector::GetData<int64_t>(sequence_number);
	auto file_sequence_number_data = FlatVector::GetData<int64_t>(file_sequence_number);
	auto partition_spec_id_data = FlatVector::GetData<int32_t>(partition_spec_id);
	auto manifest_file_sequence_number_data = FlatVector::GetData<int64_t>(manifest_file_sequence_number);
	auto manifest_file_index_data = FlatVector::GetData<uint64_t>(manifest_file_index);

	int32_t *content_data;
	if (iceberg_version >= 2) {
		content_data = FlatVector::GetData<int32_t>(*content);
	}
	auto file_path_data = FlatVector::GetData<string_t>(file_path);
	auto file_format_data = FlatVector::GetData<string_t>(file_format);
	// auto partition_data = FlatVector::GetData<int32_t>(partition);
	auto record_count_data = FlatVector::GetData<int64_t>(record_count);
	auto file_size_in_bytes_data = FlatVector::GetData<int64_t>(file_size_in_bytes);

	vector<std::pair<int32_t, reference<Vector>>> partition_vectors;
	if (partition.GetType().id() != LogicalTypeId::SQLNULL) {
		auto &partition_children = StructVector::GetEntries(partition);
		D_ASSERT(partition_children.size() == scan_info.partition_field_id_to_type.size());
		idx_t child_index = 0;
		for (auto &it : scan_info.partition_field_id_to_type) {
			partition_vectors.emplace_back(it.first, *partition_children[child_index++]);
		}
	}

	idx_t produced = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status_data[index];
		if (this->skip_deleted && entry.status == IcebergManifestEntryStatusType::DELETED) {
			//! Skip this entry, we don't care about deleted entries
			continue;
		}

		auto &data_file = entry.data_file;
		data_file.file_path = file_path_data[index].GetString();
		data_file.file_format = file_format_data[index].GetString();
		data_file.record_count = record_count_data[index];
		data_file.file_size_in_bytes = file_size_in_bytes_data[index];

		data_file.lower_bounds = GetBounds(lower_bounds, index);
		data_file.upper_bounds = GetBounds(upper_bounds, index);
		data_file.column_sizes = GetCounts(column_sizes, index);
		data_file.value_counts = GetCounts(value_counts, index);
		data_file.null_value_counts = GetCounts(null_value_counts, index);
		data_file.nan_value_counts = GetCounts(nan_value_counts, index);

		if (referenced_data_file && FlatVector::Validity(*referenced_data_file).RowIsValid(index)) {
			data_file.referenced_data_file = FlatVector::GetData<string_t>(*referenced_data_file)[index].GetString();
		}
		if (content_offset && FlatVector::Validity(*content_offset).RowIsValid(index)) {
			data_file.content_offset = content_offset->GetValue(index);
		}
		if (content_size_in_bytes && FlatVector::Validity(*content_size_in_bytes).RowIsValid(index)) {
			data_file.content_size_in_bytes = content_size_in_bytes->GetValue(index);
		}

		if (iceberg_version >= 2) {
			data_file.content = (IcebergManifestEntryContentType)content_data[index];
			data_file.equality_ids = GetEqualityIds(equality_ids, index);

			if (FlatVector::Validity(sequence_number).RowIsValid(index)) {
				entry.sequence_number = sequence_number_data[index];
			} else {
				//! Value should only be NULL for ADDED manifest entries, to support inheritance
				D_ASSERT(entry.status == IcebergManifestEntryStatusType::ADDED);
				entry.sequence_number = manifest_file_sequence_number_data[index];
			}
		} else {
			entry.sequence_number = manifest_file_sequence_number_data[index];
			data_file.content = IcebergManifestEntryContentType::DATA;
		}

		entry.partition_spec_id = partition_spec_id_data[index];
		entry.manifest_file_idx = manifest_file_index_data[index];
		for (auto &it : partition_vectors) {
			auto field_id = it.first;
			auto &partition_vector = it.second.get();

			data_file.partition_values.emplace_back(field_id, partition_vector.GetValue(index));
		}
		produced++;
		result.push_back(entry);
	}
	return produced;
}

} // namespace manifest_file

} // namespace duckdb
