#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/string.hpp"

namespace duckdb {

namespace manifest_file {

ManifestReader::ManifestReader(const AvroScan &scan) : BaseManifestReader(scan) {
}

ManifestReader::~ManifestReader() {
}

void ManifestReader::Read() {
	if (finished) {
		return;
	}
	ScanInternal();
}

static unordered_map<int32_t, Value> GetBounds(RecursiveUnifiedVectorFormat &format, idx_t i) {
	unordered_map<int32_t, Value> parsed_bounds;

	auto &validity = format.unified.validity;
	auto index = format.unified.sel->get_index(i);
	if (!validity.RowIsValid(index)) {
		return parsed_bounds;
	}
	auto bounds_data = format.unified.GetData<list_entry_t>(format.unified);
	auto list_entry = bounds_data[index];

	auto &inner_struct_format = format.children[0];
	auto &keys_format = inner_struct_format.children[0].unified;
	auto &values_format = inner_struct_format.children[1].unified;
	auto keys_data = keys_format.GetData<int32_t>(keys_format);
	auto values_data = values_format.GetData<string_t>(values_format);
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto keys_idx = keys_format.sel->get_index(list_entry.offset + j);
		auto values_idx = values_format.sel->get_index(list_entry.offset + j);

		auto value = Value(LogicalType::BLOB);
		if (values_format.validity.RowIsValid(values_idx)) {
			auto &str = values_data[values_idx];
			value = Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
		}
		parsed_bounds[keys_data[keys_idx]] = value;
	}
	return parsed_bounds;
}

static unordered_map<int32_t, int64_t> GetCounts(const char *name, RecursiveUnifiedVectorFormat &format, idx_t i) {
	unordered_map<int32_t, int64_t> parsed_counts;

	auto &validity = format.unified.validity;
	auto index = format.unified.sel->get_index(i);
	if (!validity.RowIsValid(index)) {
		return parsed_counts;
	}
	auto bounds_data = format.unified.GetDataUnsafe<list_entry_t>(format.unified);
	auto list_entry = bounds_data[index];

	auto &inner_struct_format = format.children[0];
	auto &keys_format = inner_struct_format.children[0].unified;
	auto &values_format = inner_struct_format.children[1].unified;
	auto keys_data = keys_format.GetData<int32_t>(keys_format);
	auto values_data = values_format.GetData<int64_t>(values_format);
	for (idx_t j = 0; j < list_entry.length; j++) {
		auto keys_idx = keys_format.sel->get_index(list_entry.offset + j);
		auto values_idx = values_format.sel->get_index(list_entry.offset + j);

		auto key = keys_data[keys_idx];
		if (!values_format.validity.RowIsValid(values_idx)) {
			throw InvalidConfigurationException("'%s' map's value for key '%d' is NULL", name, key);
		}
		parsed_counts[key] = values_data[values_idx];
	}
	return parsed_counts;
}

template <class T>
static vector<T> GetListTemplated(const char *name, RecursiveUnifiedVectorFormat &item_format, idx_t i) {
	vector<T> result;
	auto &format = item_format.unified;

	auto &validity = format.validity;
	auto index = format.sel->get_index(i);
	if (!validity.RowIsValid(index)) {
		return result;
	}

	auto &child_format = item_format.children[0].unified;
	auto child_data = child_format.GetDataUnsafe<T>(child_format);

	auto list_data = format.GetDataUnsafe<list_entry_t>(format);
	auto list_entry = list_data[index];

	for (idx_t j = 0; j < list_entry.length; j++) {
		auto list_idx = child_format.sel->get_index(list_entry.offset + j);
		if (!child_format.validity.RowIsValid(list_idx)) {
			throw InvalidConfigurationException("'%s' list contains NULL", name);
		}
		result.push_back(child_data[list_idx]);
	}

	return result;
}

static vector<int32_t> GetEqualityIds(RecursiveUnifiedVectorFormat &equality_ids, idx_t i) {
	return GetListTemplated<int32_t>("equality_ids", equality_ids, i);
}

static vector<int64_t> GetSplitOffsets(RecursiveUnifiedVectorFormat &split_offsets, idx_t i) {
	return GetListTemplated<int64_t>("split_offsets", split_offsets, i);
}

template <class T>
static T ReadRequiredField(const char *name, UnifiedVectorFormat &format, idx_t i) {
	auto data = format.GetDataUnsafe<T>(format);
	auto index = format.sel->get_index(i);
	if (!format.validity.RowIsValid(index)) {
		throw InvalidConfigurationException("required field '%s' is NULL!", name);
	}
	return data[index];
}

template <class T>
static bool ReadOptionalField(UnifiedVectorFormat &format, idx_t i, T &result) {
	auto data = format.GetDataUnsafe<T>(format);
	auto index = format.sel->get_index(i);
	if (format.validity.RowIsValid(index)) {
		result = data[index];
		return true;
	}
	return false;
}

void ManifestReader::ReadChunk(DataChunk &chunk, const map<idx_t, LogicalType> &partition_field_id_to_type,
                               const IcebergTableMetadata &metadata, vector<IcebergManifestEntry> &result) {
	idx_t count = chunk.size();
	auto &partition_specs = metadata.partition_specs;
	auto &iceberg_version = metadata.iceberg_version;

	//! Setup logic

	//! NOTE: the order of these columns is defined by the order that they are produced in BuildManifestSchema
	//! see `iceberg_avro_multi_file_reader.cpp`
	idx_t vector_index = 0;

	auto &status = chunk.data[vector_index++];
	UnifiedVectorFormat status_format;
	status.ToUnifiedFormat(count, status_format);

	auto &snapshot_id = chunk.data[vector_index++];
	UnifiedVectorFormat snapshot_id_format;
	snapshot_id.ToUnifiedFormat(count, snapshot_id_format);

	auto &sequence_number = chunk.data[vector_index++];
	UnifiedVectorFormat sequence_number_format;
	sequence_number.ToUnifiedFormat(count, sequence_number_format);

	auto &file_sequence_number = chunk.data[vector_index++];
	UnifiedVectorFormat file_sequence_number_format;
	file_sequence_number.ToUnifiedFormat(count, file_sequence_number_format);

	auto &data_file = chunk.data[vector_index++];
	idx_t entry_index = 0;
	auto &data_file_entries = StructVector::GetEntries(data_file);

	UnifiedVectorFormat content_format;
	optional_ptr<Vector> content;
	if (iceberg_version >= 2) {
		content = *data_file_entries[entry_index++];
		content->ToUnifiedFormat(count, content_format);
	}

	auto &file_path = *data_file_entries[entry_index++];
	UnifiedVectorFormat file_path_format;
	file_path.ToUnifiedFormat(count, file_path_format);

	auto &file_format = *data_file_entries[entry_index++];
	UnifiedVectorFormat file_format_format;
	file_format.ToUnifiedFormat(count, file_format_format);

	auto &partition = *data_file_entries[entry_index++];

	auto &record_count = *data_file_entries[entry_index++];
	UnifiedVectorFormat record_count_format;
	record_count.ToUnifiedFormat(count, record_count_format);

	auto &file_size_in_bytes = *data_file_entries[entry_index++];
	UnifiedVectorFormat file_size_in_bytes_format;
	file_size_in_bytes.ToUnifiedFormat(count, file_size_in_bytes_format);

	auto &column_sizes = *data_file_entries[entry_index++];
	RecursiveUnifiedVectorFormat column_sizes_format;
	Vector::RecursiveToUnifiedFormat(column_sizes, count, column_sizes_format);

	auto &value_counts = *data_file_entries[entry_index++];
	RecursiveUnifiedVectorFormat value_counts_format;
	Vector::RecursiveToUnifiedFormat(value_counts, count, value_counts_format);

	auto &null_value_counts = *data_file_entries[entry_index++];
	RecursiveUnifiedVectorFormat null_value_counts_format;
	Vector::RecursiveToUnifiedFormat(null_value_counts, count, null_value_counts_format);

	auto &nan_value_counts = *data_file_entries[entry_index++];
	RecursiveUnifiedVectorFormat nan_value_counts_format;
	Vector::RecursiveToUnifiedFormat(nan_value_counts, count, nan_value_counts_format);

	auto &lower_bounds = *data_file_entries[entry_index++];
	RecursiveUnifiedVectorFormat lower_bounds_format;
	Vector::RecursiveToUnifiedFormat(lower_bounds, count, lower_bounds_format);

	auto &upper_bounds = *data_file_entries[entry_index++];
	RecursiveUnifiedVectorFormat upper_bounds_format;
	Vector::RecursiveToUnifiedFormat(upper_bounds, count, upper_bounds_format);

	auto &split_offsets = *data_file_entries[entry_index++];
	RecursiveUnifiedVectorFormat split_offsets_format;
	Vector::RecursiveToUnifiedFormat(split_offsets, count, split_offsets_format);

	auto &equality_ids = *data_file_entries[entry_index++];
	RecursiveUnifiedVectorFormat equality_ids_format;
	Vector::RecursiveToUnifiedFormat(equality_ids, count, equality_ids_format);

	auto &sort_order_id = *data_file_entries[entry_index++];
	UnifiedVectorFormat sort_order_id_format;
	sort_order_id.ToUnifiedFormat(count, sort_order_id_format);

	UnifiedVectorFormat first_row_id_format;
	optional_ptr<Vector> first_row_id;
	if (iceberg_version >= 3) {
		first_row_id = *data_file_entries[entry_index++];
		first_row_id->ToUnifiedFormat(count, first_row_id_format);
	}

	UnifiedVectorFormat referenced_data_file_format;
	optional_ptr<Vector> referenced_data_file;
	if (iceberg_version >= 2) {
		referenced_data_file = *data_file_entries[entry_index++];
		referenced_data_file->ToUnifiedFormat(count, referenced_data_file_format);
	}
	UnifiedVectorFormat content_offset_format;
	optional_ptr<Vector> content_offset;

	UnifiedVectorFormat content_size_in_bytes_format;
	optional_ptr<Vector> content_size_in_bytes;
	if (iceberg_version >= 3) {
		content_offset = *data_file_entries[entry_index++];
		content_size_in_bytes = *data_file_entries[entry_index++];

		content_offset->ToUnifiedFormat(count, content_offset_format);
		content_size_in_bytes->ToUnifiedFormat(count, content_size_in_bytes_format);
	}

	vector<std::pair<int32_t, reference<Vector>>> partition_vectors;
	if (partition.GetType().id() != LogicalTypeId::SQLNULL) {
		auto &partition_children = StructVector::GetEntries(partition);
		D_ASSERT(partition_children.size() == partition_field_id_to_type.size());
		idx_t child_index = 0;
		for (auto &it : partition_field_id_to_type) {
			partition_vectors.emplace_back(it.first, *partition_children[child_index++]);
		}
	}

	//! Conversion logic
	for (idx_t i = 0; i < count; i++) {
		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)ReadRequiredField<int32_t>("status", status_format, i);

		auto &data_file = entry.data_file;
		data_file.file_path = ReadRequiredField<string_t>("file_path", file_path_format, i).GetString();
		data_file.file_format = ReadRequiredField<string_t>("file_format", file_format_format, i).GetString();
		data_file.record_count = ReadRequiredField<int64_t>("record_count", record_count_format, i);
		data_file.file_size_in_bytes = ReadRequiredField<int64_t>("file_size_in_bytes", file_size_in_bytes_format, i);

		data_file.lower_bounds = GetBounds(lower_bounds_format, i);
		data_file.upper_bounds = GetBounds(upper_bounds_format, i);
		data_file.column_sizes = GetCounts("column_sizes", column_sizes_format, i);
		data_file.value_counts = GetCounts("value_counts", value_counts_format, i);
		data_file.null_value_counts = GetCounts("null_value_counts", null_value_counts_format, i);
		data_file.nan_value_counts = GetCounts("nan_value_counts", nan_value_counts_format, i);

		data_file.split_offsets = GetSplitOffsets(split_offsets_format, i);
		int32_t sort_order_id;
		if (ReadOptionalField<int32_t>(sort_order_id_format, i, sort_order_id)) {
			data_file.has_sort_order_id = true;
			data_file.sort_order_id = sort_order_id;
		}

		int64_t snapshot_id;
		if (ReadOptionalField<int64_t>(snapshot_id_format, i, snapshot_id)) {
			entry.SetSnapshotId(snapshot_id);
		}

		//! >= V2
		if (iceberg_version >= 2) {
			data_file.content =
			    (IcebergManifestEntryContentType)ReadRequiredField<int32_t>("content", content_format, i);
			data_file.equality_ids = GetEqualityIds(equality_ids_format, i);

			int64_t sequence_number;
			if (ReadOptionalField<int64_t>(sequence_number_format, i, sequence_number)) {
				entry.SetSequenceNumber(sequence_number);
			}
			int64_t file_sequence_number;
			if (ReadOptionalField<int64_t>(file_sequence_number_format, i, file_sequence_number)) {
				entry.SetFileSequenceNumber(file_sequence_number);
			}

			string_t referenced_data_file;
			if (ReadOptionalField<string_t>(referenced_data_file_format, i, referenced_data_file)) {
				data_file.referenced_data_file = referenced_data_file.GetString();
			}
		} else {
			//! SPEC: Data file field content must default to 0 (data)
			data_file.content = IcebergManifestEntryContentType::DATA;
		}

		//! >= V3
		if (iceberg_version >= 3) {
			int64_t first_row_id;
			if (ReadOptionalField<int64_t>(first_row_id_format, i, first_row_id)) {
				data_file.SetFirstRowId(first_row_id);
			}

			int64_t content_offset;
			if (ReadOptionalField<int64_t>(content_offset_format, i, content_offset)) {
				data_file.content_offset = Value::BIGINT(content_offset);
			} else {
				data_file.content_offset = Value(LogicalType::BIGINT);
			}

			int64_t content_size_in_bytes;
			if (ReadOptionalField<int64_t>(content_size_in_bytes_format, i, content_size_in_bytes)) {
				data_file.content_size_in_bytes = Value::BIGINT(content_size_in_bytes);
			} else {
				data_file.content_size_in_bytes = Value(LogicalType::BIGINT);
			}
		}

		for (auto &it : partition_vectors) {
			auto field_id = it.first;
			auto &partition_vector = it.second.get();
			for (auto &id_spec : partition_specs) {
				auto &spec = id_spec.second;
				for (auto &field : spec.fields) {
					if (field_id == field.partition_field_id) {
						IcebergPartitionInfo info;
						info.field_id = static_cast<uint64_t>(field_id);
						info.value = partition_vector.GetValue(i);
						data_file.partition_info.push_back(std::move(info));
					}
				}
			}
		}
		result.push_back(entry);
	}
}

} // namespace manifest_file

} // namespace duckdb
