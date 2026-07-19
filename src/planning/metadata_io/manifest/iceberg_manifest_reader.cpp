#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/string.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/optional.hpp"

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

using IntStringMapEntries = VectorIterator<VectorListType<VectorStructType<int32_t, string_t>>>;
using IntIntMapEntries = VectorIterator<VectorListType<VectorStructType<int32_t, int64_t>>>;
using Int32ListEntries = VectorIterator<VectorListType<int32_t>>;
using Int64ListEntries = VectorIterator<VectorListType<int64_t>>;
using Int32Entries = VectorIterator<int32_t>;
using Int64Entries = VectorIterator<int64_t>;
using StringEntries = VectorIterator<string_t>;

template <class T, class ENTRY>
static T ReadRequiredField(const char *name, const ENTRY &entry) {
	if (!entry.IsValid()) {
		throw InvalidConfigurationException("required field '%s' is NULL!", name);
	}
	return entry.GetValueUnsafe();
}

template <class T, class ENTRY>
static optional<T> ReadOptionalField(const ENTRY &entry) {
	if (entry.IsValid()) {
		return entry.GetValueUnsafe();
	}
	return std::nullopt;
}

static unordered_map<int32_t, Value> GetBounds(const IntStringMapEntries::ValueEntry &entry) {
	unordered_map<int32_t, Value> parsed_bounds;
	if (!entry.IsValid()) {
		return parsed_bounds;
	}

	for (const auto bounds_entry : entry.GetChildValues()) {
		auto key_entry = bounds_entry.template GetChildValue<0>();
		auto value_entry = bounds_entry.template GetChildValue<1>();
		auto value = Value(LogicalType::BLOB);
		if (value_entry.IsValid()) {
			auto &str = value_entry.GetValueUnsafe();
			value = Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
		}
		parsed_bounds[key_entry.GetValueUnsafe()] = value;
	}
	return parsed_bounds;
}

static unordered_map<int32_t, int64_t> GetCounts(const char *name, const IntIntMapEntries::ValueEntry &entry) {
	unordered_map<int32_t, int64_t> parsed_counts;
	if (!entry.IsValid()) {
		return parsed_counts;
	}

	for (const auto count_entry : entry.GetChildValues()) {
		auto key_entry = count_entry.template GetChildValue<0>();
		auto value_entry = count_entry.template GetChildValue<1>();
		auto key = key_entry.GetValueUnsafe();
		if (!value_entry.IsValid()) {
			throw InvalidConfigurationException("'%s' map's value for key '%d' is NULL", name, key);
		}
		parsed_counts[key] = value_entry.GetValueUnsafe();
	}
	return parsed_counts;
}

template <class T>
static vector<T> GetListTemplated(const char *name,
                                  const typename VectorIterator<VectorListType<T>>::ValueEntry &entry) {
	vector<T> result;
	if (!entry.IsValid()) {
		return result;
	}
	for (const auto list_entry : entry.GetChildValues()) {
		if (!list_entry.IsValid()) {
			throw InvalidConfigurationException("'%s' list contains NULL", name);
		}
		result.push_back(list_entry.GetValueUnsafe());
	}

	return result;
}

static vector<int32_t> GetEqualityIds(const Int32ListEntries::ValueEntry &entry) {
	return GetListTemplated<int32_t>("equality_ids", entry);
}

static vector<int64_t> GetSplitOffsets(const Int64ListEntries::ValueEntry &entry) {
	return GetListTemplated<int64_t>("split_offsets", entry);
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
	auto status_entries = status.Values<int32_t>();

	auto &snapshot_id = chunk.data[vector_index++];
	auto snapshot_id_entries = snapshot_id.Values<int64_t>();

	auto &sequence_number = chunk.data[vector_index++];
	auto sequence_number_entries = sequence_number.Values<int64_t>();

	auto &file_sequence_number = chunk.data[vector_index++];
	auto file_sequence_number_entries = file_sequence_number.Values<int64_t>();

	auto &data_file = chunk.data[vector_index++];
	idx_t entry_index = 0;
	auto &data_file_entries = StructVector::GetEntries(data_file);
	auto &file_path = data_file_entries[entry_index++];
	auto file_path_entries = file_path.Values<string_t>();

	auto &file_format = data_file_entries[entry_index++];
	auto file_format_entries = file_format.Values<string_t>();

	auto &partition = data_file_entries[entry_index++];

	auto &record_count = data_file_entries[entry_index++];
	auto record_count_entries = record_count.Values<int64_t>();

	auto &file_size_in_bytes = data_file_entries[entry_index++];
	auto file_size_in_bytes_entries = file_size_in_bytes.Values<int64_t>();

	auto &column_sizes = data_file_entries[entry_index++];
	auto column_sizes_entries = column_sizes.Values<VectorListType<VectorStructType<int32_t, int64_t>>>();

	auto &value_counts = data_file_entries[entry_index++];
	auto value_counts_entries = value_counts.Values<VectorListType<VectorStructType<int32_t, int64_t>>>();

	auto &null_value_counts = data_file_entries[entry_index++];
	auto null_value_counts_entries = null_value_counts.Values<VectorListType<VectorStructType<int32_t, int64_t>>>();

	auto &nan_value_counts = data_file_entries[entry_index++];
	auto nan_value_counts_entries = nan_value_counts.Values<VectorListType<VectorStructType<int32_t, int64_t>>>();

	auto &lower_bounds = data_file_entries[entry_index++];
	auto lower_bounds_entries = lower_bounds.Values<VectorListType<VectorStructType<int32_t, string_t>>>();

	auto &upper_bounds = data_file_entries[entry_index++];
	auto upper_bounds_entries = upper_bounds.Values<VectorListType<VectorStructType<int32_t, string_t>>>();

	auto &split_offsets = data_file_entries[entry_index++];
	auto split_offsets_entries = split_offsets.Values<VectorListType<int64_t>>();

	auto &equality_ids = data_file_entries[entry_index++];
	auto equality_ids_entries = equality_ids.Values<VectorListType<int32_t>>();

	auto &sort_order_id = data_file_entries[entry_index++];
	auto sort_order_id_entries = sort_order_id.Values<int32_t>();

	optional_ptr<Vector> content;
	optional<Int32Entries> content_entries;
	optional_ptr<Vector> referenced_data_file;
	optional<StringEntries> referenced_data_file_entries;
	if (iceberg_version >= 2) {
		content = data_file_entries[entry_index++];
		content_entries.emplace(content->Values<int32_t>());

		referenced_data_file = data_file_entries[entry_index++];
		referenced_data_file_entries.emplace(referenced_data_file->Values<string_t>());
	}

	optional_ptr<Vector> first_row_id;
	optional<Int64Entries> first_row_id_entries;
	if (iceberg_version >= 3) {
		first_row_id = data_file_entries[entry_index++];
		first_row_id_entries.emplace(first_row_id->Values<int64_t>());
	}
	optional_ptr<Vector> content_offset;
	optional<Int64Entries> content_offset_entries;

	optional_ptr<Vector> content_size_in_bytes;
	optional<Int64Entries> content_size_in_bytes_entries;
	if (iceberg_version >= 3) {
		content_offset = data_file_entries[entry_index++];
		content_size_in_bytes = data_file_entries[entry_index++];
		content_offset_entries.emplace(content_offset->Values<int64_t>());
		content_size_in_bytes_entries.emplace(content_size_in_bytes->Values<int64_t>());
	}

	vector<std::pair<int32_t, reference<Vector>>> partition_vectors;
	if (partition.GetType().id() != LogicalTypeId::SQLNULL) {
		auto &partition_children = StructVector::GetEntries(partition);
		D_ASSERT(partition_children.size() == partition_field_id_to_type.size());
		idx_t child_index = 0;
		for (auto &it : partition_field_id_to_type) {
			partition_vectors.emplace_back(it.first, partition_children[child_index++]);
		}
	}

	//! Conversion logic
	for (idx_t i = 0; i < count; i++) {
		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)ReadRequiredField<int32_t>("status", status_entries[i]);

		auto &data_file = entry.data_file;
		data_file.file_path = ReadRequiredField<string_t>("file_path", file_path_entries[i]).GetString();
		data_file.file_format = ReadRequiredField<string_t>("file_format", file_format_entries[i]).GetString();
		data_file.record_count = ReadRequiredField<int64_t>("record_count", record_count_entries[i]);
		data_file.file_size_in_bytes = ReadRequiredField<int64_t>("file_size_in_bytes", file_size_in_bytes_entries[i]);

		data_file.lower_bounds = GetBounds(lower_bounds_entries[i]);
		data_file.upper_bounds = GetBounds(upper_bounds_entries[i]);
		data_file.column_sizes = GetCounts("column_sizes", column_sizes_entries[i]);
		data_file.value_counts = GetCounts("value_counts", value_counts_entries[i]);
		data_file.null_value_counts = GetCounts("null_value_counts", null_value_counts_entries[i]);
		data_file.nan_value_counts = GetCounts("nan_value_counts", nan_value_counts_entries[i]);

		data_file.split_offsets = GetSplitOffsets(split_offsets_entries[i]);
		data_file.sort_order_id = ReadOptionalField<int32_t>(sort_order_id_entries[i]);

		entry.SetSnapshotId(ReadOptionalField<int64_t>(snapshot_id_entries[i]));

		//! >= V2
		if (iceberg_version >= 2) {
			data_file.content =
			    (IcebergManifestEntryContentType)ReadRequiredField<int32_t>("content", (*content_entries)[i]);
			data_file.equality_ids = GetEqualityIds(equality_ids_entries[i]);

			entry.SetSequenceNumber(ReadOptionalField<int64_t>(sequence_number_entries[i]));
			entry.SetFileSequenceNumber(ReadOptionalField<int64_t>(file_sequence_number_entries[i]));

			if (auto val = ReadOptionalField<string_t>((*referenced_data_file_entries)[i])) {
				data_file.referenced_data_file = val->GetString();
			}
		} else {
			//! SPEC: Data file field content must default to 0 (data)
			data_file.content = IcebergManifestEntryContentType::DATA;
			//! SPEC: Manifest entry field sequence_number must default to 0
			entry.SetSequenceNumber(0);
			//! SPEC: Manifest entry field file_sequence_number must default to 0
			entry.SetFileSequenceNumber(0);
		}

		//! >= V3
		if (iceberg_version >= 3) {
			data_file.SetFirstRowId(ReadOptionalField<int64_t>((*first_row_id_entries)[i]));
			data_file.content_offset = ReadOptionalField<int64_t>((*content_offset_entries)[i]);
			data_file.content_size_in_bytes = ReadOptionalField<int64_t>((*content_size_in_bytes_entries)[i]);
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
