#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/optional.hpp"

namespace duckdb {

namespace manifest_list {

ManifestListReader::ManifestListReader(const AvroScan &scan) : BaseManifestReader(scan) {
}

ManifestListReader::~ManifestListReader() {
}

void ManifestListReader::Read() {
	if (finished) {
		return;
	}
	ScanInternal();
}

template <class T>
static T ReadRequiredField(const char *name, const typename VectorIterator<T>::ValueEntry &entry) {
	if (!entry.IsValid()) {
		throw InvalidConfigurationException("required field '%s' is NULL!", name);
	}
	return entry.GetValueUnsafe();
}

template <class T>
static optional<T> ReadOptionalField(const typename VectorIterator<T>::ValueEntry &entry) {
	if (entry.IsValid()) {
		return entry.GetValueUnsafe();
	}
	return std::nullopt;
}

void ManifestListReader::ReadChunk(DataChunk &chunk, idx_t iceberg_version, vector<IcebergManifestListEntry> &result) {
	auto count = chunk.size();

	//! Setup logic

	//! NOTE: the order of these columns is defined by the order that they are produced in BuildManifestListSchema
	//! see `iceberg_avro_multi_file_reader.cpp`
	idx_t vector_index = 0;

	auto &manifest_path = chunk.data[vector_index++];
	auto manifest_path_entries = manifest_path.Values<string_t>();

	auto &manifest_length = chunk.data[vector_index++];
	auto manifest_length_entries = manifest_length.Values<int64_t>();

	auto &partition_spec_id = chunk.data[vector_index++];
	auto partition_spec_id_entries = partition_spec_id.Values<int32_t>();

	auto &added_snapshot_id = chunk.data[vector_index++];
	auto added_snapshot_id_entries = added_snapshot_id.Values<int64_t>();

	auto &added_files_count = chunk.data[vector_index++];
	auto added_files_count_entries = added_files_count.Values<int32_t>();

	auto &existing_files_count = chunk.data[vector_index++];
	auto existing_files_count_entries = existing_files_count.Values<int32_t>();

	auto &deleted_files_count = chunk.data[vector_index++];
	auto deleted_files_count_entries = deleted_files_count.Values<int32_t>();

	auto &added_rows_count = chunk.data[vector_index++];
	auto added_rows_count_entries = added_rows_count.Values<int64_t>();

	auto &existing_rows_count = chunk.data[vector_index++];
	auto existing_rows_count_entries = existing_rows_count.Values<int64_t>();

	auto &deleted_rows_count = chunk.data[vector_index++];
	auto deleted_rows_count_entries = deleted_rows_count.Values<int64_t>();

	//! 'partitions'
	auto &partitions = chunk.data[vector_index++];
	auto partitions_entries = partitions.Values<VectorListType<VectorStructType<bool, bool, string_t, string_t>>>();

	optional_ptr<Vector> content;
	optional<VectorIterator<int32_t>> content_entries;

	optional_ptr<Vector> sequence_number;
	optional<VectorIterator<int64_t>> sequence_number_entries;

	optional_ptr<Vector> min_sequence_number;
	optional<VectorIterator<int64_t>> min_sequence_number_entries;
	if (iceberg_version >= 2) {
		content = chunk.data[vector_index++];
		sequence_number = chunk.data[vector_index++];
		min_sequence_number = chunk.data[vector_index++];
		content_entries.emplace(content->Values<int32_t>());
		sequence_number_entries.emplace(sequence_number->Values<int64_t>());
		min_sequence_number_entries.emplace(min_sequence_number->Values<int64_t>());
	}

	optional_ptr<Vector> first_row_id;
	optional<VectorIterator<int64_t>> first_row_id_entries;
	if (iceberg_version >= 3) {
		first_row_id = chunk.data[vector_index++];
		first_row_id_entries.emplace(first_row_id->Values<int64_t>());
	}

	//! Conversion logic
	for (idx_t i = 0; i < count; i++) {
		auto manifest_path = ReadRequiredField<string_t>("manifest_path", manifest_path_entries[i]).GetString();
		IcebergManifestFile manifest(manifest_path);
		manifest.manifest_length = ReadRequiredField<int64_t>("manifest_length", manifest_length_entries[i]);
		manifest.added_snapshot_id = ReadRequiredField<int64_t>("added_snapshot_id", added_snapshot_id_entries[i]);
		manifest.partition_spec_id = ReadRequiredField<int32_t>("partition_spec_id", partition_spec_id_entries[i]);
		manifest.added_files_count = ReadRequiredField<int32_t>("added_files_count", added_files_count_entries[i]);
		manifest.existing_files_count =
		    ReadRequiredField<int32_t>("existing_files_count", existing_files_count_entries[i]);
		manifest.deleted_files_count =
		    ReadRequiredField<int32_t>("deleted_files_count", deleted_files_count_entries[i]);
		manifest.added_rows_count = ReadRequiredField<int64_t>("added_rows_count", added_rows_count_entries[i]);
		manifest.existing_rows_count =
		    ReadRequiredField<int64_t>("existing_rows_count", existing_rows_count_entries[i]);
		manifest.deleted_rows_count = ReadRequiredField<int64_t>("deleted_rows_count", deleted_rows_count_entries[i]);

		if (iceberg_version >= 2) {
			manifest.content = IcebergManifestContentType(ReadRequiredField<int32_t>("content", (*content_entries)[i]));
			manifest.sequence_number = ReadRequiredField<int64_t>("sequence_number", (*sequence_number_entries)[i]);
			manifest.min_sequence_number =
			    ReadRequiredField<int64_t>("min_sequence_number", (*min_sequence_number_entries)[i]);
		} else {
			//! SPEC: Manifest list field sequence-number must default to 0
			manifest.sequence_number = 0;
			//! SPEC: Manifest list field min-sequence-number must default to 0
			manifest.min_sequence_number = 0;
			//! SPEC: Manifest list field content must default to 0 (data)
			manifest.content = IcebergManifestContentType::DATA;
		}

		if (iceberg_version >= 3) {
			manifest.first_row_id = ReadOptionalField<int64_t>((*first_row_id_entries)[i]);
		}

		auto partition_entry = partitions_entries[i];
		if (partition_entry.IsValid()) {
			manifest.partitions.has_partitions = true;
			auto &summaries = manifest.partitions.field_summary;
			for (const auto summary_entry : partition_entry.GetChildValues()) {
				FieldSummary summary;
				auto contains_null_entry = summary_entry.template GetChildValue<0>();
				if (contains_null_entry.IsValid()) {
					summary.contains_null = contains_null_entry.GetValueUnsafe();
				}
				auto contains_nan_entry = summary_entry.template GetChildValue<1>();
				if (contains_nan_entry.IsValid()) {
					summary.contains_nan = contains_nan_entry.GetValueUnsafe();
				}
				auto lower_bound_entry = summary_entry.template GetChildValue<2>();
				if (lower_bound_entry.IsValid()) {
					auto &str = lower_bound_entry.GetValueUnsafe();
					summary.lower_bound = Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
				} else {
					summary.lower_bound = Value(LogicalType::BLOB);
				}
				auto upper_bound_entry = summary_entry.template GetChildValue<3>();
				if (upper_bound_entry.IsValid()) {
					auto &str = upper_bound_entry.GetValueUnsafe();
					summary.upper_bound = Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
				} else {
					summary.upper_bound = Value(LogicalType::BLOB);
				}
				summaries.push_back(summary);
			}
		}
		result.push_back(std::move(manifest));
	}
}

} // namespace manifest_list

} // namespace duckdb
