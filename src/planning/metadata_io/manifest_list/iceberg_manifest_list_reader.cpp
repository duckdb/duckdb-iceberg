#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"
#include "planning/metadata_io/manifest_list/iceberg_manifest_list_reader.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

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

void ManifestListReader::ReadChunk(DataChunk &chunk, idx_t iceberg_version, vector<IcebergManifestListEntry> &result) {
	auto count = chunk.size();

	//! Setup logic

	//! NOTE: the order of these columns is defined by the order that they are produced in BuildManifestListSchema
	//! see `iceberg_avro_multi_file_reader.cpp`
	idx_t vector_index = 0;

	auto &manifest_path = chunk.data[vector_index++];
	UnifiedVectorFormat manifest_path_format;
	manifest_path.ToUnifiedFormat(count, manifest_path_format);

	auto &manifest_length = chunk.data[vector_index++];
	UnifiedVectorFormat manifest_length_format;
	manifest_length.ToUnifiedFormat(count, manifest_length_format);

	auto &partition_spec_id = chunk.data[vector_index++];
	UnifiedVectorFormat partition_spec_id_format;
	partition_spec_id.ToUnifiedFormat(count, partition_spec_id_format);

	UnifiedVectorFormat content_format;
	optional_ptr<Vector> content;

	UnifiedVectorFormat sequence_number_format;
	optional_ptr<Vector> sequence_number;

	UnifiedVectorFormat min_sequence_number_format;
	optional_ptr<Vector> min_sequence_number;
	if (iceberg_version >= 2) {
		content = chunk.data[vector_index++];
		sequence_number = chunk.data[vector_index++];
		min_sequence_number = chunk.data[vector_index++];

		content->ToUnifiedFormat(count, content_format);
		sequence_number->ToUnifiedFormat(count, sequence_number_format);
		min_sequence_number->ToUnifiedFormat(count, min_sequence_number_format);
	}

	auto &added_snapshot_id = chunk.data[vector_index++];
	UnifiedVectorFormat added_snapshot_id_format;
	added_snapshot_id.ToUnifiedFormat(count, added_snapshot_id_format);

	auto &added_files_count = chunk.data[vector_index++];
	UnifiedVectorFormat added_files_count_format;
	added_files_count.ToUnifiedFormat(count, added_files_count_format);

	auto &existing_files_count = chunk.data[vector_index++];
	UnifiedVectorFormat existing_files_count_format;
	existing_files_count.ToUnifiedFormat(count, existing_files_count_format);

	auto &deleted_files_count = chunk.data[vector_index++];
	UnifiedVectorFormat deleted_files_count_format;
	deleted_files_count.ToUnifiedFormat(count, deleted_files_count_format);

	auto &added_rows_count = chunk.data[vector_index++];
	UnifiedVectorFormat added_rows_count_format;
	added_rows_count.ToUnifiedFormat(count, added_rows_count_format);

	auto &existing_rows_count = chunk.data[vector_index++];
	UnifiedVectorFormat existing_rows_count_format;
	existing_rows_count.ToUnifiedFormat(count, existing_rows_count_format);

	auto &deleted_rows_count = chunk.data[vector_index++];
	UnifiedVectorFormat deleted_rows_count_format;
	deleted_rows_count.ToUnifiedFormat(count, deleted_rows_count_format);

	//! 'partitions'
	auto &partitions = chunk.data[vector_index++];
	RecursiveUnifiedVectorFormat partitions_format;
	Vector::RecursiveToUnifiedFormat(partitions, count, partitions_format);

	auto &field_summary_format = partitions_format.children[0];
	auto list_data = partitions_format.unified.GetData<list_entry_t>(partitions_format.unified);

	idx_t partition_index = 0;
	auto &contains_null_format = field_summary_format.children[partition_index++].unified;
	auto contains_null_data = contains_null_format.GetData<bool>(contains_null_format);

	auto &contains_nan_format = field_summary_format.children[partition_index++].unified;
	auto contains_nan_data = contains_nan_format.GetData<bool>(contains_nan_format);

	auto &lower_bound_format = field_summary_format.children[partition_index++].unified;
	auto lower_bound_data = lower_bound_format.GetData<string_t>(lower_bound_format);

	auto &upper_bound_format = field_summary_format.children[partition_index++].unified;
	auto upper_bound_data = upper_bound_format.GetData<string_t>(upper_bound_format);

	UnifiedVectorFormat first_row_id_format;
	optional_ptr<Vector> first_row_id;
	if (iceberg_version >= 3) {
		first_row_id = chunk.data[vector_index++];
		first_row_id->ToUnifiedFormat(count, first_row_id_format);
	}

	//! Conversion logic
	for (idx_t i = 0; i < count; i++) {
		auto manifest_path = ReadRequiredField<string_t>("manifest_path", manifest_path_format, i).GetString();
		IcebergManifestFile manifest(manifest_path);
		manifest.manifest_length = ReadRequiredField<int64_t>("manifest_length", manifest_length_format, i);
		manifest.added_snapshot_id = ReadRequiredField<int64_t>("added_snapshot_id", added_snapshot_id_format, i);
		manifest.partition_spec_id = ReadRequiredField<int32_t>("partition_spec_id", partition_spec_id_format, i);
		//! This flag is only used for writing, not for reading
		manifest.has_min_sequence_number = true;

		if (iceberg_version >= 2) {
			manifest.content = IcebergManifestContentType(ReadRequiredField<int32_t>("content", content_format, i));
			manifest.sequence_number = ReadRequiredField<int64_t>("sequence_number", sequence_number_format, i);
			manifest.min_sequence_number =
			    ReadRequiredField<int64_t>("min_sequence_number", min_sequence_number_format, i);
			manifest.added_files_count = ReadRequiredField<int32_t>("added_files_count", added_files_count_format, i);
			manifest.existing_files_count =
			    ReadRequiredField<int32_t>("existing_files_count", existing_files_count_format, i);
			manifest.deleted_files_count =
			    ReadRequiredField<int32_t>("deleted_files_count", deleted_files_count_format, i);

			manifest.added_rows_count = ReadRequiredField<int64_t>("added_rows_count", added_rows_count_format, i);
			manifest.existing_rows_count =
			    ReadRequiredField<int64_t>("existing_rows_count", existing_rows_count_format, i);
			manifest.deleted_rows_count =
			    ReadRequiredField<int64_t>("deleted_rows_count", deleted_rows_count_format, i);
		} else {
			//! SPEC: Manifest list field sequence-number must default to 0
			manifest.sequence_number = 0;
			//! SPEC: Manifest list field min-sequence-number must default to 0
			manifest.min_sequence_number = 0;
			//! SPEC: Manifest list field content must default to 0 (data)
			manifest.content = IcebergManifestContentType::DATA;
		}

		if (iceberg_version >= 3) {
			int64_t first_row_id;
			if (ReadOptionalField<int64_t>(first_row_id_format, i, first_row_id)) {
				manifest.first_row_id = first_row_id;
				manifest.has_first_row_id = true;
			}
		}

		idx_t partitions_index = partitions_format.unified.sel->get_index(i);
		if (partitions_format.unified.validity.RowIsValid(partitions_index)) {
			manifest.partitions.has_partitions = true;
			auto &summaries = manifest.partitions.field_summary;
			auto list_entry = list_data[partitions_index];
			for (idx_t j = 0; j < list_entry.length; j++) {
				FieldSummary summary;
				auto contains_null_index = contains_null_format.sel->get_index(list_entry.offset + j);
				if (contains_null_format.validity.RowIsValid(contains_null_index)) {
					summary.contains_null = contains_null_data[contains_null_index];
				}
				auto contains_nan_index = contains_nan_format.sel->get_index(list_entry.offset + j);
				if (contains_nan_format.validity.RowIsValid(contains_nan_index)) {
					summary.contains_nan = contains_nan_data[contains_nan_index];
				}
				auto lower_bound_index = lower_bound_format.sel->get_index(list_entry.offset + j);
				if (lower_bound_format.validity.RowIsValid(lower_bound_index)) {
					auto &str = lower_bound_data[lower_bound_index];
					summary.lower_bound = Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
				} else {
					summary.lower_bound = Value(LogicalType::BLOB);
				}
				auto upper_bound_index = upper_bound_format.sel->get_index(list_entry.offset + j);
				if (upper_bound_format.validity.RowIsValid(upper_bound_index)) {
					auto &str = upper_bound_data[upper_bound_index];
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
