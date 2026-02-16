#include "manifest_reader.hpp"
#include "include/metadata/iceberg_manifest_list.hpp"

namespace duckdb {

namespace manifest_list {

ManifestListReader::ManifestListReader(const AvroScan &scan) : BaseManifestReader(scan) {
}

ManifestListReader::~ManifestListReader() {
}

idx_t ManifestListReader::Read(idx_t count, vector<IcebergManifestFile> &result) {
	if (finished) {
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

	//! NOTE: the order of these columns is defined by the order that they are produced in BuildManifestListSchema
	//! see `iceberg_avro_multi_file_reader.cpp`
	idx_t vector_index = 0;
	auto &manifest_path = chunk.data[vector_index++];
	auto &manifest_length = chunk.data[vector_index++];
	auto &partition_spec_id = chunk.data[vector_index++];
	optional_ptr<Vector> content;
	optional_ptr<Vector> sequence_number;
	optional_ptr<Vector> min_sequence_number;
	if (iceberg_version >= 2) {
		content = chunk.data[vector_index++];
		sequence_number = chunk.data[vector_index++];
		min_sequence_number = chunk.data[vector_index++];
	}
	auto &added_snapshot_id = chunk.data[vector_index++];
	auto &added_files_count = chunk.data[vector_index++];
	auto &existing_files_count = chunk.data[vector_index++];
	auto &deleted_files_count = chunk.data[vector_index++];
	auto &added_rows_count = chunk.data[vector_index++];
	auto &existing_rows_count = chunk.data[vector_index++];
	auto &deleted_rows_count = chunk.data[vector_index++];

	//! 'partitions'
	auto &partitions = chunk.data[vector_index++];
	auto &field_summary_vec = ListVector::GetEntry(partitions);
	auto &child_vectors = StructVector::GetEntries(field_summary_vec);

	idx_t partition_index = 0;
	auto &contains_null = *child_vectors[partition_index++];
	auto &contains_nan = *child_vectors[partition_index++];
	auto &lower_bound = *child_vectors[partition_index++];
	auto &upper_bound = *child_vectors[partition_index++];

	optional_ptr<Vector> first_row_id;
	if (iceberg_version >= 3) {
		first_row_id = chunk.data[vector_index++];
	}

	auto manifest_path_data = FlatVector::GetData<string_t>(manifest_path);
	auto manifest_length_data = FlatVector::GetData<int64_t>(manifest_length);
	auto partition_spec_id_data = FlatVector::GetData<int32_t>(partition_spec_id);
	int32_t *content_data = nullptr;
	int64_t *sequence_number_data = nullptr;
	int64_t *min_sequence_number_data = nullptr;
	int64_t *first_row_id_data = nullptr;
	if (iceberg_version >= 2) {
		content_data = FlatVector::GetData<int32_t>(*content);
		sequence_number_data = FlatVector::GetData<int64_t>(*sequence_number);
		min_sequence_number_data = FlatVector::GetData<int64_t>(*min_sequence_number);
	}
	if (iceberg_version >= 3) {
		first_row_id_data = FlatVector::GetData<int64_t>(*first_row_id);
	}
	auto added_snapshot_id_data = FlatVector::GetData<int64_t>(added_snapshot_id);
	auto added_files_count_data = FlatVector::GetData<int32_t>(added_files_count);
	auto existing_files_count_data = FlatVector::GetData<int32_t>(existing_files_count);
	auto deleted_files_count_data = FlatVector::GetData<int32_t>(deleted_files_count);
	auto added_rows_count_data = FlatVector::GetData<int64_t>(added_rows_count);
	auto existing_rows_count_data = FlatVector::GetData<int64_t>(existing_rows_count);
	auto deleted_rows_count_data = FlatVector::GetData<int64_t>(deleted_rows_count);

	auto &partitions_validity = FlatVector::Validity(partitions);
	auto field_summary = FlatVector::GetData<list_entry_t>(partitions);
	auto contains_null_data = FlatVector::GetData<bool>(contains_null);
	auto contains_nan_data = FlatVector::GetData<bool>(contains_nan);

	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestFile manifest(manifest_path_data[index].GetString());
		manifest.manifest_length = manifest_length_data[index];
		manifest.added_snapshot_id = added_snapshot_id_data[index];
		manifest.partition_spec_id = partition_spec_id_data[index];
		//! This flag is only used for writing, not for reading
		manifest.has_min_sequence_number = true;

		if (iceberg_version >= 2) {
			manifest.content = IcebergManifestContentType(content_data[index]);
			manifest.sequence_number = sequence_number_data[index];
			manifest.min_sequence_number = min_sequence_number_data[index];
			manifest.added_files_count = added_files_count_data[index];
			manifest.existing_files_count = existing_files_count_data[index];
			manifest.deleted_files_count = deleted_files_count_data[index];
			manifest.added_rows_count = added_rows_count_data[index];
			manifest.existing_rows_count = existing_rows_count_data[index];
			manifest.deleted_rows_count = deleted_rows_count_data[index];
		} else {
			manifest.content = IcebergManifestContentType::DATA;
			manifest.sequence_number = 0;
			manifest.min_sequence_number = 0;
		}

		if (iceberg_version >= 3) {
			if (!FlatVector::Validity(*first_row_id).RowIsValid(index)) {
				if (manifest.content != IcebergManifestContentType::DELETE) {
					throw InternalException(
					    "Malformed manifest_file detected, 'first-row-id' is not set for a DATA manifest");
				}
				manifest.has_first_row_id = false;
			} else {
				manifest.first_row_id = first_row_id_data[index];
				manifest.has_first_row_id = true;
			}
		}

		if (field_summary && partitions_validity.RowIsValid(index)) {
			manifest.partitions.has_partitions = true;
			auto &summaries = manifest.partitions.field_summary;
			auto list_entry = field_summary[index];
			for (idx_t j = 0; j < list_entry.length; j++) {
				FieldSummary summary;
				auto list_idx = list_entry.offset + j;
				if (FlatVector::Validity(contains_null).RowIsValid(list_idx)) {
					summary.contains_null = contains_null_data[list_idx];
				}
				if (FlatVector::Validity(contains_nan).RowIsValid(list_idx)) {
					summary.contains_nan = contains_nan_data[list_idx];
				}
				summary.lower_bound = lower_bound.GetValue(list_idx);
				summary.upper_bound = upper_bound.GetValue(list_idx);
				summaries.push_back(summary);
			}
		}
		result.push_back(manifest);
	}
	return count;
}

} // namespace manifest_list

} // namespace duckdb
