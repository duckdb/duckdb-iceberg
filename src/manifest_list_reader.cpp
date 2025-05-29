#include "manifest_reader.hpp"

namespace duckdb {

ManifestListReader::ManifestListReader(idx_t iceberg_version) : BaseManifestReader(iceberg_version) {
}

idx_t ManifestListReader::Read(idx_t count, vector<IcebergManifest> &result) {
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

idx_t ManifestListReader::ReadChunk(idx_t offset, idx_t count, vector<IcebergManifest> &result) {
	D_ASSERT(offset < chunk.size());
	D_ASSERT(offset + count <= chunk.size());

	auto manifest_path = FlatVector::GetData<string_t>(chunk.data[name_to_vec.at("manifest_path").GetPrimaryIndex()]);
	auto partition_spec_id =
	    FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("partition_spec_id").GetPrimaryIndex()]);

	int32_t *content = nullptr;
	int64_t *sequence_number = nullptr;
	int64_t *added_rows_count = nullptr;
	int64_t *existing_rows_count = nullptr;

	if (iceberg_version > 1) {
		//! 'content'
		content = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("content").GetPrimaryIndex()]);
		//! 'sequence_number'
		sequence_number = FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("sequence_number").GetPrimaryIndex()]);
		//! 'added_rows_count'
		added_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("added_rows_count").GetPrimaryIndex()]);
		//! 'existing_rows_count'
		existing_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("existing_rows_count").GetPrimaryIndex()]);
	}

	//! 'partitions'
	list_entry_t *field_summary;
	optional_ptr<Vector> contains_null = nullptr;
	optional_ptr<Vector> contains_nan = nullptr;
	optional_ptr<Vector> lower_bound = nullptr;
	optional_ptr<Vector> upper_bound = nullptr;

	bool *contains_null_data = nullptr;
	bool *contains_nan_data = nullptr;

	auto partitions_it = name_to_vec.find("partitions");
	if (partitions_it != name_to_vec.end()) {
		auto &partitions = chunk.data[name_to_vec.at("partitions").GetPrimaryIndex()];

		auto &field_summary_vec = ListVector::GetEntry(partitions);
		field_summary = FlatVector::GetData<list_entry_t>(partitions);
		auto &child_vectors = StructVector::GetEntries(field_summary_vec);
		auto &child_types = StructType::GetChildTypes(ListType::GetChildType(partitions.GetType()));
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &kv = child_types[i];
			auto &name = kv.first;

			if (StringUtil::CIEquals(name, "contains_null")) {
				contains_null = child_vectors[i].get();
				contains_null_data = FlatVector::GetData<bool>(*child_vectors[i]);
			} else if (StringUtil::CIEquals(name, "contains_nan")) {
				contains_nan = child_vectors[i].get();
				contains_nan_data = FlatVector::GetData<bool>(*child_vectors[i]);
			} else if (StringUtil::CIEquals(name, "lower_bound")) {
				lower_bound = child_vectors[i].get();
			} else if (StringUtil::CIEquals(name, "upper_bound")) {
				upper_bound = child_vectors[i].get();
			}
		}
	}

	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifest manifest;
		manifest.manifest_path = manifest_path[index].GetString();
		manifest.partition_spec_id = partition_spec_id[index];
		manifest.sequence_number = 0;

		if (iceberg_version > 1) {
			manifest.content = IcebergManifestContentType(content[index]);
			manifest.sequence_number = sequence_number[index];
			manifest.added_rows_count = added_rows_count[index];
			manifest.existing_rows_count = existing_rows_count[index];
		} else {
			manifest.content = IcebergManifestContentType::DATA;
			manifest.sequence_number = 0;
			manifest.added_rows_count = 0;
			manifest.existing_rows_count = 0;
		}

		if (field_summary) {
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

bool ManifestListReader::ValidateNameMapping() {
	if (!name_to_vec.count("manifest_path")) {
		return false;
	}
	if (!name_to_vec.count("partition_spec_id")) {
		return false;
	}
	if (iceberg_version > 1) {
		if (!name_to_vec.count("content")) {
			return false;
		}
	}
	return true;
}

void ManifestListReader::CreateNameMapping(idx_t column_id, const LogicalType &type, const string &name) {
	name_to_vec[name] = ColumnIndex(column_id);
}

} // namespace duckdb
