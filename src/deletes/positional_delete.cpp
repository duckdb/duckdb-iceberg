#include "deletes/positional_delete.hpp"
#include "iceberg_multi_file_list.hpp"

namespace duckdb {

unique_ptr<DeleteFilter> IcebergPositionalDeleteData::ToFilter() const {
	return make_uniq<IcebergPositionalDeleteFilter>(shared_from_this());
}

void IcebergPositionalDeleteData::ToSet(set<idx_t> &out) const {
	out.insert(invalid_rows.begin(), invalid_rows.end());
}

static optional_ptr<IcebergPositionalDeleteData>
TryGetOrCreate(case_insensitive_map_t<shared_ptr<IcebergDeleteData>> &deletes, const IcebergManifestEntry &entry,
               const string &file_path) {
	auto it = deletes.find(file_path);
	if (it == deletes.end()) {
		it = deletes.emplace(file_path, make_shared_ptr<IcebergPositionalDeleteData>(entry)).first;
	} else if (it->second->type == IcebergDeleteType::POSITIONAL_DELETE) {
		it->second->entries.push_back(entry);
	}
	if (it->second->type != IcebergDeleteType::POSITIONAL_DELETE) {
		return nullptr;
	}
	return reinterpret_cast<IcebergPositionalDeleteData &>(*it->second);
}

void IcebergMultiFileList::ScanPositionalDeleteFile(const IcebergManifestEntry &entry, DataChunk &result) const {
	//! FIXME: might want to check the 'columns' of the 'reader' to check, field-ids are:
	auto names = FlatVector::GetData<string_t>(result.data[0]);  //! 2147483546
	auto row_ids = FlatVector::GetData<int64_t>(result.data[1]); //! 2147483545

	auto count = result.size();
	if (count == 0) {
		return;
	}
	reference<string_t> current_file_path = names[0];
	auto initial_key = current_file_path.get().GetString();
	auto deletes = TryGetOrCreate(positional_delete_data, entry, initial_key);

	for (idx_t i = 0; i < count; i++) {
		auto &name = names[i];
		auto &row_id = row_ids[i];

		if (name != current_file_path.get()) {
			current_file_path = name;
			auto key = current_file_path.get().GetString();
			deletes = TryGetOrCreate(positional_delete_data, entry, key);
		}
		if (!deletes) {
			continue;
		}
		deletes->AddRow(row_id);
	}
}

} // namespace duckdb
