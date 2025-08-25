#include "deletes/positional_delete.hpp"
#include "iceberg_multi_file_list.hpp"

namespace duckdb {

void IcebergMultiFileList::ScanPositionalDeleteFile(DataChunk &result) const {
	//! FIXME: might want to check the 'columns' of the 'reader' to check, field-ids are:
	auto names = FlatVector::GetData<string_t>(result.data[0]);  //! 2147483546
	auto row_ids = FlatVector::GetData<int64_t>(result.data[1]); //! 2147483545

	auto count = result.size();
	if (count == 0) {
		return;
	}
	reference<string_t> current_file_path = names[0];

	auto initial_key = current_file_path.get().GetString();
	auto it = positional_delete_data.find(initial_key);
	if (it == positional_delete_data.end()) {
		it = positional_delete_data.emplace(initial_key, make_uniq<IcebergPositionalDeleteData>()).first;
	}
	reference<IcebergPositionalDeleteData> deletes = reinterpret_cast<IcebergPositionalDeleteData &>(*it->second);

	for (idx_t i = 0; i < count; i++) {
		auto &name = names[i];
		auto &row_id = row_ids[i];

		if (name != current_file_path.get()) {
			current_file_path = name;
			auto key = current_file_path.get().GetString();
			auto it = positional_delete_data.find(key);
			if (it == positional_delete_data.end()) {
				it = positional_delete_data.emplace(key, make_uniq<IcebergPositionalDeleteData>()).first;
			}
			deletes = reinterpret_cast<IcebergPositionalDeleteData &>(*it->second);
		}

		deletes.get().AddRow(row_id);
	}
}

} // namespace duckdb
