#include "iceberg_avro_multi_file_reader.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

unique_ptr<MultiFileReader> IcebergAvroMultiFileReader::CreateInstance(const TableFunction &table) {
	(void)table;
	return make_uniq<IcebergAvroMultiFileReader>();
}

shared_ptr<MultiFileList> IcebergAvroMultiFileReader::CreateFileList(ClientContext &context,
                                                                     const vector<string> &paths,
                                                                     const FileGlobInput &glob_input) {
	vector<OpenFileInfo> open_files;
	for (auto &path : paths) {
		open_files.emplace_back(path);
		open_files.back().extended_info = make_uniq<ExtendedOpenFileInfo>();
		open_files.back().extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
		open_files.back().extended_info->options["force_full_download"] = Value::BOOLEAN(true);
	}
	auto res = make_uniq<SimpleMultiFileList>(std::move(open_files));
	return std::move(res);
}

} // namespace duckdb
