#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;

struct IcebergWrittenFile {
	string file_path;
	int64_t record_count = 0;
	int64_t file_size_in_bytes = 0;
};

//! With iceberg_store_written_files, the data files an Iceberg write staged are added to the temporary table
//! iceberg_written_files, in the write's transaction
struct IcebergWrittenFiles {
	static constexpr const char *SETTING = "iceberg_store_written_files";
	static constexpr const char *TABLE = "iceberg_written_files";

	static bool IsEnabled(ClientContext &context);
	static void Append(ClientContext &context, const string &table, const vector<IcebergWrittenFile> &files);
};

} // namespace duckdb
