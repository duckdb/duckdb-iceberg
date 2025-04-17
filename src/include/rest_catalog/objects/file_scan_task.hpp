
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/data_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression;

class FileScanTask {
public:
	FileScanTask();
	FileScanTask(const FileScanTask &) = delete;
	FileScanTask &operator=(const FileScanTask &) = delete;
	FileScanTask(FileScanTask &&) = default;
	FileScanTask &operator=(FileScanTask &&) = default;

public:
	static FileScanTask FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	DataFile data_file;
	vector<int64_t> delete_file_references;
	bool has_delete_file_references = false;
	unique_ptr<Expression> residual_filter;
	bool has_residual_filter = false;
};

} // namespace rest_api_objects
} // namespace duckdb
