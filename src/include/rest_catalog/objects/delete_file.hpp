
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/equality_delete_file.hpp"
#include "rest_catalog/objects/position_delete_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class DeleteFile {
public:
	DeleteFile();
	DeleteFile(const DeleteFile &) = delete;
	DeleteFile &operator=(const DeleteFile &) = delete;
	DeleteFile(DeleteFile &&) = default;
	DeleteFile &operator=(DeleteFile &&) = default;

public:
	static DeleteFile FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	PositionDeleteFile position_delete_file;
	bool has_position_delete_file = false;
	EqualityDeleteFile equality_delete_file;
	bool has_equality_delete_file = false;
};

} // namespace rest_api_objects
} // namespace duckdb
