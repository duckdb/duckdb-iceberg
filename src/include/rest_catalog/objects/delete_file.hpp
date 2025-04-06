
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
	DeleteFile::DeleteFile() {
	}

public:
	static DeleteFile FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		do {
			error = base_position_delete_file.TryFromJSON(obj);
			if (error.empty()) {
				has_position_delete_file = true;
				break;
			}
			error = base_equality_delete_file.TryFromJSON(obj);
			if (error.empty()) {
				has_equality_delete_file = true;
				break;
			}
			return "DeleteFile failed to parse, none of the oneOf candidates matched";
		} while (false);

		return string();
	}

public:
	EqualityDeleteFile equality_delete_file;
	PositionDeleteFile position_delete_file;

public:
};

} // namespace rest_api_objects
} // namespace duckdb
