
#pragma once

#include "duckdb.hpp"
#include "catalog_api.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;
namespace duckdb {
class IcebergSchemaEntry;
class IcebergTransaction;

struct YyjsonDocDeleter {
	void operator()(yyjson_doc *doc) {
		yyjson_doc_free(doc);
	}
	void operator()(yyjson_mut_doc *doc) {
		yyjson_mut_doc_free(doc);
	}
};

class ICUtils {
public:
	static yyjson_val *get_error_message(const string &api_result);
	static yyjson_doc *api_result_to_doc(const string &api_result);
	static string JsonToString(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc);
};

} // namespace duckdb
