
#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"

#include "catalog/rest/api/catalog_api.hpp"

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
	static yyjson_val *GetErrorMessage(const string &api_result,
	                                   std::unique_ptr<yyjson_doc, YyjsonDocDeleter> &out_doc);
	static std::unique_ptr<yyjson_doc, YyjsonDocDeleter> APIResultToDoc(const string &api_result);
	static string JsonToString(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc);
};

} // namespace duckdb
