#include "catalog_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "storage/irc_schema_entry.hpp"

namespace duckdb {

yyjson_val *ICUtils::get_error_message(const string &api_result) {
	auto *doc = yyjson_read(api_result.c_str(), api_result.size(), 0);
	auto *root = yyjson_doc_get_root(doc);
	auto *error = yyjson_obj_get(root, "error");
	if (error == nullptr) {
		return nullptr;
	}
	auto message = yyjson_obj_get(error, "message");
	auto type = yyjson_obj_get(error, "type");
	auto code = yyjson_obj_get(error, "code");
	if (message != nullptr && type != nullptr && code != nullptr) {
		return root;
	}
	return nullptr;
}

yyjson_doc *ICUtils::api_result_to_doc(const string &api_result) {
	auto *doc = yyjson_read(api_result.c_str(), api_result.size(), 0);
	auto *root = yyjson_doc_get_root(doc);
	auto *error = yyjson_obj_get(root, "error");
	if (error != NULL) {
		try {
			auto message = yyjson_obj_get(error, "message");
			auto message_str = message ? yyjson_get_str(message) : nullptr;
			throw InvalidInputException(!message ? "No message available" : string(message_str));
		} catch (InvalidConfigurationException &e) {
			// keep going, we will throw the whole api result as an error message
			throw InvalidConfigurationException(api_result);
		}
		throw InvalidConfigurationException("Could not parse api_result");
	}
	return doc;
}

string ICUtils::JsonToString(std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc) {
	auto root_object = yyjson_mut_doc_get_root(doc.get());

	//! Write the result to a string
	auto data = yyjson_mut_val_write_opts(root_object, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, nullptr, nullptr);
	if (!data) {
		throw InvalidInputException("Could not serialize the JSON to string, yyjson failed");
	}
	auto res = string(data);
	free(data);
	return res;
}

} // namespace duckdb
