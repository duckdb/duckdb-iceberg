#include "catalog/rest/api/catalog_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/logging/logger.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

void ICUtils::LogPostBody(ClientContext &context, const IRCEndpointBuilder &url_builder, const string &body) {
	if (!Logger::Get(context).ShouldLog(IcebergLogType::NAME, IcebergLogType::LEVEL)) {
		return;
	}
	idx_t truncate_limit = 10000;
	Value limit_value;
	if (context.TryGetCurrentSetting("iceberg_logging_post_body_truncate_limit", limit_value)) {
		truncate_limit = limit_value.GetValue<idx_t>();
	}
	string body_to_log;
	if (truncate_limit == 0) {
		body_to_log = "<body omitted>";
	} else if (body.size() > truncate_limit) {
		body_to_log = body.substr(0, truncate_limit) + "... (truncated)";
	} else {
		body_to_log = body;
	}
	DUCKDB_LOG(context, IcebergLogType, "POST %s body=%s", url_builder.GetURLEncoded(), body_to_log);
}

JSONValue ICUtils::GetErrorMessage(const string &api_result, unique_ptr<JSONDocument> &out_doc) {
	JSONParseError parse_error;
	out_doc = JSONDocument::TryParse(api_result.c_str(), api_result.size(), parse_error);
	if (!out_doc) {
		return JSONValue();
	}

	auto root = out_doc->GetRoot();
	auto error = root.GetMember("error");

	if (!error.IsValid()) {
		return JSONValue();
	}
	auto message = error.GetMember("message");
	auto type = error.GetMember("type");
	auto code = error.GetMember("code");
	if (message.IsValid() && type.IsValid() && code.IsValid()) {
		return root;
	}
	return JSONValue();
}

unique_ptr<JSONDocument> ICUtils::APIResultToDoc(const string &api_result) {
	auto doc = JSONDocument::Parse(api_result.c_str(), api_result.size());
	auto root = doc->GetRoot();
	auto error = root.GetMember("error");
	if (error.IsValid()) {
		try {
			auto message = error.GetMember("message");
			throw InvalidInputException(message.IsString() ? message.GetString() : "No message available");
		} catch (InvalidConfigurationException &e) {
			// keep going, we will throw the whole api result as an error message
			throw InvalidConfigurationException(api_result);
		}
		throw InvalidConfigurationException("Could not parse api_result");
	}
	return doc;
}

} // namespace duckdb
