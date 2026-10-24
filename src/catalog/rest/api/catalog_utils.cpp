#include "catalog/rest/api/catalog_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

rest_api_objects::LoadTableResult ICUtils::ParseLoadTableResult(JSONValue root) {
	auto restrictions = root.GetMember("read-restrictions");
	auto row_filter = restrictions.GetMember("required-row-filter");
	auto projections = restrictions.GetMember("required-column-projections");
	bool has_projections = false;
	if (projections.IsArray()) {
		projections.IterateArray([&](JSONValue) { has_projections = true; });
	}
	// Check the raw response so unknown predicates and actions also fail before
	// the generated parser attempts to interpret them.
	if (has_projections || (row_filter.IsValid() && !row_filter.IsNull())) {
		throw NotImplementedException("Iceberg read-restrictions are not supported");
	}
	return rest_api_objects::LoadTableResult::FromJSON(root);
}

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

bool ICUtils::IsSensitiveConfigKey(const string &key) {
	// Match on fragments rather than an exact list of FileIO properties, so credentials vended under
	// properties we don't know about (yet) are redacted as well.
	static const char *const SENSITIVE_FRAGMENTS[] = {"secret",     "token",     "password", "credential",
	                                                  "shared-key", "signature", "sse.key"};
	// Properties that describe a credential without containing it, e.g. 's3.session-token-expires-at-ms'.
	static const char *const NON_SENSITIVE_SUFFIXES[] = {"-expires-at", "-expires-at-ms", "-expires-in-ms", "-enabled",
	                                                     "-endpoint"};

	auto lower_key = StringUtil::Lower(key);
	for (auto suffix : NON_SENSITIVE_SUFFIXES) {
		if (StringUtil::EndsWith(lower_key, suffix)) {
			return false;
		}
	}
	for (auto fragment : SENSITIVE_FRAGMENTS) {
		if (StringUtil::Contains(lower_key, fragment)) {
			return true;
		}
	}
	return false;
}

string ICUtils::RedactConfigValue(const string &key, const string &value) {
	return IsSensitiveConfigKey(key) ? "redacted" : value;
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
