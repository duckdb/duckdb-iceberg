#include "storage/authorization/sigv4.hpp"
#include "api_utils.hpp"
#include <curl/curl.h>

namespace duckdb {

SIGV4Authorization::SIGV4Authorization() : IRCAuthorization(IRCAuthorizationType::SIGV4) {
}
SIGV4Authorization::SIGV4Authorization(const string &secret)
    : IRCAuthorization(IRCAuthorizationType::SIGV4), secret(secret) {
}

unique_ptr<IRCAuthorization> SIGV4Authorization::FromAttachOptions(IcebergAttachOptions &input) {
	auto result = make_uniq<SIGV4Authorization>();

	unordered_map<string, Value> remaining_options;
	for (auto &entry : input.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			if (!result->secret.empty()) {
				throw InvalidInputException("Duplicate 'secret' option detected!");
			}
			result->secret = StringUtil::Lower(entry.second.ToString());
		} else {
			remaining_options.emplace(std::move(entry));
		}
	}
	input.options = std::move(remaining_options);
	return result;
}

string SIGV4Authorization::GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
                                      curl_slist *extra_headers) {
	return APIUtils::GetRequestAws(context, endpoint_builder, secret);
}

} // namespace duckdb
