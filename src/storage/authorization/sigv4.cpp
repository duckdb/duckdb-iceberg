#include "storage/authorization/sigv4.hpp"
#include "api_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/setting_info.hpp"
#include "storage/catalog/iceberg_catalog.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

namespace {

struct HostDecompositionResult {
	string authority;
	vector<string> path_components;
};

HostDecompositionResult DecomposeHost(const string &host) {
	HostDecompositionResult result;

	auto start_of_path = host.find('/');
	if (start_of_path != string::npos) {
		//! Authority consists of everything (assuming the host does not contain the scheme) before the first slash
		result.authority = host.substr(0, start_of_path);
		auto remainder = host.substr(start_of_path + 1);
		result.path_components = StringUtil::Split(remainder, '/');
	} else {
		result.authority = host;
	}
	return result;
}

} // namespace

SIGV4Authorization::SIGV4Authorization() : IcebergAuthorization(IcebergAuthorizationType::SIGV4) {
}

SIGV4Authorization::SIGV4Authorization(const string &secret)
    : IcebergAuthorization(IcebergAuthorizationType::SIGV4), secret(secret) {
}

unique_ptr<IcebergAuthorization> SIGV4Authorization::FromAttachOptions(IcebergAttachOptions &input) {
	auto result = make_uniq<SIGV4Authorization>();

	unordered_map<string, Value> remaining_options;
	for (auto &entry : input.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			if (!result->secret.empty()) {
				throw InvalidInputException("Duplicate 'secret' option detected!");
			}
			result->secret = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "extra_http_headers") {
			// Parse extra_http_headers if provided directly in attach options
			IcebergAuthorization::ParseExtraHttpHeaders(entry.second, result->extra_http_headers);
		} else {
			remaining_options.emplace(std::move(entry));
		}
	}
	input.options = std::move(remaining_options);
	return std::move(result);
}

static bool IsAwsRegion(const string &token) {
	static const vector<string> prefixes = {"us-", "eu-", "ap-", "sa-", "ca-", "me-", "af-", "il-", "mx-"};
	bool has_prefix = false;
	for (auto &prefix : prefixes) {
		if (StringUtil::StartsWith(token, prefix)) {
			has_prefix = true;
			break;
		}
	}
	if (!has_prefix) {
		return false;
	}
	if (token.empty() || !StringUtil::CharacterIsDigit(token.back())) {
		return false;
	}
	return true;
}

static string GetAwsRegion(const string &host) {
	auto parts = StringUtil::Split(host, '.');
	for (auto &part : parts) {
		if (IsAwsRegion(part)) {
			return part;
		}
	}
	throw InvalidInputException("Could not parse AWS region from host: %s", host);
}

static string GetAwsService(const string &host) {
	auto parts = StringUtil::Split(host, '.');
	for (idx_t i = 0; i < parts.size(); i++) {
		if (IsAwsRegion(parts[i]) && i > 0) {
			return parts[i - 1];
		}
	}
	throw InvalidInputException("Could not parse AWS service from host: %s", host);
}

AWSInput SIGV4Authorization::CreateAWSInput(ClientContext &context, const IRCEndpointBuilder &endpoint_builder) {
	AWSInput aws_input;
	aws_input.cert_path = APIUtils::GetCURLCertPath();

	// Set the user Agent
	auto &config = DBConfig::GetConfig(context);
	aws_input.user_agent = config.UserAgent();
	Value val;
	auto lookup_result = context.TryGetCurrentSetting("http_timeout", val);
	if (lookup_result.GetScope() != SettingScope::INVALID) {
		aws_input.use_httpfs_timeout = true;
		// http timeout is in seconds, multiply by 1000 to get ms
		aws_input.request_timeout_in_ms = val.GetValue<idx_t>() * 1000;
	}

	// AWS service and region
	aws_input.service = GetAwsService(endpoint_builder.GetHost());
	aws_input.region = GetAwsRegion(endpoint_builder.GetHost());

	// Host decomposition
	auto decomposed_host = DecomposeHost(endpoint_builder.GetHost());
	aws_input.authority = decomposed_host.authority;

	for (auto &component : decomposed_host.path_components) {
		aws_input.path_segments.push_back(component);
	}
	for (auto &component : endpoint_builder.path_components) {
		aws_input.path_segments.push_back(component);
	}
	for (auto &param : endpoint_builder.GetParams()) {
		aws_input.query_string_parameters.emplace_back(param);
	}

	// AWS credentials
	auto secret_entry = IcebergCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	aws_input.key_id = kv_secret.secret_map["key_id"].GetValue<string>();
	aws_input.secret = kv_secret.secret_map["secret"].GetValue<string>();
	aws_input.session_token =
	    kv_secret.secret_map["session_token"].IsNull() ? "" : kv_secret.secret_map["session_token"].GetValue<string>();

	return aws_input;
}

unique_ptr<HTTPResponse> SIGV4Authorization::Request(RequestType request_type, ClientContext &context,
                                                     const IRCEndpointBuilder &endpoint_builder, HTTPHeaders &headers,
                                                     const string &data) {
	// Note: For SIGV4, custom headers should be added BEFORE signing so they're included in the signature
	// Merge extra HTTP headers first
	for (auto &entry : extra_http_headers) {
		headers.Insert(entry.first, entry.second);
	}

	auto aws_input = CreateAWSInput(context, endpoint_builder);
	return aws_input.Request(request_type, context, client, headers, data);
}

} // namespace duckdb
