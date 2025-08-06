#include "storage/authorization/sigv4.hpp"
#include "api_utils.hpp"
#include "storage/irc_catalog.hpp"

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
	return std::move(result);
}

static string GetAwsRegion(const string &host) {
	idx_t first_dot = host.find_first_of('.');
	idx_t second_dot = host.find_first_of('.', first_dot + 1);
	return host.substr(first_dot + 1, second_dot - first_dot - 1);
}

static string GetAwsService(const string &host) {
	return host.substr(0, host.find_first_of('.'));
}

unique_ptr<HTTPResponse> SIGV4Authorization::PostRequest(ClientContext &context,
                                                         const IRCEndpointBuilder &endpoint_builder,
                                                         const string &body) {
	AWSInput aws_input;
	aws_input.cert_path = APIUtils::GetCURLCertPath();

	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	aws_input.user_agent = config.UserAgent();

	aws_input.service = GetAwsService(endpoint_builder.GetHost());
	aws_input.region = GetAwsRegion(endpoint_builder.GetHost());

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

	// Will error if no secret can be found for AWS services
	auto secret_entry = IRCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	aws_input.key_id = kv_secret.secret_map["key_id"].GetValue<string>();
	aws_input.secret = kv_secret.secret_map["secret"].GetValue<string>();
	aws_input.session_token =
	    kv_secret.secret_map["session_token"].IsNull() ? "" : kv_secret.secret_map["session_token"].GetValue<string>();

	return aws_input.PostRequest(context, body);
}

unique_ptr<HTTPResponse> SIGV4Authorization::GetRequest(ClientContext &context,
                                                        const IRCEndpointBuilder &endpoint_builder) {
	AWSInput aws_input;
	aws_input.cert_path = APIUtils::GetCURLCertPath();
	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	aws_input.user_agent = config.UserAgent();

	aws_input.service = GetAwsService(endpoint_builder.GetHost());
	aws_input.region = GetAwsRegion(endpoint_builder.GetHost());

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

	// will error if no secret can be found for AWS services
	auto secret_entry = IRCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	aws_input.key_id = kv_secret.secret_map["key_id"].GetValue<string>();
	aws_input.secret = kv_secret.secret_map["secret"].GetValue<string>();
	aws_input.session_token =
	    kv_secret.secret_map["session_token"].IsNull() ? "" : kv_secret.secret_map["session_token"].GetValue<string>();

	return aws_input.GetRequest(context);
}

unique_ptr<HTTPResponse> SIGV4Authorization::DeleteRequest(ClientContext &context,
                                                           const IRCEndpointBuilder &endpoint_builder) {
	AWSInput aws_input;
	aws_input.cert_path = APIUtils::GetCURLCertPath();
	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	aws_input.user_agent = config.UserAgent();

	aws_input.service = GetAwsService(endpoint_builder.GetHost());
	aws_input.region = GetAwsRegion(endpoint_builder.GetHost());

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

	// will error if no secret can be found for AWS services
	auto secret_entry = IRCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	aws_input.key_id = kv_secret.secret_map["key_id"].GetValue<string>();
	aws_input.secret = kv_secret.secret_map["secret"].GetValue<string>();
	aws_input.session_token =
	    kv_secret.secret_map["session_token"].IsNull() ? "" : kv_secret.secret_map["session_token"].GetValue<string>();

	return aws_input.DeleteRequest(context);
}

} // namespace duckdb
