#include "catalog/aws/lakeformation/lf_credentials.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "catalog/rest/storage/aws.hpp"

#ifndef EMSCRIPTEN

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/AssumeRoleRequest.h>

namespace duckdb {

namespace {

class DuckDBSecretCredentialProvider : public Aws::Auth::AWSCredentialsProvider {
public:
	DuckDBSecretCredentialProvider(const string &key_id, const string &secret, const string &session_token) {
		credentials.SetAWSAccessKeyId(key_id.c_str());
		credentials.SetAWSSecretKey(secret.c_str());
		credentials.SetSessionToken(session_token.c_str());
	}

	Aws::Auth::AWSCredentials GetAWSCredentials() override {
		return credentials;
	}

private:
	Aws::Auth::AWSCredentials credentials;
};

class LakeFormationTaggedAssumeRoleProvider : public Aws::Auth::AWSCredentialsProvider {
public:
	// LF application integration requires the assumed role session to carry the
	// LakeFormationAuthorizedCaller tag (configured via LF_SESSION_TAG on attach).
	// Glue/LF APIs are then called with those tagged credentials.
	LakeFormationTaggedAssumeRoleProvider(std::shared_ptr<Aws::Auth::AWSCredentialsProvider> base_provider,
	                                      const string &role_arn, const string &session_tag, const string &region)
	    : base_provider(std::move(base_provider)), role_arn(role_arn), session_tag(session_tag) {
		::Aws::Client::ClientConfiguration config;
		config.region = region;
		sts_client =
		    Aws::MakeShared<::Aws::STS::STSClient>("LakeFormationTaggedAssumeRoleProvider", base_provider, config);
	}

	Aws::Auth::AWSCredentials GetAWSCredentials() override {
		Aws::STS::Model::AssumeRoleRequest request;
		request.SetRoleArn(role_arn);
		request.SetRoleSessionName("duckdb-lakeformation");
		request.AddTags(Aws::STS::Model::Tag().WithKey("LakeFormationAuthorizedCaller").WithValue(session_tag));
		auto outcome = sts_client->AssumeRole(request);
		if (!outcome.IsSuccess()) {
			throw InvalidConfigurationException("Failed to assume role for Lake Formation integration: %s",
			                                    outcome.GetError().GetMessage());
		}
		auto &creds = outcome.GetResult().GetCredentials();
		return Aws::Auth::AWSCredentials(creds.GetAccessKeyId(), creds.GetSecretAccessKey(), creds.GetSessionToken());
	}

private:
	std::shared_ptr<Aws::Auth::AWSCredentialsProvider> base_provider;
	std::shared_ptr<Aws::STS::STSClient> sts_client;
	string role_arn;
	string session_tag;
};

static void InitAWSAPI() {
	static bool loaded = false;
	if (!loaded) {
		Aws::SDKOptions options;
		Aws::InitAPI(options);
		loaded = true;
	}
}

} // namespace

LakeFormationClientConfig BuildLakeFormationClientConfig(ClientContext &context, IcebergCatalog &catalog) {
	InitAWSAPI();
	LakeFormationClientConfig result;
	if (catalog.auth_handler->type != IcebergAuthorizationType::SIGV4) {
		throw InvalidConfigurationException(
		    "Lake Formation data filters require SIGV4 authorization on the Iceberg catalog");
	}
	auto &sigv4 = catalog.auth_handler->Cast<SIGV4Authorization>();
	auto secret_entry = IcebergCatalog::GetStorageSecret(context, sigv4.secret);
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	result.region = kv_secret.TryGetValue("region").ToString();
	if (result.region.empty() && !sigv4.sigv4_region.empty()) {
		result.region = sigv4.sigv4_region;
	}
	if (result.region.empty()) {
		throw InvalidConfigurationException("Lake Formation integration requires a region in the catalog secret");
	}

	auto key_id = kv_secret.TryGetValue("key_id").ToString();
	auto secret = kv_secret.TryGetValue("secret").ToString();
	auto session_token =
	    kv_secret.TryGetValue("session_token").IsNull() ? "" : kv_secret.TryGetValue("session_token").ToString();

	std::shared_ptr<::Aws::Auth::AWSCredentialsProvider> provider =
	    std::make_shared<DuckDBSecretCredentialProvider>(key_id, secret, session_token);

	auto assume_role_arn = kv_secret.TryGetValue("assume_role_arn");
	// When a data-filter grant targets a role, the catalog secret's base credentials
	// assume that role with the LF session tag before any Glue/LF SDK calls.
	if (!assume_role_arn.IsNull() && !catalog.attach_options.lf_session_tag.empty()) {
		provider = std::make_shared<LakeFormationTaggedAssumeRoleProvider>(
		    provider, assume_role_arn.ToString(), catalog.attach_options.lf_session_tag, result.region);
	}

	result.credentials_provider = std::move(provider);
	result.aws_config.region = result.region;
	return result;
}

} // namespace duckdb

#else

namespace duckdb {

LakeFormationClientConfig BuildLakeFormationClientConfig(ClientContext &context, IcebergCatalog &catalog) {
	throw NotImplementedException("Lake Formation integration is not available in duckdb-wasm builds");
}

} // namespace duckdb

#endif
