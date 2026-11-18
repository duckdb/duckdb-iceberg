#include "catalog/rest/vendor/iceberg_glue_rest_backend.hpp"

#include "duckdb/main/secret/secret.hpp"

#include "catalog/iceberg_backend_registry.hpp"
#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_catalog.hpp"

#include <regex>

namespace duckdb {

IcebergGlueRestBackend::IcebergGlueRestBackend(const Identifier &catalog_name) : IcebergRestBackend(catalog_name) {
}

IcebergBackendFactory IcebergGlueRestBackend::Factory() {
	IcebergBackendFactory factory;
	factory.catalog_type = "rest";
	factory.endpoint_type = "glue";
	factory.matches = [](const IcebergAttachInput &input) {
		auto uri = input.client_options.find("uri");
		return uri != input.client_options.end() && !RegionFromURI(uri->second.ToString()).empty();
	};
	factory.create = [](IcebergAttachInput &input) -> unique_ptr<IcebergCatalogBackend> {
		auto backend = make_uniq<IcebergGlueRestBackend>(Identifier(input.name));
		backend->Attach(input);
		return std::move(backend);
	};
	return factory;
}

string IcebergGlueRestBackend::RegionFromURI(const string &uri) {
	auto host = StringUtil::Lower(StripScheme(uri));
	host = host.substr(0, host.find_first_of("/?#"));
	// Match the whole authority, not a Glue hostname appearing in a path or a
	// subdomain of another service. Accept explicit ports and China endpoints.
	static const std::regex pattern("^glue\\.([a-z0-9-]+)\\.amazonaws\\.com(\\.cn)?(:[0-9]+)?$");
	std::smatch match;
	if (!std::regex_match(host, match, pattern)) {
		return string();
	}
	return match[1].str();
}

namespace {

static bool SanityCheckGlueWarehouse(const string &warehouse) {
	// See: https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html#prefix-catalog-path-parameters

	const std::regex patterns[] = {
	    std::regex("^:$"),                  // Default catalog ":" in current account
	    std::regex("^\\d{12}$"),            // Default catalog in a specific account
	    std::regex("^\\d{12}:[^:/]+$"),     // Specific catalog in a specific account
	    std::regex("^[^:]+/[^:]+$"),        // Nested catalog in the current account
	    std::regex("^\\d{12}:[^/]+/[^:]+$") // Nested catalog in a specific account
	};

	for (const auto &pattern : patterns) {
		if (std::regex_match(warehouse, pattern)) {
			return true;
		}
	}

	throw InvalidConfigurationException(
	    "Invalid Glue Catalog Format: '%s'. Expected format: ':', '12-digit account ID', "
	    "'catalog1/catalog2', or '12-digit accountId:catalog1/catalog2'.",
	    warehouse);
}

static void GlueAttach(ClientContext &context, IcebergRestOptions &input) {
	SanityCheckGlueWarehouse(input.warehouse);

	string secret;
	auto secret_it = input.options.find("secret");
	if (secret_it != input.options.end()) {
		secret = secret_it->second.ToString();
	}

	// look up any s3 secret

	// if there is no secret, an error will be thrown
	auto secret_entry = IcebergCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	auto region = kv_secret.TryGetValue("region");

	if (region.IsNull()) {
		throw InvalidConfigurationException("Assumed catalog secret '%s' for catalog '%s' does not have a region",
		                                    secret_entry->secret->GetName().GetIdentifierName(), input.name);
	}
	IcebergRestBackend::UseAWSSigV4(input, "glue", region.ToString());
}

} // namespace

void IcebergGlueRestBackend::ConfigureEndpoint(IcebergAttachInput &input, IcebergRestOptions &options,
                                               const case_insensitive_set_t &set_by_user) {
	if (!input.endpoint_type.empty()) {
		// ENDPOINT_TYPE 'glue'
		GlueAttach(input.context, options);
		ApplyAWSPresets(input, options, set_by_user);
		return;
	}
	// A Glue endpoint URI
	auto glue_region = RegionFromURI(options.catalog_uri);
	ApplyAWSPresets(input, options, set_by_user);
	// URL-based attaches may already specify authorization and signing
	// options. Only supply defaults, using the URL's region.
	if (options.authorization_type == IcebergAuthorizationType::INVALID) {
		options.authorization_type = IcebergAuthorizationType::SIGV4;
	}
	if (options.authorization_type == IcebergAuthorizationType::SIGV4) {
		options.options.emplace("sigv4_service", Value("glue"));
		options.options.emplace("sigv4_region", Value(glue_region));
	}
}

void IcebergGlueRestBackend::AddFallbackOptions(unordered_map<string, Value> &fallback_options) {
	AddAWSFallbackOptions(fallback_options);
}

string IcebergGlueRestBackend::GetConfigWarehouse(const IcebergRestOptions &options) const {
	// The default catalog ':' of the account is selected with an empty warehouse
	if (options.warehouse == ":") {
		return string();
	}
	return options.warehouse;
}

void IcebergGlueRestBackend::AddDefaultEndpoints() {
	// insert namespaces based on REST API spec.
	// List namespaces
	supported_urls.insert("GET /v1/{prefix}/namespaces");
	// create namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces");
	// Load metadata for a Namespace
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}");
	// Drop a namespace
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}");
	// list all table identifiers
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables");
	// create table in the namespace
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables");
	// get table from the catalog
	supported_urls.insert("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// table exists
	supported_urls.insert("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// commit updates to a table
	supported_urls.insert("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}");
	// drop table from a catalog
	supported_urls.insert("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}");
}

void IcebergGlueRestBackend::AdjustCapabilities(IcebergBackendCapabilities &capabilities) const {
	//! Deleting a Glue namespace also deletes its tables
	capabilities.drop_namespace = IcebergDropNamespaceMode::CASCADE_ONLY;
}

} // namespace duckdb
