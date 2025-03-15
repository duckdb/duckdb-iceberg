#define DUCKDB_EXTENSION_MAIN

#include "iceberg_extension.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction_manager.hpp"

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "iceberg_functions.hpp"
#include "yyjson.hpp"
#include "catalog_api.hpp"
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <regex>

namespace duckdb {

static unique_ptr<BaseSecret> CreateCatalogSecretFunction(ClientContext &context, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "iceberg", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "key_id" ||
		    lower_name == "secret" ||
		    lower_name == "endpoint" ||
		    lower_name == "aws_region") {
			result->secret_map[lower_name] = named_param.second.ToString();
		} else {
			throw InternalException("Unknown named parameter passed to CreateIRCSecretFunction: " + lower_name);
		}
	}

	// Get token from catalog
	result->secret_map["token"] = IRCAPI::GetToken(
		context,
	    result->secret_map["key_id"].ToString(),
	    result->secret_map["secret"].ToString(),
	    result->secret_map["endpoint"].ToString());
	
	//! Set redact keys
	result->redact_keys = {"token", "client_id", "client_secret"};

	return std::move(result);
}

static void SetCatalogSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["client_id"] = LogicalType::VARCHAR;
	function.named_parameters["client_secret"] = LogicalType::VARCHAR;
	function.named_parameters["endpoint"] = LogicalType::VARCHAR;
	function.named_parameters["aws_region"] = LogicalType::VARCHAR;
	function.named_parameters["token"] = LogicalType::VARCHAR;
}

static bool SanityCheckGlueWarehouse(string warehouse) {
    // See: https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html#prefix-catalog-path-parameters

	const std::regex patterns[] = {
        std::regex("^:$"),                   // Default catalog ":" in current account
        std::regex("^\\d{12}$"),             // Default catalog in a specific account
        std::regex("^\\d{12}:[^:/]+$"),      // Specific catalog in a specific account
        std::regex("^[^:]+/[^:]+$"),         // Nested catalog in the current account
        std::regex("^\\d{12}:[^/]+/[^:]+$")  // Nested catalog in a specific account
    };

    for (const auto& pattern : patterns) {
        if (std::regex_match(warehouse, pattern)) {
            return true;
        }
    }

	throw IOException("Invalid Glue Catalog Format: '" + warehouse + "'. Expected format: ':', '12-digit account ID', 'catalog1/catalog2', or '12-digit accountId:catalog1/catalog2'.");
}

static unique_ptr<Catalog> IcebergCatalogAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                           AttachedDatabase &db, const string &name, AttachInfo &info,
                                           AccessMode access_mode) {
	IRCCredentials credentials;
	IRCEndpointBuilder endpoint_builder;

	string account_id;
	string service;
	string endpoint_type;
	string endpoint;

	// check if we have a secret provided
	string secret_name;
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			// already handled
		} else if (lower_name == "secret") {
			secret_name = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "endpoint_type") {
			endpoint_type = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "endpoint") {
			endpoint = StringUtil::Lower(entry.second.ToString());
			StringUtil::RTrim(endpoint, "/");
		} else {
			throw BinderException("Unrecognized option for PC attach: %s", entry.first);
		}
	}
	auto warehouse = info.path;

	if (endpoint_type == "glue" || endpoint_type == "s3_tables") {
		if (endpoint_type == "s3_tables") {
			service = "s3tables";
		} else {
			service = endpoint_type;
		}
		// look up any s3 secret
		auto catalog = make_uniq<IRCatalog>(db, access_mode, credentials);
		// if there is no secret, an error will be thrown
		auto secret_entry = IRCatalog::GetSecret(context, secret_name);
        auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		auto region = kv_secret.TryGetValue("region");
		if (region.IsNull()) {
			throw IOException("Assumed catalog secret " + secret_entry->secret->GetName() + " for catalog " + name + " does not have a region");
		}
		if (service == "s3tables") {
			auto substrings = StringUtil::Split(warehouse, ":");
			if (substrings.size() != 6) {
				throw InvalidInputException("Could not parse S3 Tables arn warehouse value");
			}
			region = Value::CreateValue<string>(substrings[3]);
			catalog->warehouse = warehouse;
		}
		else if (service == "glue") {
			SanityCheckGlueWarehouse(warehouse);
			catalog->warehouse = StringUtil::Replace(warehouse, "/", ":");
		}
		catalog->host = service + "." + region.ToString() + ".amazonaws.com";
		catalog->version = "v1";
		catalog->secret_name = secret_name;
		return std::move(catalog);
	}

	if (!endpoint_type.empty()) {
		throw IOException("Unrecognized endpoint point: %s. Expected either S3_TABLES or GLUE", endpoint_type);
	}
	if (endpoint_type.empty() && endpoint.empty()) {
		throw IOException("No endpoint type or endpoint provided");
	}

	// Default IRC path
	Value endpoint_val;
	// Lookup a secret we can use to access the rest catalog.
	// if no secret is referenced, this throw
	auto secret_entry = IRCatalog::GetSecret(context, secret_name);
	if (!secret_entry) {
		throw IOException("No secret found to use with catalog " + name);
	}
 	// secret found - read data
 	const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	Value key_val = kv_secret.TryGetValue("key_id");
	Value secret_val = kv_secret.TryGetValue("secret");
	CreateSecretInput create_secret_input;
	create_secret_input.options["key_id"] = key_val;
	create_secret_input.options["secret"] = secret_val;
	create_secret_input.options["endpoint"] = endpoint;
	auto new_secret = CreateCatalogSecretFunction(context, create_secret_input);
	auto &kv_secret_new = dynamic_cast<KeyValueSecret &>(*new_secret);
	Value token = kv_secret_new.TryGetValue("token");
	if (token.IsNull()) {
		throw IOException("Failed to generate oath token");
	}
	credentials.token = token.ToString();
	auto catalog = make_uniq<IRCatalog>(db, access_mode, credentials);
	catalog->host = endpoint;
	catalog->warehouse = warehouse;
	catalog->version = "v1";
	catalog->secret_name = secret_name;
	return std::move(catalog);
}

static unique_ptr<TransactionManager> CreateTransactionManager(StorageExtensionInfo *storage_info, AttachedDatabase &db,
                                                               Catalog &catalog) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	return make_uniq<ICTransactionManager>(db, ic_catalog);
}

class IRCStorageExtension : public StorageExtension {
public:
	IRCStorageExtension() {
		attach = IcebergCatalogAttach;
		create_transaction_manager = CreateTransactionManager;
	}
};

static void LoadInternal(DatabaseInstance &instance) {
	Aws::SDKOptions options;
	Aws::InitAPI(options); // Should only be called once.

	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption(
		"unsafe_enable_version_guessing",
		"Enable globbing the filesystem (if possible) to find the latest version metadata. This could result in reading an uncommitted version.",
		LogicalType::BOOLEAN,
		Value::BOOLEAN(false)
	);

	// Iceberg Table Functions
	for (auto &fun : IcebergFunctions::GetTableFunctions()) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	// Iceberg Scalar Functions
	for (auto &fun : IcebergFunctions::GetScalarFunctions()) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	IRCAPI::InitializeCurl();

	SecretType secret_type;
	secret_type.name = "iceberg";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);
	CreateSecretFunction secret_function = {"iceberg", "config", CreateCatalogSecretFunction};
	SetCatalogSecretParameters(secret_function);
	ExtensionUtil::RegisterFunction(instance, secret_function);

	config.storage_extensions["iceberg"] = make_uniq<IRCStorageExtension>();
}

void IcebergExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string IcebergExtension::Name() {
	return "iceberg";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void iceberg_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *iceberg_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
