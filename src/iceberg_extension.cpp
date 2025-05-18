#define DUCKDB_EXTENSION_MAIN

#include "iceberg_extension.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction_manager.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "iceberg_functions.hpp"
#include "storage/irc_authorization.hpp"
#include "yyjson.hpp"
#include "catalog_api.hpp"
#include "aws/core/Aws.h"
#include "aws/s3/S3Client.h"
#include "duckdb/main/extension_helper.hpp"

#include "storage/authorization/oauth2.hpp"
#include "storage/authorization/sigv4.hpp"

namespace duckdb {
// namespace
namespace {

enum class IcebergEndpointType : uint8_t { AWS_S3TABLES, AWS_GLUE, INVALID };

static IcebergEndpointType EndpointTypeFromString(const string &input) {
	D_ASSERT(StringUtil::Lower(input) == input);

	static const case_insensitive_map_t<IcebergEndpointType> mapping {{"glue", IcebergEndpointType::AWS_GLUE},
	                                                                  {"s3_tables", IcebergEndpointType::AWS_S3TABLES}};

	for (auto &entry : mapping) {
		if (entry.first == input) {
			return entry.second;
		}
	}
	set<string> options;
	for (auto &entry : mapping) {
		options.insert(entry.first);
	}
	throw InvalidConfigurationException("Unrecognized 'endpoint_type' (%s), accepted options are: %s", input,
	                                    StringUtil::Join(options, ", "));
}

} // namespace

//! Streamlined initialization for recognized catalog types

static void S3OrGlueAttachInternal(IcebergAttachOptions &input, const string &service, const string &region) {
	if (input.authorization_type != IRCAuthorizationType::INVALID) {
		throw InvalidConfigurationException("'endpoint_type' can not be combined with 'authorization_type'");
	}

	input.authorization_type = IRCAuthorizationType::SIGV4;
	input.endpoint = StringUtil::Format("%s.%s.amazonaws.com/iceberg", service, region);
}

static void S3TablesAttach(IcebergAttachOptions &input) {
	// extract region from the amazon ARN
	auto substrings = StringUtil::Split(input.warehouse, ":");
	if (substrings.size() != 6) {
		throw InvalidInputException("Could not parse S3 Tables ARN warehouse value");
	}
	auto region = substrings[3];
	S3OrGlueAttachInternal(input, "s3tables", region);
}

static bool SanityCheckGlueWarehouse(const string &warehouse) {
	// valid glue catalog warehouse is <account_id>:s3tablescatalog/<bucket>
	auto end_account_id = warehouse.find_first_of(':');
	bool account_id_correct = end_account_id == 12;
	auto bucket_sep = warehouse.find_first_of('/');
	bool bucket_sep_correct = bucket_sep == 28;
	if (!account_id_correct) {
		throw InvalidConfigurationException("Invalid Glue Catalog Format: '%s'. Expect 12 digits for account_id.",
		                                    warehouse);
	}
	if (bucket_sep_correct) {
		return true;
	}
	throw InvalidConfigurationException(
	    "Invalid Glue Catalog Format: '%s'. Expected '<account_id>:s3tablescatalog/<bucket>", warehouse);
}

static void GlueAttach(ClientContext &context, IcebergAttachOptions &input) {
	SanityCheckGlueWarehouse(input.warehouse);

	string secret;
	auto secret_it = input.options.find("secret");
	if (secret_it != input.options.end()) {
		secret = secret_it->second.ToString();
	}

	// look up any s3 secret

	// if there is no secret, an error will be thrown
	auto secret_entry = IRCatalog::GetStorageSecret(context, secret);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
	auto region = kv_secret.TryGetValue("region");

	if (region.IsNull()) {
		throw InvalidConfigurationException("Assumed catalog secret '%s' for catalog '%s' does not have a region",
		                                    secret_entry->secret->GetName(), input.name);
	}
	S3OrGlueAttachInternal(input, "glue", region.ToString());
}

//! Base attach method

static unique_ptr<Catalog> IcebergCatalogAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                                AttachedDatabase &db, const string &name, AttachInfo &info,
                                                AccessMode access_mode) {
	IRCEndpointBuilder endpoint_builder;

	string endpoint_type_string;
	string authorization_type_string;

	IcebergAttachOptions attach_options;
	attach_options.warehouse = info.path;
	attach_options.name = name;

	// check if we have a secret provided
	string secret_name;
	//! First handle generic attach options
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			continue;
		}

		if (lower_name == "endpoint_type") {
			endpoint_type_string = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "authorization_type") {
			authorization_type_string = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "endpoint") {
			attach_options.endpoint = StringUtil::Lower(entry.second.ToString());
			StringUtil::RTrim(attach_options.endpoint, "/");
		} else {
			attach_options.options.emplace(std::move(entry));
		}
	}

	//! Then check any if the 'endpoint_type' is set, for any well known catalogs
	if (!endpoint_type_string.empty()) {
		auto endpoint_type = EndpointTypeFromString(endpoint_type_string);
		switch (endpoint_type) {
		case IcebergEndpointType::AWS_GLUE: {
			GlueAttach(context, attach_options);
			break;
		}
		case IcebergEndpointType::AWS_S3TABLES: {
			S3TablesAttach(attach_options);
			break;
		}
		default:
			throw InternalException("Endpoint type (%s) not implemented", endpoint_type_string);
		}
	}

	//! Then check the authorization type
	if (!authorization_type_string.empty()) {
		if (attach_options.authorization_type != IRCAuthorizationType::INVALID) {
			throw InvalidConfigurationException("'authorization_type' can not be combined with 'endpoint_type'");
		}
		attach_options.authorization_type = IRCAuthorization::TypeFromString(authorization_type_string);
	}
	if (attach_options.authorization_type == IRCAuthorizationType::INVALID) {
		attach_options.authorization_type = IRCAuthorizationType::OAUTH2;
	}

	//! Finally, create the auth_handler class from the authorization_type and the remaining options
	unique_ptr<IRCAuthorization> auth_handler;
	switch (attach_options.authorization_type) {
	case IRCAuthorizationType::OAUTH2: {
		auth_handler = OAuth2Authorization::FromAttachOptions(context, attach_options);
		break;
	}
	case IRCAuthorizationType::SIGV4: {
		auth_handler = SIGV4Authorization::FromAttachOptions(attach_options);
		break;
	}
	default:
		throw InternalException("Authorization Type (%s) not implemented", authorization_type_string);
	}

	//! We throw if there are any additional options not handled by previous steps
	if (!attach_options.options.empty()) {
		set<string> unrecognized_options;
		for (auto &entry : attach_options.options) {
			unrecognized_options.insert(entry.first);
		}
		throw InvalidConfigurationException("Unhandled options found: %s",
		                                    StringUtil::Join(unrecognized_options, ", "));
	}

	if (attach_options.endpoint.empty()) {
		throw InvalidConfigurationException("Missing 'endpoint' option for Iceberg attach");
	}

	D_ASSERT(auth_handler);
	auto catalog = make_uniq<IRCatalog>(db, access_mode, std::move(auth_handler), attach_options.warehouse,
	                                    attach_options.endpoint);
	catalog->GetConfig(context);
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

	ExtensionHelper::AutoLoadExtension(instance, "parquet");
	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The iceberg extension requires the parquet extension to be loaded!");
	}

	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption("unsafe_enable_version_guessing",
	                          "Enable globbing the filesystem (if possible) to find the latest version metadata. This "
	                          "could result in reading an uncommitted version.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Iceberg Table Functions
	for (auto &fun : IcebergFunctions::GetTableFunctions(instance)) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	// Iceberg Scalar Functions
	for (auto &fun : IcebergFunctions::GetScalarFunctions()) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	SecretType secret_type;
	secret_type.name = "iceberg";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);
	CreateSecretFunction secret_function = {"iceberg", "config", OAuth2Authorization::CreateCatalogSecretFunction};
	OAuth2Authorization::SetCatalogSecretParameters(secret_function);
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
