#include "iceberg_extension.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction_manager.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "iceberg_functions.hpp"
#include "catalog_api.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "storage/authorization/oauth2.hpp"
#include "storage/authorization/sigv4.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_logging.hpp"

namespace duckdb {

static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                               AttachedDatabase &db, Catalog &catalog) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	return make_uniq<ICTransactionManager>(db, ic_catalog);
}

class IRCStorageExtension : public StorageExtension {
public:
	IRCStorageExtension() {
		attach = IRCatalog::Attach;
		create_transaction_manager = CreateTransactionManager;
	}
};

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	ExtensionHelper::AutoLoadExtension(instance, "parquet");
	ExtensionHelper::AutoLoadExtension(instance, "avro");

	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The iceberg extension requires the parquet extension to be loaded!");
	}
	if (!instance.ExtensionIsLoaded("avro")) {
		throw MissingExtensionException("The iceberg extension requires the avro extension to be loaded!");
	}

	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption("unsafe_enable_version_guessing",
	                          "Enable globbing the filesystem (if possible) to find the latest version metadata. This "
	                          "could result in reading an uncommitted version.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("iceberg_via_aws_sdk_for_catalog_interactions",
	                          "Use legacy code to interact with AWS-based catalogs, via AWS's SDK",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Iceberg Table Functions
	for (auto &fun : IcebergFunctions::GetTableFunctions(loader)) {
		loader.RegisterFunction(std::move(fun));
	}

	// Iceberg Scalar Functions
	for (auto &fun : IcebergFunctions::GetScalarFunctions()) {
		loader.RegisterFunction(fun);
	}

	SecretType secret_type;
	secret_type.name = "iceberg";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);
	CreateSecretFunction secret_function = {"iceberg", "config", OAuth2Authorization::CreateCatalogSecretFunction};
	OAuth2Authorization::SetCatalogSecretParameters(secret_function);
	loader.RegisterFunction(secret_function);

	auto &log_manager = instance.GetLogManager();
	log_manager.RegisterLogType(make_uniq<IcebergLogType>());
	StorageExtension::Register(config, "iceberg", make_shared_ptr<IRCStorageExtension>());
}

void IcebergExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
string IcebergExtension::Name() {
	return "iceberg";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(iceberg, loader) {
	LoadInternal(loader);
}
}
