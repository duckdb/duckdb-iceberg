#include "iceberg_extension.hpp"

#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb.hpp"

#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction_manager.hpp"
#include "function/iceberg_functions.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/storage/authorization/oauth2.hpp"
#include "catalog/rest/storage/authorization/sigv4.hpp"
#include "common/iceberg_utils.hpp"
#include "iceberg_logging.hpp"
#include "function/copy/iceberg_copy_function.hpp"

namespace duckdb {

static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                               AttachedDatabase &db, Catalog &catalog) {
	auto &ic_catalog = catalog.Cast<IcebergCatalog>();
	return make_uniq<IcebergTransactionManager>(db, ic_catalog);
}

class IRCStorageExtension : public StorageExtension {
public:
	IRCStorageExtension() {
		attach = IcebergCatalog::Attach;
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
	config.AddExtensionOption("iceberg_test_force_token_expiry",
	                          "DEBUG SETTING: force OAuth2 token expiry for testing automatic refresh",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Iceberg Table Functions
	for (auto &fun : IcebergFunctions::GetTableFunctions(loader)) {
		loader.RegisterFunction(std::move(fun));
	}

	// Iceberg Scalar Functions
	for (auto &fun : IcebergFunctions::GetScalarFunctions()) {
		loader.RegisterFunction(fun);
	}

	// Iceberg COPY Function
	loader.RegisterFunction(IcebergCopyFunction::Create());

	SecretType secret_type;
	secret_type.name = "iceberg";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);
	CreateSecretFunction secret_function = {"iceberg", "config", OAuth2Authorization::CreateCatalogSecretFunction};
	OAuth2Authorization::SetCatalogSecretParameters(secret_function);
	loader.RegisterFunction(secret_function);

	// Register DATE -> INTEGER implicit cast.
	// Spark writes day-transform partition values as Avro 'date' logical type (int32 days since epoch) when it should
	// be int. DuckDB's date_t is also int32 days since epoch, so this is a physical no-op reinterpretation. Cost 200
	// keeps it low-priority so it doesn't interfere with standard casts elsewhere.
	loader.RegisterCastFunction(LogicalType::DATE, LogicalType::INTEGER, BoundCastInfo(DefaultCasts::ReinterpretCast),
	                            200);

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
