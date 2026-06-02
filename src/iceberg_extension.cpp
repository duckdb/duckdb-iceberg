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
#include "iceberg_options.hpp"
#include "function/copy/iceberg_copy_function.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "planning/iceberg_optimizer.hpp"

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
	config.AddExtensionOption(
	    "iceberg_use_metadata_log",
	    "Whether or not to make use of the (optional) 'metadata-log' of a table to ensure atomicity guarantees hold, "
	    "at the cost of making another GET for json metadata in rare circumstances",
	    LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption("ignore_target_file_size_for_partitioned_tables",
	                          "Ignore unsupported write.target-file-size-bytes table property for partitioned tables",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption(
	    "ignore_row_group_size_for_partitioned_tables",
	    "Ignore unsupported write.parquet.row-group-size-bytes table property for partitioned tables",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption(
	    "iceberg_logging_post_body_truncate_limit",
	    "Maximum number of characters of a REST catalog POST body to include in Iceberg log messages. "
	    "Bodies longer than this are truncated with a trailing '... (truncated)' marker. Set to 0 to omit the body.",
	    LogicalType::UBIGINT, Value::UBIGINT(10000));
	config.AddExtensionOption(
	    "unsafe_iceberg_ignore_sort_order",
	    "Allow INSERT/UPDATE on iceberg tables that declare a sort order, without applying that sort order to "
	    "the written data. The Iceberg spec permits this (writers are not required to honour a declared sort "
	    "order, and readers do not assume files are sorted), but skipping the sort may reduce later file-pruning "
	    "effectiveness and compression.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false));
#ifdef ICEBERG_ENABLE_EQUALITY_DELETE_WRITES
	config.AddExtensionOption(
	    ENABLE_EQUALITY_DELETES_CONFIG_VARIABLE,
	    "DANGEROUS TESTING-ONLY SETTING: when enabled, a DELETE on a v2 Iceberg table whose WHERE clause is a pure "
	    "conjunction of equality predicates writes an Iceberg equality-delete file. Used to exercise the "
	    "equality-delete read path.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false));
#endif

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

	auto &log_manager = instance.GetLogManager();
	log_manager.RegisterLogType(make_uniq<IcebergLogType>());
	StorageExtension::Register(config, "iceberg", make_shared_ptr<IRCStorageExtension>());

	// Re-introduces equality-delete columns onto iceberg_scan LogicalGets after the built-in
	// optimizers have run; see planning/iceberg_optimizer.hpp for the why.
	OptimizerExtension::Register(config, IcebergOptimizer::Create());
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
