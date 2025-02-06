#include "storage/ic_catalog.hpp"
#include "storage/ic_schema_entry.hpp"
#include "storage/ic_table_entry.hpp"
#include "storage/ic_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "catalog_api.hpp"
#include "../../duckdb/third_party/catch/catch.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"


namespace duckdb {

ICTableEntry::ICTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
}

ICTableEntry::ICTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, ICTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = false;
}

unique_ptr<BaseStatistics> ICTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void ICTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                         ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints");
}

struct MyIcebergFunctionData : public FunctionData {
    std::string path; // store the path or any other relevant info here

    // Optional: implement Copy for caching/pushdown logic
    unique_ptr<FunctionData> Copy() const override {
        auto copy = make_uniq<MyIcebergFunctionData>();
        copy->path = path;
        return copy;
    }

    // Optional: implement Equals for caching
    bool Equals(const FunctionData &other_p) const override {
        auto &other = (const MyIcebergFunctionData &)other_p;
        return path == other.path;
    }
};

TableFunction ICTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &ic_catalog = catalog.Cast<ICCatalog>();

	auto &parquet_function_set = ExtensionUtil::GetTableFunction(db, "parquet_scan");
	auto parquet_scan_function = parquet_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	auto &iceberg_function_set = ExtensionUtil::GetTableFunction(db, "iceberg_scan");
	auto iceberg_scan_function = iceberg_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	D_ASSERT(table_data);

	if (table_data->data_source_format != "ICEBERG") {
		throw NotImplementedException("Table '%s' is of unsupported format '%s', ", table_data->name,
		                              table_data->data_source_format);
	}

	if (table_data->metadata_location.find("file://") != 0) {
		auto &secret_manager = SecretManager::Get(context);
		// Get Credentials from ICAPI
		auto table_credentials = ICAPI::GetTableCredentials(
			ic_catalog.internal_name, table_data->schema_name, table_data->name, ic_catalog.credentials);
		// Inject secret into secret manager scoped to this path
		CreateSecretInfo info(OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::TEMPORARY);
		info.name = "__internal_ic_" + table_data->table_id;
		info.type = "s3";
		info.provider = "config";
		info.storage_type = "memory";
		info.options = {
		    {"key_id", table_credentials.key_id},
		    {"secret", table_credentials.secret},
		    {"session_token", table_credentials.session_token},
		    {"region", ic_catalog.credentials.aws_region},
		};

		std::string lc_storage_location;
		lc_storage_location.resize(table_data->metadata_location.size());
		std::transform(table_data->metadata_location.begin(), table_data->metadata_location.end(), lc_storage_location.begin(), ::tolower);
		size_t metadata_pos = lc_storage_location.find("metadata");
		if (metadata_pos != std::string::npos) {
    		info.scope = {lc_storage_location.substr(0, metadata_pos)};
		} else {
			throw std::runtime_error("Substring not found");
		}

		auto my_secret = secret_manager.CreateSecret(context, info);
	}

	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	// Set the S3 path as input to table function
	vector<Value> inputs = {table_data->metadata_location};

	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, 
									  iceberg_scan_function, empty_ref);

	auto table_ref = iceberg_scan_function.bind_replace(context, bind_input);

	// 1) Create a Binder and bind the parser-level TableRef -> BoundTableRef
    auto binder = Binder::CreateBinder(context);
    auto bound_ref = binder->Bind(*table_ref);

    // 2) Create a logical plan from the bound reference
    unique_ptr<LogicalOperator> logical_plan = binder->CreatePlan(*bound_ref);

    // 3) Recursively search the logical plan for a LogicalGet node
    //    For a single table function, you often have just one operator: LogicalGet
    LogicalOperator *op = logical_plan.get();
    if (op->type != LogicalOperatorType::LOGICAL_GET) {
        throw std::runtime_error("Expected a LogicalGet, but got something else!");
    }

    // 4) Access the bind_data inside LogicalGet
    auto &get = (LogicalGet &)*op;
	bind_data = std::move(get.bind_data);

	return parquet_scan_function;
}

TableStorageInfo ICTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

} // namespace duckdb
