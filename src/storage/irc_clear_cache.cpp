#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/irc_catalog.hpp"

namespace duckdb {

struct ClearCacheFunctionData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> ClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<ClearCacheFunctionData>();
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void ClearIRCCaches(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = db_ref.get();
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "iceberg") {
			continue;
		}
		catalog.Cast<IRCatalog>().ClearCache();
	}
}

static void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
	if (data.finished) {
		return;
	}
	ClearIRCCaches(context);
	data.finished = true;
}

void ICRClearCacheFunction::ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter) {
	ClearIRCCaches(context);
}

//! FIXME: why is this called 'pc' ??
ICRClearCacheFunction::ICRClearCacheFunction()
    : TableFunction("pc_clear_cache", {}, ClearCacheFunction, ClearCacheBind) {
}
} // namespace duckdb
