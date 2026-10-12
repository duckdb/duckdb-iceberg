#include "iceberg_test_table_loader_extension.hpp"

#include "duckdb_static_extension.h"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"

#include "catalog/rest/iceberg_table_loader.hpp"

namespace duckdb {

static constexpr const char *METADATA_LOCATION_SETTING = "iceberg_test_table_loader_metadata_location";
static constexpr const char *RETURN_VIEW_SETTING = "iceberg_test_table_loader_return_view";

//! Records every request, and builds the table from the metadata file the setting names; NULL means no such table.
//! Returns the table, or a view of 'SELECT 1 AS i' if the return view setting is true.
class TestGovernedIcebergTableLoader : public GovernedIcebergTableLoader {
public:
	optional_ptr<CatalogEntry> Load(ClientContext &context, const IcebergLoadGovernedTableRequest &request,
	                                const build_entry_t &build_entry) override {
		{
			lock_guard<mutex> guard(lock);
			requests.push_back(request);
		}
		Value metadata_location;
		context.TryGetCurrentSetting(METADATA_LOCATION_SETTING, metadata_location);
		if (metadata_location.IsNull()) {
			return nullptr;
		}
		auto &table = build_entry(metadata_location.GetValue<string>());
		Value return_view;
		context.TryGetCurrentSetting(RETURN_VIEW_SETTING, return_view);
		if (!BooleanValue::Get(return_view)) {
			return &table;
		}
		CreateViewInfo info(table.ParentSchema(), Identifier(request.table_name));
		auto parser = Parser::GetBuiltinParser();
		info.query = CreateViewInfo::ParseSelect(parser, "SELECT 1 AS i");
		lock_guard<mutex> guard(lock);
		views.push_back(make_uniq<ViewCatalogEntry>(table.ParentCatalog(), table.ParentSchema(), info));
		return views.back().get();
	}

	vector<IcebergLoadGovernedTableRequest> GetRequests() {
		lock_guard<mutex> guard(lock);
		return requests;
	}

private:
	mutex lock;
	vector<IcebergLoadGovernedTableRequest> requests;
	vector<unique_ptr<ViewCatalogEntry>> views;
};

struct RequestsData : public TableFunctionData {
	vector<IcebergLoadGovernedTableRequest> requests;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> RequestsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RequestsData>();
	for (auto &callback : ExtensionCallback::Iterate(context)) {
		auto loader = dynamic_cast<TestGovernedIcebergTableLoader *>(callback.get());
		if (loader) {
			result->requests = loader->GetRequests();
			break;
		}
	}
	for (auto name :
	     {"catalog_name", "catalog_id", "database_name", "table_name", "region", "key_id", "secret", "session_token"}) {
		names.emplace_back(name);
		return_types.push_back(LogicalType::VARCHAR);
	}
	return std::move(result);
}

static void RequestsFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<RequestsData>();
	idx_t count = 0;
	for (; data.offset < data.requests.size() && count < STANDARD_VECTOR_SIZE; data.offset++, count++) {
		auto &request = data.requests[data.offset];
		idx_t col = 0;
		for (auto &field : {request.catalog_name, request.catalog_id, request.database_name, request.table_name,
		                    request.region, request.key_id, request.secret, request.session_token}) {
			output.data[col++].SetValue(count, Value(field));
		}
	}
	output.SetChildCardinality(count);
}

static void RegisterLoader(ClientContext &context, const FunctionParameters &parameters) {
	ExtensionCallback::Register(DBConfig::GetConfig(context), make_shared_ptr<TestGovernedIcebergTableLoader>());
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(METADATA_LOCATION_SETTING, "The metadata file the test table loader builds tables from",
	                          LogicalType::VARCHAR, Value(LogicalType::VARCHAR));
	config.AddExtensionOption(RETURN_VIEW_SETTING, "Whether the test table loader returns a view instead of the table",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	ExtensionCallback::Register(config, make_shared_ptr<TestGovernedIcebergTableLoader>());
	// The requests the first test table loader received, in order
	loader.RegisterFunction(TableFunction("iceberg_test_table_loader_requests", {}, RequestsFunction, RequestsBind));
	// Registers one more test table loader
	loader.RegisterFunction(PragmaFunction::PragmaCall("iceberg_test_table_loader_register", RegisterLoader, {}));
}

void IcebergTestTableLoaderExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

string IcebergTestTableLoaderExtension::Name() {
	return "iceberg_test_table_loader";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(iceberg_test_table_loader, loader) {
	duckdb::LoadInternal(loader);
}

static int32_t iceberg_test_table_loader_describe(duckdb_extension_descriptor *descriptor) {
	descriptor->version = DUCKDB_EXTENSION_DESCRIPTOR_VERSION;
	descriptor->name = "iceberg_test_table_loader";
	descriptor->entry_cpp = reinterpret_cast<void (*)(void)>(iceberg_test_table_loader_duckdb_cpp_init);
	return 0;
}
}

// Linked into unittest only, which no generated loader registers it in: register before main
static const int32_t iceberg_test_table_loader_registered =
    duckdb_register_static_extension(iceberg_test_table_loader_describe);
