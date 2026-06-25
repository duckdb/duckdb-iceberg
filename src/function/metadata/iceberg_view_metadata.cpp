#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "rest_catalog/objects/list.hpp"
#include "rest_catalog/objects/load_view_result.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

//! Table function `iceberg_view_metadata('catalog.schema.view')` — returns the raw result of a
//! LoadView REST call, mirroring `iceberg_load_table_response` for tables. Useful for discovering
//! views on a REST catalog, including views written in a SQL dialect DuckDB can't parse.
struct IcebergViewMetadataBindData : public TableFunctionData {
	IcebergCatalog &ic_catalog;
	IcebergSchemaEntry &ic_schema;
	string view_name;

	IcebergViewMetadataBindData(IcebergCatalog &ic_catalog, IcebergSchemaEntry &ic_schema, string view_name)
	    : ic_catalog(ic_catalog), ic_schema(ic_schema), view_name(std::move(view_name)) {
	}
};

struct IcebergViewMetadataGlobalState : public GlobalTableFunctionState {
	bool done = false;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergViewMetadataGlobalState>();
	}
};

static unique_ptr<HTTPResponse> MakeRequest(ClientContext &context, const IcebergViewMetadataBindData &bind_data) {
	auto &ic_catalog = bind_data.ic_catalog;
	auto &ic_schema = bind_data.ic_schema;

	auto url_builder = ic_catalog.GetBaseUrl();
	url_builder.AddPrefixComponent(ic_catalog.prefix, ic_catalog.prefix_is_one_component);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(ic_schema.namespace_items));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("views"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(bind_data.view_name));

	HTTPHeaders headers(*context.db);
	unique_ptr<HTTPResponse> response =
	    ic_catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
	if (!response->Success()) {
		throw IOException("GET request to '%s' failed with status %s: %s", url_builder.GetURLEncoded(),
		                  EnumUtil::ToString(response->status), response->body);
	}
	return response;
}

static unique_ptr<FunctionData> IcebergViewMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto input_string = input.inputs[0].ToString();
	auto qualified_name = QualifiedName::ParseComponents(input_string);

	if (qualified_name.size() != 3) {
		throw InvalidInputException("Expected fully qualified view name (catalog.schema.view), got: %s", input_string);
	}

	EntryLookupInfo view_lookup(CatalogType::VIEW_ENTRY, qualified_name[2]);
	auto catalog_entry =
	    Catalog::GetEntry(context, qualified_name[0], qualified_name[1], view_lookup, OnEntryNotFound::THROW_EXCEPTION);

	if (catalog_entry->type != CatalogType::VIEW_ENTRY) {
		throw InvalidInputException("'%s' is not a view", input_string);
	}
	auto &view_entry = catalog_entry->Cast<ViewCatalogEntry>();
	auto &view_catalog = view_entry.ParentCatalog();
	if (view_catalog.GetCatalogType() != "iceberg") {
		throw InvalidInputException("View '%s' is not in an Iceberg REST catalog", input_string);
	}

	auto &ic_catalog = view_catalog.Cast<IcebergCatalog>();
	auto &ic_schema = view_entry.schema.Cast<IcebergSchemaEntry>();

	auto ret = make_uniq<IcebergViewMetadataBindData>(ic_catalog, ic_schema, qualified_name[2]);

	// metadata_location
	names.push_back("metadata_location");
	return_types.push_back(LogicalType::VARCHAR);

	// metadata (JSON -> VARIANT)
	names.push_back("metadata");
	return_types.push_back(LogicalType::VARIANT());

	// config
	names.push_back("config");
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	// request_url
	names.push_back("request_url");
	return_types.push_back(LogicalType::VARCHAR);

	return std::move(ret);
}

static void OutputMap(const case_insensitive_map_t<string> &config, Vector &config_vec) {
	auto config_count = config.size();
	ListVector::Reserve(config_vec, config_count);
	auto &config_key_vec = MapVector::GetKeys(config_vec);
	auto &config_val_vec = MapVector::GetValues(config_vec);
	idx_t config_idx = 0;
	for (auto &kv : config) {
		FlatVector::GetData<string_t>(config_key_vec)[config_idx] = StringVector::AddString(config_key_vec, kv.first);
		FlatVector::GetData<string_t>(config_val_vec)[config_idx] = StringVector::AddString(config_val_vec, kv.second);
		config_idx++;
	}
	ListVector::SetListSize(config_vec, config_count);
	auto &config_list_data = FlatVector::GetData<list_entry_t>(config_vec)[0];
	config_list_data.offset = 0;
	config_list_data.length = config_count;
}

static void IcebergViewMetadataFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergViewMetadataBindData>();
	auto &global_state = data.global_state->Cast<IcebergViewMetadataGlobalState>();

	if (global_state.done) {
		return;
	}
	global_state.done = true;

	auto response = MakeRequest(context, bind_data);

	// Parse the response using yyjson
	auto doc = ICUtils::APIResultToDoc(response->body);
	auto *root = yyjson_doc_get_root(doc.get());

	auto load_result = rest_api_objects::LoadViewResult::FromJSON(root);

	output.SetCardinality(1);

	// metadata_location
	auto &metadata_location_vector = output.data[0];
	FlatVector::GetData<string_t>(metadata_location_vector)[0] =
	    StringVector::AddString(metadata_location_vector, load_result.metadata_location);

	// metadata (VARIANT)
	auto &metadata_vector = output.data[1];
	{
		auto *metadata_val = yyjson_obj_get(root, "metadata");
		if (metadata_val) {
			auto *json_str = yyjson_val_write(metadata_val, 0, nullptr);
			if (json_str) {
				Vector json_vec(LogicalType::JSON(), 1);
				FlatVector::GetData<string_t>(json_vec)[0] = StringVector::AddString(json_vec, string(json_str));
				free(json_str);
				VectorOperations::Cast(context, json_vec, metadata_vector, 1);
			}
		}
	}

	// config MAP(VARCHAR, VARCHAR)
	auto &config_vector = output.data[2];
	OutputMap(load_result.config, config_vector);

	// request_url
	auto &request_endpoint_vector = output.data[3];
	FlatVector::GetData<string_t>(request_endpoint_vector)[0] =
	    StringVector::AddString(request_endpoint_vector, response->url);
}

TableFunctionSet IcebergFunctions::GetIcebergViewMetadataFunction() {
	TableFunctionSet function_set("iceberg_view_metadata");

	auto fun = TableFunction({LogicalType::VARCHAR}, IcebergViewMetadataFunction, IcebergViewMetadataBind,
	                         IcebergViewMetadataGlobalState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
