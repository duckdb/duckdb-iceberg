#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "rest_catalog/objects/list.hpp"
#include "duckdb/common/json_document.hpp"

namespace duckdb {

struct IcebergLoadTableResponseBindData : public TableFunctionData {
	IcebergTableSchemaVersion &table_entry;
	IcebergCatalog &ic_catalog;
	IcebergSchemaEntry &ic_schema;

	IcebergLoadTableResponseBindData(IcebergTableSchemaVersion &table_entry, IcebergCatalog &ic_catalog,
	                                 IcebergSchemaEntry &ic_schema)
	    : table_entry(table_entry), ic_catalog(ic_catalog), ic_schema(ic_schema) {
	}
};

struct IcebergLoadTableResponseGlobalState : public GlobalTableFunctionState {
	bool done = false;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergLoadTableResponseGlobalState>();
	}
};

static unique_ptr<HTTPResponse> MakeRequest(ClientContext &context, const IcebergLoadTableResponseBindData &bind_data) {
	auto &ic_catalog = bind_data.ic_catalog;
	auto &ic_schema = bind_data.ic_schema;
	auto &ic_table_entry = bind_data.table_entry;

	auto url_builder = ic_catalog.GetBaseUrl();
	url_builder.AddPrefixComponents(ic_catalog.prefix);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(
	    IRCPathComponent::NamespaceComponent(ic_schema.namespace_items, ic_catalog.namespace_separator));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(ic_table_entry.name.GetIdentifierName()));

	HTTPHeaders headers(*context.db);
	if (ic_catalog.attach_options.access_mode == IRCAccessDelegationMode::VENDED_CREDENTIALS) {
		headers.Insert("X-Iceberg-Access-Delegation", "vended-credentials");
	}
	unique_ptr<HTTPResponse> response =
	    ic_catalog.auth_handler->Request(RequestType::GET_REQUEST, context, url_builder, headers);
	if (!response->Success()) {
		throw IOException("GET request to '%s' failed with status %s: %s", url_builder.GetURLEncoded(),
		                  EnumUtil::ToString(response->status), response->body);
	}
	return response;
}

static unique_ptr<FunctionData> IcebergLoadTableResponseBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types,
                                                             vector<Identifier> &names) {
	auto input_string = input.inputs[0].ToString();
	auto qualified_name = QualifiedName::ParseComponents(input_string);

	if (qualified_name.size() != 3) {
		throw InvalidInputException("Expected fully qualified table name (catalog.schema.table), got: %s",
		                            input_string);
	}

	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY,
	                             QualifiedName(qualified_name[0], qualified_name[1], qualified_name[2]));
	auto catalog_entry = Catalog::GetEntry(context, table_lookup, OnEntryNotFound::THROW_EXCEPTION);

	if (catalog_entry->type != CatalogType::TABLE_ENTRY) {
		throw InvalidInputException("'%s' is not a table", input_string);
	}
	auto &table = catalog_entry->Cast<TableCatalogEntry>();
	if (table.catalog.GetCatalogType() != "iceberg") {
		throw InvalidInputException("Table '%s' is not an Iceberg REST catalog table", input_string);
	}

	auto &table_entry = catalog_entry->Cast<IcebergTableSchemaVersion>();
	auto &ic_catalog = table_entry.catalog.Cast<IcebergCatalog>();
	auto &ic_schema = table_entry.schema.Cast<IcebergSchemaEntry>();

	auto ret = make_uniq<IcebergLoadTableResponseBindData>(table_entry, ic_catalog, ic_schema);

	// metadata_location
	names.push_back("metadata_location");
	return_types.push_back(LogicalType::VARCHAR);

	// metadata (JSON)
	names.push_back("metadata");
	return_types.push_back(LogicalType::VARIANT());

	// config
	names.push_back("config");
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));

	// storage_credentials
	names.push_back("storage_credentials");
	auto credential_struct = LogicalType::STRUCT({
	    {"prefix", LogicalType::VARCHAR},
	    {"config", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	});
	return_types.push_back(LogicalType::LIST(credential_struct));

	// request_url
	names.push_back("request_url");
	return_types.push_back(LogicalType::VARCHAR);

	return std::move(ret);
}

//! Write 'config' as row 'row_idx' of MAP vector 'map_vec', appending to the entries already written.
//! Secret values (vended keys, tokens, ...) are redacted so they can't be read back through SQL.
static void OutputMap(const case_insensitive_map_t<string> &config, Vector &map_vec, idx_t row_idx) {
	auto offset = ListVector::GetListSize(map_vec);
	auto count = config.size();
	ListVector::Reserve(map_vec, offset + count);
	auto &key_vec = MapVector::GetKeys(map_vec);
	auto &val_vec = MapVector::GetValues(map_vec);
	idx_t entry_idx = offset;
	for (auto &kv : config) {
		FlatVector::GetDataMutable<string_t>(key_vec)[entry_idx] = StringVector::AddString(key_vec, kv.first);
		FlatVector::GetDataMutable<string_t>(val_vec)[entry_idx] =
		    StringVector::AddString(val_vec, ICUtils::RedactConfigValue(kv.first, kv.second));
		entry_idx++;
	}
	ListVector::SetListSize(map_vec, offset + count);
	auto &list_data = FlatVector::GetDataMutable<list_entry_t>(map_vec)[row_idx];
	list_data.offset = offset;
	list_data.length = count;
}

static void IcebergLoadTableResponseFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergLoadTableResponseBindData>();
	auto &global_state = data.global_state->Cast<IcebergLoadTableResponseGlobalState>();

	if (global_state.done) {
		return;
	}
	global_state.done = true;

	auto response = MakeRequest(context, bind_data);

	// Parse the response as JSON
	auto doc = ICUtils::APIResultToDoc(response->body);
	auto root = doc->GetRoot();

	auto load_result = ICUtils::ParseLoadTableResult(root);

	output.SetChildCardinality(1);

	// metadata_location
	auto &metadata_location_vector = output.data[0];
	if (load_result.metadata_location) {
		FlatVector::GetDataMutable<string_t>(metadata_location_vector)[0] =
		    StringVector::AddString(metadata_location_vector, *load_result.metadata_location);
	} else {
		FlatVector::ValidityMutable(metadata_location_vector).SetInvalid(0);
	}

	// metadata (VARIANT)
	auto &metadata_vector = output.data[1];
	{
		auto metadata_val = root.GetMember("metadata");
		if (metadata_val.IsValid()) {
			Vector json_vec(LogicalType::JSON(), 1);
			FlatVector::GetDataMutable<string_t>(json_vec)[0] =
			    StringVector::AddString(json_vec, metadata_val.ToString());
			VectorOperations::Cast(context, json_vec, metadata_vector, 1);
		}
	}

	// config MAP(VARCHAR, VARCHAR)
	auto &config_vector = output.data[2];
	if (load_result.config) {
		OutputMap(*load_result.config, config_vector, 0);
	} else {
		FlatVector::ValidityMutable(config_vector).SetInvalid(0);
	}

	// storage_credentials LIST(STRUCT(prefix, config))
	auto &storage_credentials_vector = output.data[3];

	if (auto &credentials = load_result.storage_credentials) {
		auto &storage_credentials = *credentials;
		auto cred_count = storage_credentials.size();
		ListVector::Reserve(storage_credentials_vector, cred_count);
		auto &cred_entry = ListVector::GetChildMutable(storage_credentials_vector);
		auto &prefix_vec = StructVector::GetEntries(cred_entry)[0];
		auto &cred_config_vec = StructVector::GetEntries(cred_entry)[1];

		for (idx_t struct_idx = 0; struct_idx < cred_count; struct_idx++) {
			auto &cred = storage_credentials[struct_idx];

			// prefix
			FlatVector::GetDataMutable<string_t>(prefix_vec)[struct_idx] =
			    StringVector::AddString(prefix_vec, cred.prefix);

			// config map for this credential
			OutputMap(cred.config, cred_config_vec, struct_idx);
		}
		ListVector::SetListSize(storage_credentials_vector, cred_count);

		auto &cred_list_data = FlatVector::GetDataMutable<list_entry_t>(storage_credentials_vector)[0];
		cred_list_data.offset = 0;
		cred_list_data.length = cred_count;
	} else {
		FlatVector::ValidityMutable(storage_credentials_vector).SetInvalid(0);
	}

	// request_url
	auto &request_endpoint_vector = output.data[4];
	FlatVector::GetDataMutable<string_t>(request_endpoint_vector)[0] =
	    StringVector::AddString(request_endpoint_vector, response->url);
}

TableFunctionSet IcebergFunctions::GetIcebergLoadTableResponseFunction() {
	TableFunctionSet function_set("iceberg_load_table_response");

	auto fun = TableFunction(FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
	                         IcebergLoadTableResponseFunction, IcebergLoadTableResponseBind,
	                         IcebergLoadTableResponseGlobalState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
