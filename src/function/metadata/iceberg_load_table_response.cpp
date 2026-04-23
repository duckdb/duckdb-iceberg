#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/api/catalog_utils.hpp"
#include "catalog/rest/api/url_utils.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "rest_catalog/objects/list.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

struct StorageCredentialData {
	string prefix;
	case_insensitive_map_t<string> config;
};

struct IcebergLoadTableResponseBindData : public TableFunctionData {
	string metadata_location;
	string metadata_json;
	case_insensitive_map_t<string> config;
	vector<StorageCredentialData> storage_credentials;
	string request_url;
};

struct IcebergLoadTableResponseGlobalState : public GlobalTableFunctionState {
	bool done = false;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergLoadTableResponseGlobalState>();
	}
};

static unique_ptr<HTTPResponse> MakeRequest(ClientContext &context, TableFunctionBindInput &input) {
	auto input_string = input.inputs[0].ToString();
	auto qualified_name = QualifiedName::ParseComponents(input_string);

	if (qualified_name.size() != 3) {
		throw InvalidInputException("Expected fully qualified table name (catalog.schema.table), got: %s",
		                            input_string);
	}

	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, qualified_name[2]);
	auto catalog_entry = Catalog::GetEntry(context, qualified_name[0], qualified_name[1], table_lookup,
	                                       OnEntryNotFound::THROW_EXCEPTION);

	if (catalog_entry->type != CatalogType::TABLE_ENTRY) {
		throw InvalidInputException("'%s' is not a table", input_string);
	}
	auto &table = catalog_entry->Cast<TableCatalogEntry>();
	if (table.catalog.GetCatalogType() != "iceberg") {
		throw InvalidInputException("Table '%s' is not an Iceberg REST catalog table", input_string);
	}

	auto &table_entry = catalog_entry->Cast<IcebergTableEntry>();
	auto &ic_catalog = table_entry.catalog.Cast<IcebergCatalog>();
	auto &ic_schema = table_entry.schema.Cast<IcebergSchemaEntry>();

	// Make the GET request
	auto url_builder = ic_catalog.GetBaseUrl();
	url_builder.AddPrefixComponent(ic_catalog.prefix, ic_catalog.prefix_is_one_component);
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("namespaces"));
	url_builder.AddPathComponent(IRCPathComponent::NamespaceComponent(ic_schema.namespace_items));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent("tables"));
	url_builder.AddPathComponent(IRCPathComponent::RegularComponent(table_entry.name));

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
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto ret = make_uniq<IcebergLoadTableResponseBindData>();

	const unique_ptr<HTTPResponse> response = MakeRequest(context, input);

	// Parse the response using yyjson
	auto doc = ICUtils::APIResultToDoc(response->body);
	auto *root = yyjson_doc_get_root(doc.get());

	// Parse via LoadTableResult for the typed fields
	auto load_result = rest_api_objects::LoadTableResult::FromJSON(root);

	if (load_result.has_metadata_location) {
		ret->metadata_location = load_result.metadata_location;
	}

	// Re-serialize the metadata sub-object as JSON
	auto *metadata_val = yyjson_obj_get(root, "metadata");
	if (metadata_val) {
		auto *json_str = yyjson_val_write(metadata_val, 0, nullptr);
		if (json_str) {
			ret->metadata_json = json_str;
			free(json_str);
		}
	}

	if (load_result.has_config) {
		ret->config = std::move(load_result.config);
	}

	if (load_result.has_storage_credentials) {
		for (auto &cred : load_result.storage_credentials) {
			StorageCredentialData data;
			data.prefix = cred.prefix;
			data.config = cred.config;
			ret->storage_credentials.push_back(std::move(data));
		}
	}

	ret->request_url = response->url;

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

static void OutputMap(const case_insensitive_map_t<string> &config, Vector &config_vec) {
	auto config_count = config.size();
	ListVector::Reserve(config_vec, config_count);
	auto &config_key_vec = MapVector::GetKeys(config_vec);
	auto &config_val_vec = MapVector::GetValues(config_vec);
	idx_t i = 0;
	for (auto &kv : config) {
		FlatVector::GetData<string_t>(config_key_vec)[i] = StringVector::AddString(config_key_vec, kv.first);
		FlatVector::GetData<string_t>(config_val_vec)[i] = StringVector::AddString(config_val_vec, kv.second);
		i++;
	}
	ListVector::SetListSize(config_vec, config_count);
	auto &config_list_data = FlatVector::GetData<list_entry_t>(config_vec)[0];
	config_list_data.offset = 0;
	config_list_data.length = config_count;
}

static void IcebergLoadTableResponseFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	// contains the actual data
	auto &bind_data = data.bind_data->Cast<IcebergLoadTableResponseBindData>();
	auto &global_state = data.global_state->Cast<IcebergLoadTableResponseGlobalState>();

	// We do this to signal let the engine know we have only this row to produce
	if (global_state.done) {
		return;
	}
	global_state.done = true;
	output.SetCardinality(1);

	// metadata_location
	auto &metadata_location_vector = output.data[0];
	FlatVector::GetData<string_t>(metadata_location_vector)[0] =
	    StringVector::AddString(metadata_location_vector, bind_data.metadata_location);

	// metadata (VARIANT)
	auto &metadata_vector = output.data[1];
	{
		Vector json_vec(LogicalType::JSON(), 1);
		FlatVector::GetData<string_t>(json_vec)[0] = StringVector::AddString(json_vec, bind_data.metadata_json);
		VectorOperations::Cast(context, json_vec, metadata_vector, 1);
	}

	// config MAP(VARCHAR, VARCHAR)
	auto &config_vector = output.data[2];
	OutputMap(bind_data.config, config_vector);

	// storage_credentials LIST(STRUCT(prefix, config))
	auto &storage_credentials_vector = output.data[3];
	auto cred_count = bind_data.storage_credentials.size();
	ListVector::Reserve(storage_credentials_vector, cred_count);
	auto &cred_entry = ListVector::GetEntry(storage_credentials_vector);
	auto &prefix_vec = StructVector::GetEntries(cred_entry)[0];
	auto &cred_config_vec = StructVector::GetEntries(cred_entry)[1];

	for (idx_t i = 0; i < cred_count; i++) {
		auto &cred = bind_data.storage_credentials[i];
		idx_t struct_idx = i;

		// prefix
		FlatVector::GetData<string_t>(*prefix_vec)[struct_idx] = StringVector::AddString(*prefix_vec, cred.prefix);

		// config map for this credential
		OutputMap(cred.config, *cred_config_vec);

		auto inner_config_count = cred.config.size();
		ListVector::Reserve(*cred_config_vec, inner_config_count);
		auto &inner_key_vec = MapVector::GetKeys(*cred_config_vec);
		auto &inner_val_vec = MapVector::GetValues(*cred_config_vec);
		idx_t j = 0;
		for (auto &kv : cred.config) {
			FlatVector::GetData<string_t>(inner_key_vec)[j] = StringVector::AddString(inner_key_vec, kv.first);
			FlatVector::GetData<string_t>(inner_val_vec)[j] = StringVector::AddString(inner_val_vec, kv.second);
			j++;
		}
		ListVector::SetListSize(*cred_config_vec, inner_config_count);
		auto &inner_list_data = FlatVector::GetData<list_entry_t>(*cred_config_vec)[struct_idx];
		inner_list_data.offset = 0;
		inner_list_data.length = inner_config_count;
	}
	ListVector::SetListSize(storage_credentials_vector, cred_count);
	auto &cred_list_data = FlatVector::GetData<list_entry_t>(storage_credentials_vector)[0];
	cred_list_data.offset = 0;
	cred_list_data.length = cred_count;

	// request_url
	auto &request_endpoint_vector = output.data[4];
	FlatVector::GetData<string_t>(request_endpoint_vector)[0] =
	    StringVector::AddString(metadata_location_vector, bind_data.request_url);
}

TableFunctionSet IcebergFunctions::GetIcebergLoadTableResponseFunction() {
	TableFunctionSet function_set("iceberg_load_table_response");

	auto fun = TableFunction({LogicalType::VARCHAR}, IcebergLoadTableResponseFunction, IcebergLoadTableResponseBind,
	                         IcebergLoadTableResponseGlobalState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
