#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "storage/ic_catalog.hpp"
#include "yyjson.hpp"

#include <curl/curl.h>
#include <sys/stat.h>
#include <iostream>
#include <optional>

using namespace duckdb_yyjson;
namespace duckdb {

//! We use a global here to store the path that is selected on the ICAPI::InitializeCurl call
static string SELECTED_CURL_CERT_PATH = "";

static size_t RequestWriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	((std::string *)userp)->append((char *)contents, size * nmemb);
	return size * nmemb;
}

// we statically compile in libcurl, which means the cert file location of the build machine is the
// place curl will look. But not every distro has this file in the same location, so we search a
// number of common locations and use the first one we find.
static string certFileLocations[] = {
        // Arch, Debian-based, Gentoo
        "/etc/ssl/certs/ca-certificates.crt",
        // RedHat 7 based
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        // Redhat 6 based
        "/etc/pki/tls/certs/ca-bundle.crt",
        // OpenSUSE
        "/etc/ssl/ca-bundle.pem",
        // Alpine
        "/etc/ssl/cert.pem"
};

struct YyjsonDocDeleter {
    void operator()(yyjson_doc* doc) {
        yyjson_doc_free(doc);
    }
    void operator()(yyjson_mut_doc* doc) {
        yyjson_mut_doc_free(doc);
    }
};

// Look through the the above locations and if one of the files exists, set that as the location curl should use.
static bool SelectCurlCertPath() {
	for (string& caFile : certFileLocations) {
		struct stat buf;
		if (stat(caFile.c_str(), &buf) == 0) {
			SELECTED_CURL_CERT_PATH = caFile;
		}
	}
	return false;
}

static bool SetCurlCAFileInfo(CURL* curl) {
	if (!SELECTED_CURL_CERT_PATH.empty()) {
		curl_easy_setopt(curl, CURLOPT_CAINFO, SELECTED_CURL_CERT_PATH.c_str());
        return true;
	}
    return false;
}

// Note: every curl object we use should set this, because without it some linux distro's may not find the CA certificate.
static void InitializeCurlObject(CURL * curl, const string &token) {
  	if (!token.empty()) {
		curl_easy_setopt(curl, CURLOPT_XOAUTH2_BEARER, token.c_str());
		curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
	}
    SetCurlCAFileInfo(curl);
}

template <class TYPE, uint8_t TYPE_NUM, TYPE (*get_function)(yyjson_val *obj)>
static TYPE TemplatedTryGetYYJson(yyjson_val *obj, const string &field, TYPE default_val,
                                  bool fail_on_missing = true) {
	auto val = yyjson_obj_get(obj, field.c_str());
	if (val && yyjson_get_type(val) == TYPE_NUM) {
		return get_function(val);
	} else if (!fail_on_missing) {
		return default_val;
	}
	throw IOException("Invalid field found while parsing field: " + field);
}

static uint64_t TryGetNumFromObject(yyjson_val *obj, const string &field, bool fail_on_missing = true,
                                    uint64_t default_val = 0) {
	return TemplatedTryGetYYJson<uint64_t, YYJSON_TYPE_NUM, yyjson_get_uint>(obj, field, default_val,
	                                                                                        fail_on_missing);
}
static bool TryGetBoolFromObject(yyjson_val *obj, const string &field, bool fail_on_missing = false,
                                 bool default_val = false) {
	return TemplatedTryGetYYJson<bool, YYJSON_TYPE_BOOL, yyjson_get_bool>(obj, field, default_val,
	                                                                                     fail_on_missing);
}
static string TryGetStrFromObject(yyjson_val *obj, const string &field, bool fail_on_missing = true,
                                  const char *default_val = "") {
	return TemplatedTryGetYYJson<const char *, YYJSON_TYPE_STR, yyjson_get_str>(obj, field, default_val,
	                                                                                           fail_on_missing);
}

static string DeleteRequest(const string &url, const string &token = "", curl_slist *extra_headers = NULL) {
    CURL *curl;
    CURLcode res;
    string readBuffer;

    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        
        if(extra_headers) {
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, extra_headers);
        }
        
        InitializeCurlObject(curl, token);
        res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if (res != CURLcode::CURLE_OK) {
            string error = curl_easy_strerror(res);
            throw IOException("Curl DELETE Request to '%s' failed with error: '%s'", url, error);
        }
        
        return readBuffer;
    }
    throw InternalException("Failed to initialize curl");
}

static string GetRequest(const string &url, const string &token = "", curl_slist *extra_headers = NULL) {
	CURL *curl;
	CURLcode res;
	string readBuffer;

	curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
		
		if(extra_headers) {
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, extra_headers);
		}
		
		InitializeCurlObject(curl, token);
		res = curl_easy_perform(curl);
		curl_easy_cleanup(curl);

		if (res != CURLcode::CURLE_OK) {
			string error = curl_easy_strerror(res);
			throw IOException("Curl Request to '%s' failed with error: '%s'", url, error);
		}

		return readBuffer;
	}
	throw InternalException("Failed to initialize curl");
}

static string PostRequest(
		const string &url, 
		const string &post_data, 
		const string &content_type = "x-www-form-urlencoded",
		const string &token = "", 
		curl_slist *extra_headers = NULL) {
    string readBuffer;
    CURL *curl = curl_easy_init();
    if (!curl) {
		throw InternalException("Failed to initialize curl");
	}

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_POST, 1L);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, RequestWriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

	// Create default headers for content type
	struct curl_slist *headers = NULL;
	const string content_type_str = "Content-Type: application/" + content_type;
	headers = curl_slist_append(headers, content_type_str.c_str());
	
	// Append any extra headers
	if (extra_headers) {
		struct curl_slist *temp = extra_headers;
		while (temp) {
			headers = curl_slist_append(headers, temp->data);
			temp = temp->next;
		}
	}
	
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	InitializeCurlObject(curl, token);
	
	// Perform the request
	CURLcode res = curl_easy_perform(curl);
	
	// Clean up
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLcode::CURLE_OK) {
		string error = curl_easy_strerror(res);
		throw IOException("Curl Request to '%s' failed with error: '%s'", url, error);
	}
	return readBuffer;
}

static yyjson_doc *api_result_to_doc(const string &api_result) {
	auto *doc = yyjson_read(api_result.c_str(), api_result.size(), 0);
	auto *root = yyjson_doc_get_root(doc);
	auto *error = yyjson_obj_get(root, "error");
	if (error != NULL) {
		string err_msg = TryGetStrFromObject(error, "message");
		throw std::runtime_error(err_msg);
	}
	return doc;
}

static string GetTableMetadata(const string &internal, const string &schema, const string &table, ICCredentials credentials) {
	struct curl_slist *extra_headers = NULL;
	extra_headers = curl_slist_append(extra_headers, "X-Iceberg-Access-Delegation: vended-credentials");
	string api_result = GetRequest(
		credentials.endpoint + "/v1/" + internal + "/namespaces/" + schema + "/tables/" + table, 
		credentials.token,
		extra_headers);
	curl_slist_free_all(extra_headers);
	return api_result;
}

void ICAPI::InitializeCurl() {
	SelectCurlCertPath();
}

vector<string> ICAPI::GetCatalogs(const string &catalog, ICCredentials credentials) {
	throw NotImplementedException("ICAPI::GetCatalogs");
}

static ICAPIColumnDefinition ParseColumnDefinition(yyjson_val *column_def) {
	ICAPIColumnDefinition result;
	result.name = TryGetStrFromObject(column_def, "name");
	result.type_text = TryGetStrFromObject(column_def, "type");
	result.precision = (result.type_text == "decimal") ? TryGetNumFromObject(column_def, "type_precision") : -1;
	result.scale = (result.type_text == "decimal") ? TryGetNumFromObject(column_def, "type_scale") : -1;
	result.position = TryGetNumFromObject(column_def, "id") - 1;
	return result;
}

ICAPITableCredentials ICAPI::GetTableCredentials(const string &internal, const string &schema, const string &table, ICCredentials credentials) {
	ICAPITableCredentials result;
	string api_result = GetTableMetadata(internal, schema, table, credentials);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *aws_temp_credentials = yyjson_obj_get(root, "config");
	if (aws_temp_credentials) {
		result.key_id = TryGetStrFromObject(aws_temp_credentials, "s3.access-key-id");
		result.secret = TryGetStrFromObject(aws_temp_credentials, "s3.secret-access-key");
		result.session_token = TryGetStrFromObject(aws_temp_credentials, "s3.session-token");
		return result;
	}

	throw std::runtime_error("No AWS credentials found for table");
}

string ICAPI::GetToken(string id, string secret, string endpoint) {
	string post_data = "grant_type=client_credentials&client_id=" + id + "&client_secret=" + secret + "&scope=PRINCIPAL_ROLE:ALL";
	string api_result = PostRequest(endpoint + "/v1/oauth/tokens", post_data);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	return TryGetStrFromObject(root, "access_token");
}

static void populateTableMetadata(ICAPITable &table, yyjson_val *metadata_root) {
	table.storage_location = TryGetStrFromObject(metadata_root, "metadata-location");
	auto *metadata = yyjson_obj_get(metadata_root, "metadata");
	//table_result.table_id = TryGetStrFromObject(metadata, "table-uuid");

	uint64_t current_schema_id = TryGetNumFromObject(metadata, "current-schema-id");
	auto *schemas = yyjson_obj_get(metadata, "schemas");
	yyjson_val *schema;
	size_t schema_idx, schema_max;
	bool found = false;
	yyjson_arr_foreach(schemas, schema_idx, schema_max, schema) {
		uint64_t schema_id = TryGetNumFromObject(schema, "schema-id");
		if (schema_id == current_schema_id) {
			found = true;
			auto *columns = yyjson_obj_get(schema, "fields");
			yyjson_val *col;
			size_t col_idx, col_max;
			yyjson_arr_foreach(columns, col_idx, col_max, col) {
				auto column_definition = ParseColumnDefinition(col);
				table.columns.push_back(column_definition);
			}
		}
	}

	if (!found) {
		throw InternalException("Current schema not found");
	}
}

static ICAPITable createTable(const string &catalog, const string &schema, const string &table_name) { 
	ICAPITable table_result;
	table_result.catalog_name = catalog;
	table_result.schema_name = schema;
	table_result.name = table_name;
	table_result.data_source_format = "ICEBERG";
	table_result.table_id = "uuid-" + schema + "-" + "table";
	std::replace(table_result.table_id.begin(), table_result.table_id.end(), '_', '-');
	return table_result;
}

ICAPITable ICAPI::GetTable(
	const string &catalog, const string &internal, const string &schema, const string &table_name, std::optional<ICCredentials> credentials) { 
	
	ICAPITable table_result = createTable(catalog, schema, table_name);
	if (credentials) {
		string result = GetTableMetadata(internal, schema, table_result.name, *credentials);
		std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(result));
		auto *metadata_root = yyjson_doc_get_root(doc.get());
		populateTableMetadata(table_result, metadata_root);
	} else {
		// Skip fetching metadata, we'll do it later when we access the table
		ICAPIColumnDefinition col;
		col.name = "__";
		col.type_text = "int";
		col.precision = -1;
		col.scale = -1;
		col.position = 0;
		table_result.columns.push_back(col);
	}

	return table_result;
}

// TODO: handle out-of-order columns using position property
vector<ICAPITable> ICAPI::GetTables(const string &catalog, const string &internal, const string &schema, ICCredentials credentials) {
	vector<ICAPITable> result;
	string api_result = GetRequest(credentials.endpoint + "/v1/" + internal + "/namespaces/" + schema + "/tables", credentials.token);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *tables = yyjson_obj_get(root, "identifiers");
	size_t idx, max;
	yyjson_val *table;
	yyjson_arr_foreach(tables, idx, max, table) {
		auto table_result = GetTable(catalog, internal, schema, TryGetStrFromObject(table, "name"), std::nullopt);
		result.push_back(table_result);
	}

	return result;
}

vector<ICAPISchema> ICAPI::GetSchemas(const string &catalog, const string &internal, ICCredentials credentials) {
	vector<ICAPISchema> result;
	string api_result =
	    GetRequest(credentials.endpoint + "/v1/" + internal + "/namespaces", credentials.token);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *schemas = yyjson_obj_get(root, "namespaces");
	size_t idx, max;
	yyjson_val *schema;
	yyjson_arr_foreach(schemas, idx, max, schema) {
		ICAPISchema schema_result;
		schema_result.catalog_name = catalog;
		yyjson_val *value = yyjson_arr_get(schema, 0);
		schema_result.schema_name = yyjson_get_str(value);
		result.push_back(schema_result);
	}

	return result;
}

ICAPISchema ICAPI::CreateSchema(const string &catalog, const string &internal, const string &schema, ICCredentials credentials) {
	string post_data = "{\"namespace\":[\"" + schema + "\"]}";
	string api_result = PostRequest(
		credentials.endpoint + "/v1/" + internal + "/namespaces", post_data, "json", credentials.token);
	api_result_to_doc(api_result);	// if the method returns, request was successful
	
	ICAPISchema schema_result;
	schema_result.catalog_name = catalog;
	schema_result.schema_name = schema; //yyjson_get_str(value);
	return schema_result;
}

void ICAPI::DropSchema(const string &internal, const string &schema, ICCredentials credentials) {
	string api_result = DeleteRequest(
		credentials.endpoint + "/v1/" + internal + "/namespaces/" + schema, credentials.token);
	api_result_to_doc(api_result);	// if the method returns, request was successful
}

void ICAPI::DropTable(const string &catalog, const string &internal, const string &schema, string &table_name, ICCredentials credentials) {
	string api_result = DeleteRequest(
		credentials.endpoint + "/v1/" + internal + "/namespaces/" + schema + "/tables/" + table_name + "?purgeRequested=true", 
		credentials.token);
	api_result_to_doc(api_result);	// if the method returns, request was successful
}

static std::string json_to_string(yyjson_mut_doc *doc) {
    size_t len = 0;
	yyjson_write_flag flags = YYJSON_WRITE_NOFLAG;
    char *json_chars = yyjson_mut_write(doc, flags, &len);
    std::string json_str(json_chars, len);
    free(json_chars);
    return json_str;
}

ICAPITable ICAPI::CreateTable(const string &catalog, const string &internal, const string &schema, ICCredentials &credentials, CreateTableInfo *table_info) {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> dd(yyjson_mut_doc_new(NULL));
	yyjson_mut_doc *doc = dd.get();

	yyjson_mut_val *rr = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, rr);
	yyjson_mut_obj_add_str(doc, rr, "name", table_info->table.c_str());

	yyjson_mut_val *sch = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, rr, "schema", sch);
	yyjson_mut_obj_add_str(doc, sch, "type", "struct");

	yyjson_mut_val *fields = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, sch, "fields", fields);

	std::vector<std::string> column_names;
	std::vector<std::string> column_types;
	size_t num_columns = table_info->columns.LogicalColumnCount();
	column_names.reserve(num_columns);
	column_types.reserve(num_columns);
	for (auto &col : table_info->columns.Logical()) {
		// Store column name and type in vectors
		column_names.push_back(col.GetName());
		column_types.push_back(ICUtils::LogicalToIcebergType(col.GetType()));
		// Add column object to JSON
		yyjson_mut_val *col_obj = yyjson_mut_obj(doc);
		yyjson_mut_obj_add_int(doc, col_obj, "id", col.Oid());
		yyjson_mut_obj_add_bool(doc, col_obj, "required", false);
		yyjson_mut_obj_add_str(doc, col_obj, "name", column_names.back().c_str());
		yyjson_mut_obj_add_str(doc, col_obj, "type", column_types.back().c_str());
		yyjson_mut_arr_add_val(fields, col_obj);
	}

	yyjson_mut_val *props = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, rr, "properties", props);
	yyjson_mut_obj_add_str(doc, props, "write.parquet.compression-codec", "snappy");
	string post_data = json_to_string(doc);

	struct curl_slist *extra_headers = NULL;
	extra_headers = curl_slist_append(extra_headers, "X-Iceberg-Access-Delegation: vended-credentials");
	string api_result = PostRequest(
		credentials.endpoint + "/v1/" + internal + "/namespaces/" + schema + "/tables", post_data, "json", credentials.token, extra_headers);
	curl_slist_free_all(extra_headers);

	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> dd2(api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(dd2.get());	
	ICAPITable table_result = createTable(catalog, schema, table_info->table);
	populateTableMetadata(table_result, root);
	return table_result;
}

} // namespace duckdb
