#include "iceberg_attach.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

struct IcebergOption {
	const char *name;
	const char *catalog_name;
	bool authorization = false;
	bool from_catalog = true;
	IcebergAuthorizationType authorization_type = IcebergAuthorizationType::INVALID;
};

//! The supported catalog properties and their DuckDB equivalents. Properties with no
//! separate Iceberg spelling use the DuckDB name in both ATTACH and /config.
static const IcebergOption OPTIONS[] = {
    {"uri", "uri"},
    {"warehouse", "warehouse"},
    {"prefix", "prefix"},
    {"namespace_separator", "namespace-separator"},
    {"authorization_type", "rest.auth.type", true},
    {"oauth2_server_uri", "oauth2-server-uri", true, true, IcebergAuthorizationType::OAUTH2},
    {"oauth2_scope", "scope", true, true, IcebergAuthorizationType::OAUTH2},
    {"oauth2_grant_type", "oauth2_grant_type", true, true, IcebergAuthorizationType::OAUTH2},
    {"client_id", "client_id", true, true, IcebergAuthorizationType::OAUTH2},
    {"client_secret", "client_secret", true, true, IcebergAuthorizationType::OAUTH2},
    {"token", "token", true, true, IcebergAuthorizationType::OAUTH2},
    {"refresh_token", "refresh_token", true, true, IcebergAuthorizationType::OAUTH2},
    {"expires_in", "expires_in", true, true, IcebergAuthorizationType::OAUTH2},
    {"sigv4_region", "rest.signing-region", true, true, IcebergAuthorizationType::SIGV4},
    {"sigv4_service", "rest.signing-name", true, true, IcebergAuthorizationType::SIGV4},
    {"extra_http_headers", "extra_http_headers", true},
    {"default_region", "client.region"},
    {"access_delegation_mode", "access_delegation_mode"},
    {"stage_create_tables", "stage_create_tables"},
    {"disable_multi_table_commit", "disable_multi_table_commit"},
    {"skip_create_table_metadata_updates", "skip_create_table_metadata_updates"},
    {"remove_files_on_delete", "s3.delete-enabled"},
    {"support_nested_namespaces", "support_nested_namespaces"},
    {"purge_requested", "purge_requested"},
    {"default_table_location_from_namespace", "default_table_location_from_namespace"},
    {"use_metadata_location", "use_metadata_location"},
    {"table_resolution", "table_resolution"},
    {"default_schema", "default_schema"},
    {"encode_entire_prefix", "encode_entire_prefix"},
    {"max_table_staleness", "max_table_staleness"},
    // Local bootstrap controls are never accepted from a remote catalog.
    {"endpoint_type", "endpoint_type", false, false},
    {"secret", "secret", true, false},
};

static const case_insensitive_map_t<const IcebergOption *> &OptionNames() {
	static const auto names = [] {
		case_insensitive_map_t<const IcebergOption *> result;
		for (auto &option : OPTIONS) {
			result.emplace(option.name, &option);
			result.emplace(option.catalog_name, &option);
		}
		result.emplace("endpoint", result.at("uri"));
		return result;
	}();
	return names;
}

static void InsertOption(unordered_map<string, Value> &result, const string &name, const Value &value) {
	if (result.emplace(name, value).second) {
		return;
	}
	if (name == "uri") {
		throw InvalidConfigurationException(
		    "Both 'uri' and deprecated 'endpoint' were provided for Iceberg attach; use only 'uri'");
	}
	throw InvalidConfigurationException("Duplicate Iceberg option '%s' (possibly supplied through an alias)", name);
}

static Value HeadersToValue(const case_insensitive_map_t<Value> &headers) {
	vector<Value> keys;
	vector<Value> values;
	for (auto &header : headers) {
		keys.emplace_back(StringUtil::Lower(header.first));
		values.push_back(header.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

static case_insensitive_map_t<Value> ReadHeaders(const Value &value) {
	case_insensitive_map_t<Value> result;
	auto headers = value.DefaultCastAs(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	for (auto &entry : MapValue::GetChildren(headers)) {
		auto &pair = StructValue::GetChildren(entry);
		result[pair[0].GetValue<string>()] = pair[1];
	}
	return result;
}

static unordered_map<string, Value> NormalizeOptions(const unordered_map<string, Value> &options, bool from_catalog) {
	unordered_map<string, Value> result;
	case_insensitive_map_t<Value> headers;
	auto &names = OptionNames();
	for (auto &entry : options) {
		auto name = StringUtil::Lower(entry.first);
		if (StringUtil::StartsWith(name, "header.") && name.size() > 7) {
			if (!headers.emplace(name.substr(7), entry.second).second) {
				throw InvalidConfigurationException("Duplicate Iceberg header option '%s'", name);
			}
			continue;
		}
		if (name == "credential") {
			auto credential = entry.second.GetValue<string>();
			auto separator = credential.find(':');
			// Iceberg permits a secret without a client ID.
			InsertOption(result, "client_id", separator == string::npos ? "" : credential.substr(0, separator));
			InsertOption(result, "client_secret",
			             separator == string::npos ? credential : credential.substr(separator + 1));
			continue;
		}
		if (name == "rest.sigv4-enabled") {
			auto enabled = entry.second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
			InsertOption(result, "authorization_type", Value(enabled ? "sigv4" : "oauth2"));
			continue;
		}
		auto it = names.find(name);
		if (it == names.end()) {
			if (!from_catalog) {
				InsertOption(result, name, entry.second);
			}
			continue;
		}
		if (from_catalog && !it->second->from_catalog) {
			continue;
		}
		InsertOption(result, it->second->name, entry.second);
	}
	if (!headers.empty()) {
		auto it = result.find("extra_http_headers");
		if (it != result.end()) {
			for (auto &entry : ReadHeaders(it->second)) {
				if (!headers.emplace(entry).second) {
					throw InvalidConfigurationException("Duplicate Iceberg header option '%s'", entry.first);
				}
			}
		}
		result["extra_http_headers"] = HeadersToValue(headers);
	}
	return result;
}

} // namespace

unordered_map<string, Value> NormalizeIcebergAttachOptions(const unordered_map<string, Value> &options) {
	return NormalizeOptions(options, false);
}

unordered_map<string, Value> NormalizeIcebergCatalogOptions(const case_insensitive_map_t<string> &properties) {
	unordered_map<string, Value> options;
	for (auto &entry : properties) {
		options.emplace(entry.first, Value(entry.second));
	}
	return NormalizeOptions(options, true);
}

void MergeIcebergOptions(unordered_map<string, Value> &target, const unordered_map<string, Value> &source) {
	for (auto &entry : source) {
		auto it = target.find(entry.first);
		if (entry.first == "extra_http_headers" && it != target.end()) {
			auto headers = ReadHeaders(it->second);
			for (auto &header : ReadHeaders(entry.second)) {
				headers[header.first] = header.second;
			}
			it->second = HeadersToValue(headers);
		} else {
			target[entry.first] = entry.second;
		}
	}
}

bool IsIcebergAuthorizationOption(const string &name) {
	auto &names = OptionNames();
	auto it = names.find(name);
	return it != names.end() && it->second->authorization;
}

void FilterIcebergAuthorizationOptions(unordered_map<string, Value> &options, IcebergAuthorizationType type) {
	auto &names = OptionNames();
	for (auto it = options.begin(); it != options.end();) {
		auto option = names.find(it->first);
		if (option != names.end() && ((option->second->authorization_type != IcebergAuthorizationType::INVALID &&
		                               option->second->authorization_type != type) ||
		                              (it->first == "secret" && type == IcebergAuthorizationType::NONE))) {
			it = options.erase(it);
		} else {
			++it;
		}
	}
}

} // namespace duckdb
