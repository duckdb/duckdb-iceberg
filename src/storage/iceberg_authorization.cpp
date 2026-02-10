
#include "storage/iceberg_authorization.hpp"
#include "api_utils.hpp"
#include "storage/authorization/oauth2.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

IcebergAuthorizationType IcebergAuthorization::TypeFromString(const string &type) {
	static const case_insensitive_map_t<IcebergAuthorizationType> mapping {{"oauth2", IcebergAuthorizationType::OAUTH2},
	                                                                       {"sigv4", IcebergAuthorizationType::SIGV4},
	                                                                       {"none", IcebergAuthorizationType::NONE}};

	for (auto it : mapping) {
		if (StringUtil::CIEquals(it.first, type)) {
			return it.second;
		}
	}

	set<string> accepted_options;
	for (auto it : mapping) {
		accepted_options.insert(it.first);
	}
	throw InvalidConfigurationException("'authorization_type' '%s' is not supported, valid options are: %s", type,
	                                    StringUtil::Join(accepted_options, ", "));
}

void IcebergAuthorization::ParseExtraHttpHeaders(const Value &headers_value,
                                                 unordered_map<string, string> &out_headers) {
	if (headers_value.IsNull() || headers_value.type().id() != LogicalTypeId::MAP) {
		return;
	}

	// MAP is internally a LIST<STRUCT(key, value)>
	// Each entry in the list is a STRUCT with exactly two fields: key and value
	auto &map_entries = MapValue::GetChildren(headers_value);

	for (const auto &entry : map_entries) {
		if (entry.type().id() != LogicalTypeId::STRUCT) {
			continue;
		}

		auto &struct_children = StructValue::GetChildren(entry);
		if (struct_children.size() != 2) {
			continue;
		}

		// struct_children[0] = key, struct_children[1] = value
		out_headers[struct_children[0].ToString()] = struct_children[1].ToString();
	}
}

} // namespace duckdb
