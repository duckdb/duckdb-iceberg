
#include "storage/iceberg_authorization.hpp"
#include "api_utils.hpp"
#include "storage/authorization/oauth2.hpp"

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

} // namespace duckdb
