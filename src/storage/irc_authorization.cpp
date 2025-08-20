#include "storage/irc_authorization.hpp"
#include "api_utils.hpp"
#include "storage/authorization/oauth2.hpp"

namespace duckdb {

IRCAuthorizationType IRCAuthorization::TypeFromString(const string &type) {
	static const case_insensitive_map_t<IRCAuthorizationType> mapping {{"oauth2", IRCAuthorizationType::OAUTH2},
	                                                                   {"sigv4", IRCAuthorizationType::SIGV4},
	                                                                   {"none", IRCAuthorizationType::NONE}};

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
