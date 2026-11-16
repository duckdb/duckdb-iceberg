#include "common/iceberg_properties.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unordered_map<string, string>::const_iterator IcebergProperties::Find(const unordered_map<string, string> &properties,
                                                                      const string &name) {
	const auto canonical = StringUtil::Lower(name);
	auto result = properties.find(canonical);
	if (result != properties.end()) {
		return result;
	}
	for (auto it = properties.begin(); it != properties.end(); ++it) {
		if (!StringUtil::CIEquals(it->first, canonical)) {
			continue;
		}
		if (result != properties.end()) {
			throw InvalidConfigurationException(
			    "Ambiguous Iceberg property '%s': multiple case variants without a lowercase key", canonical);
		}
		result = it;
	}
	return result;
}

unordered_map<string, string>::const_iterator
IcebergProperties::FindConfig(const unordered_map<string, string> &properties, const string &name) {
	auto result = properties.end();
	for (auto it = properties.begin(); it != properties.end(); ++it) {
		if (!StringUtil::CIEquals(it->first, name)) {
			continue;
		}
		if (result != properties.end()) {
			throw InvalidConfigurationException("Duplicate Iceberg config property '%s'", StringUtil::Lower(name));
		}
		result = it;
	}
	return result;
}

} // namespace duckdb
