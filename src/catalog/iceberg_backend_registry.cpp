#include "catalog/iceberg_backend_registry.hpp"

#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

IcebergBackendRegistry &IcebergBackendRegistry::Get(DatabaseInstance &db) {
	for (auto &callback : ExtensionCallback::Iterate(db)) {
		auto registry = dynamic_cast<IcebergBackendRegistry *>(callback.get());
		if (registry) {
			return *registry;
		}
	}
	throw InternalException("The Iceberg catalog backend registry was not registered");
}

void IcebergBackendRegistry::Register(IcebergBackendFactory factory) {
	if (factory.api_version != ICEBERG_BACKEND_API_VERSION) {
		throw InvalidInputException(
		    "Cannot register the Iceberg catalog backend '%s': it was built for backend API version %d, but this "
		    "iceberg extension has version %d",
		    factory.catalog_type, factory.api_version, ICEBERG_BACKEND_API_VERSION);
	}
	if (!factory.create) {
		throw InvalidInputException("Cannot register the Iceberg catalog backend '%s' without a create function",
		                            factory.catalog_type);
	}
	lock_guard<mutex> guard(lock);
	for (auto &existing : factories) {
		if (existing.catalog_type == factory.catalog_type && existing.endpoint_type == factory.endpoint_type) {
			throw InvalidInputException("An Iceberg catalog backend for CATALOG_TYPE '%s' and ENDPOINT_TYPE '%s' is "
			                            "already registered",
			                            factory.catalog_type, factory.endpoint_type);
		}
	}
	factories.push_back(std::move(factory));
}

IcebergBackendFactory IcebergBackendRegistry::Resolve(const IcebergAttachInput &input) const {
	lock_guard<mutex> guard(lock);
	optional_ptr<const IcebergBackendFactory> plain;
	for (auto &factory : factories) {
		if (factory.catalog_type != input.catalog_type) {
			continue;
		}
		if (factory.endpoint_type.empty()) {
			plain = factory;
			continue;
		}
		if (!input.endpoint_type.empty()) {
			if (factory.endpoint_type == input.endpoint_type) {
				return factory;
			}
			continue;
		}
		if (factory.matches && factory.matches(input)) {
			return factory;
		}
	}
	if (plain) {
		return *plain;
	}
	set<string> catalog_types;
	for (auto &factory : factories) {
		catalog_types.insert(factory.catalog_type);
	}
	throw InvalidConfigurationException("Unrecognized 'catalog_type' (%s), accepted options are: %s",
	                                    input.catalog_type, StringUtil::Join(catalog_types, ", "));
}

vector<string> IcebergBackendRegistry::GetEndpointTypes(const string &catalog_type) const {
	lock_guard<mutex> guard(lock);
	set<string> endpoint_types;
	for (auto &factory : factories) {
		if (factory.catalog_type == catalog_type && !factory.endpoint_type.empty()) {
			endpoint_types.insert(factory.endpoint_type);
		}
	}
	return vector<string>(endpoint_types.begin(), endpoint_types.end());
}

} // namespace duckdb
