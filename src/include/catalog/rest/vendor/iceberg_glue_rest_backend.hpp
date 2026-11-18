//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/vendor/iceberg_glue_rest_backend.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/rest/iceberg_rest_backend.hpp"

namespace duckdb {

//! The Iceberg REST endpoint of AWS Glue: ENDPOINT_TYPE 'glue', or a glue.<region>.amazonaws.com URI. It signs
//! requests with SigV4, presets the options Glue needs, and requires CASCADE to drop a namespace.
class IcebergGlueRestBackend : public IcebergRestBackend {
public:
	static constexpr IcebergBackendTypeId TYPE = "iceberg.rest.glue";
	static constexpr const char *DESCRIPTION = "AWS Glue REST Catalog";

public:
	explicit IcebergGlueRestBackend(const Identifier &catalog_name);

	//! CATALOG_TYPE 'rest', ENDPOINT_TYPE 'glue'; also matches an attach to a Glue endpoint URI
	static IcebergBackendFactory Factory();
	//! The region of the Glue endpoint a URI names, or "" if it names none
	static string RegionFromURI(const string &uri);

public:
	bool IsInstanceOf(IcebergBackendTypeId id) const override {
		return StringUtil::Equals(id, TYPE) || IcebergRestBackend::IsInstanceOf(id);
	}
	string ProductName() const override {
		return "AWS Glue";
	}

protected:
	void ConfigureEndpoint(IcebergAttachInput &input, IcebergRestOptions &options,
	                       const case_insensitive_set_t &set_by_user) override;
	void AddFallbackOptions(unordered_map<string, Value> &fallback_options) override;
	string GetConfigWarehouse(const IcebergRestOptions &options) const override;
	void AddDefaultEndpoints() override;
	void AdjustCapabilities(IcebergBackendCapabilities &capabilities) const override;
};

} // namespace duckdb
