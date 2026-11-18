//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/vendor/iceberg_s3tables_rest_backend.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/rest/iceberg_rest_backend.hpp"

namespace duckdb {

//! The Iceberg REST endpoint of AWS S3 Tables: ENDPOINT_TYPE 's3_tables', with a table bucket ARN as the warehouse.
//! It signs requests with SigV4 and presets the options S3 Tables needs (S3 Tables collects unreferenced files
//! itself, so DuckDB never deletes the files it wrote).
class IcebergS3TablesRestBackend : public IcebergRestBackend {
public:
	static constexpr IcebergBackendTypeId TYPE = "iceberg.rest.s3_tables";
	static constexpr const char *DESCRIPTION = "AWS S3 Tables REST Catalog";

public:
	explicit IcebergS3TablesRestBackend(const Identifier &catalog_name);

	//! CATALOG_TYPE 'rest', ENDPOINT_TYPE 's3_tables'
	static IcebergBackendFactory Factory();

public:
	bool IsInstanceOf(IcebergBackendTypeId id) const override {
		return StringUtil::Equals(id, TYPE) || IcebergRestBackend::IsInstanceOf(id);
	}

protected:
	void ConfigureEndpoint(IcebergAttachInput &input, IcebergRestOptions &options,
	                       const case_insensitive_set_t &set_by_user) override;
	void AddFallbackOptions(unordered_map<string, Value> &fallback_options) override;
	void AddDefaultEndpoints() override;
};

} // namespace duckdb
