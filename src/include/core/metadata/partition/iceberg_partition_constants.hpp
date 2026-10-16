#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

//! Resolves identity-partition fallback values independently of planning or reading.
struct IcebergPartitionConstants {
	//! Use the selected schema's type when present, otherwise the widest historical
	//! source type so dropped columns' partition values are not narrowed after promotion.
	static optional_ptr<const LogicalType> GetType(int32_t field_id, const IcebergTableSchema &schema,
	                                               const IcebergTableMetadataSchemas &schemas);

	//! The last spec field for a source wins, including non-identity transforms.
	//! Missing, NULL, and untyped values are omitted. SQL encoding supplies typed
	//! NULLs where required; readers preserve their normal missing-column defaults.
	static unordered_map<int32_t, Value> Resolve(int32_t spec_id, const vector<IcebergPartitionInfo> &partition_values,
	                                             const IcebergTableMetadata &metadata,
	                                             const IcebergTableSchema &schema);
};

} // namespace duckdb
