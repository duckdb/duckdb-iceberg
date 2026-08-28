#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/partition/iceberg_partition_spec.hpp"
#include "planning/pruning/iceberg_predicate.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"

namespace duckdb {

struct IcebergFilePruner {
public:
	IcebergFilePruner(ClientContext &context, const IcebergTableMetadata &metadata, const IcebergTableSchema &schema,
	                  const IcebergTableFilters &table_filters)
	    : context(context), metadata(metadata), schema(schema), table_filters(table_filters) {
	}

	bool ManifestMatchesFilter(const IcebergManifestFile &manifest) const;
	//! Weighs both kinds of evidence per filter: the file's partition values and its own column bounds.
	METADATA_STATS_PUSHDOWN FileMatchesFilter(const IcebergManifestFile &manifest_file,
	                                          const IcebergManifestEntry &manifest_entry) const;
	bool DeleteManifestMatchesDataFile(const IcebergManifestFile &delete_manifest,
	                                   const IcebergManifestFile &data_manifest,
	                                   const IcebergManifestEntry &data_manifest_entry) const;
	bool DeleteFileMatchesDataFile(const IcebergManifestFile &delete_manifest,
	                               const IcebergManifestEntry &delete_manifest_entry,
	                               const IcebergManifestFile &data_manifest,
	                               const IcebergManifestEntry &data_manifest_entry,
	                               const partition_value_map_t &data_partition_values) const;
	//! Built once per data file: every delete file considered for it is matched against the same values.
	static partition_value_map_t PartitionValueMap(const IcebergDataFile &data_file);

private:
	//! A partition field paired with this file's value for it. `owner_key` is the column the filter is
	//! registered against, which for a nested source differs from the field's own `source_column`.
	struct PartitionFieldValue {
		ColumnIndex owner_key;
		reference<const IcebergPartitionSpecField> field;
		reference<const Value> value;
		reference<const ColumnIndex> source_column;
	};

	//! Only fields on a filtered column are included; a column can source several fields.
	vector<PartitionFieldValue> PartitionValuesForFilteredColumns(const IcebergDataFile &data_file,
	                                                              const IcebergManifestFile &manifest_file) const;
	bool EqualityDeleteMatchesDataFile(const IcebergDataFile &delete_file, const IcebergDataFile &data_file) const;

private:
	ClientContext &context;
	const IcebergTableMetadata &metadata;
	const IcebergTableSchema &schema;
	const IcebergTableFilters &table_filters;
};

} // namespace duckdb
