#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"

namespace duckdb {

struct IcebergFilePruner {
public:
	IcebergFilePruner(ClientContext &context, const IcebergTableMetadata &metadata, const IcebergTableSchema &schema,
	                  const IcebergTableFilters &table_filters)
	    : context(context), metadata(metadata), schema(schema), table_filters(table_filters) {
	}

	bool ManifestMatchesFilter(const IcebergManifestFile &manifest) const;
	bool FileMatchesFilter(const IcebergManifestFile &manifest_file, const IcebergManifestEntry &manifest_entry) const;
	//! True only when every table filter is provably satisfied by every row of the data file, proven from
	//! partition values alone. Lets a DELETE drop the file at the metadata level instead of rewriting rows.
	bool FileFullyCoveredByFilter(const IcebergManifestFile &manifest_file,
	                              const IcebergManifestEntry &manifest_entry) const;
	//! Necessary but not sufficient for FileFullyCoveredByFilter, which must also prove the partition value
	//! satisfies the filter. Depends only on the spec, the schema and the filters, never on a data file.
	bool PartitionSpecHasFieldForEveryFilterColumn(int32_t partition_spec_id) const;
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
	bool FilePartitionMatchesFilter(const IcebergDataFile &data_file, const IcebergManifestFile &manifest_file) const;
	bool EqualityDeleteMatchesDataFile(const IcebergDataFile &delete_file, const IcebergDataFile &data_file) const;

private:
	ClientContext &context;
	const IcebergTableMetadata &metadata;
	const IcebergTableSchema &schema;
	const IcebergTableFilters &table_filters;
};

} // namespace duckdb
