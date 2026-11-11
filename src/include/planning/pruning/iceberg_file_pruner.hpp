#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "planning/pruning/iceberg_table_filter.hpp"

namespace duckdb {

//! Delete-side bounds shared by all data files in one scan. An empty initialized
//! entry also caches the absence of usable statistics.
struct IcebergEqualityDeleteBounds {
	struct ColumnBounds {
		reference<const IcebergColumnDefinition> column;
		Value lower_bound;
		Value upper_bound;
	};

	bool initialized = false;
	vector<ColumnBounds> columns;
};

struct IcebergFilePruner {
public:
	IcebergFilePruner(ClientContext &context, const IcebergTableMetadata &metadata, const IcebergTableSchema &schema,
	                  const IcebergTableFilters &table_filters)
	    : context(context), metadata(metadata), schema(schema), table_filters(table_filters) {
	}

	bool ManifestMatchesFilter(const IcebergManifestListEntry &entry) const;
	bool FileMatchesFilter(const IcebergManifest &manifest_file, const IcebergManifestEntry &manifest_entry) const;
	bool DeleteManifestMatchesDataFile(const IcebergManifest &delete_manifest, const IcebergManifest &data_manifest,
	                                   const IcebergManifestEntry &data_manifest_entry) const;
	bool DeleteFileMatchesDataFile(const IcebergManifest &delete_manifest,
	                               const IcebergManifestEntry &delete_manifest_entry,
	                               const IcebergManifest &data_manifest,
	                               const IcebergManifestEntry &data_manifest_entry,
	                               const partition_value_map_t &data_partition_values,
	                               optional_ptr<IcebergEqualityDeleteBounds> delete_bounds) const;
	//! Built once per data file: every delete file considered for it is matched against the same values.
	static partition_value_map_t PartitionValueMap(const IcebergDataFile &data_file);

private:
	bool FilePartitionMatchesFilter(const IcebergDataFile &data_file, const IcebergManifest &manifest_file) const;
	void InitializeEqualityDeleteBounds(const IcebergDataFile &delete_file, IcebergEqualityDeleteBounds &bounds) const;
	bool EqualityDeleteMatchesDataFile(const IcebergDataFile &delete_file, const IcebergDataFile &data_file,
	                                   const IcebergEqualityDeleteBounds &bounds) const;

private:
	ClientContext &context;
	const IcebergTableMetadata &metadata;
	const IcebergTableSchema &schema;
	const IcebergTableFilters &table_filters;
};

} // namespace duckdb
