#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "iceberg_types.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

//! Iceberg Manifest scan routines

struct IcebergManifestV1 {
	static constexpr const char *NAME = "IcebergManifestList";
	static constexpr idx_t FORMAT_VERSION = 1;
	using entry_type = IcebergManifest;
	static void ProduceEntries(DataChunk &input, const case_insensitive_map_t<ColumnIndex> &name_to_vec, vector<entry_type> &entries);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

struct IcebergManifestV2 {
	static constexpr const char *NAME = "IcebergManifestList";
	static constexpr idx_t FORMAT_VERSION = 2;
	using entry_type = IcebergManifest;
	static void ProduceEntries(DataChunk &input, const case_insensitive_map_t<ColumnIndex> &name_to_vec, vector<entry_type> &entries);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

//! Iceberg Manifest Entry scan routines

struct IcebergManifestEntryV1 {
	static constexpr const char *NAME = "IcebergManifest";
	static constexpr idx_t FORMAT_VERSION = 1;
	using entry_type = IcebergManifestEntry;
	static void ProduceEntries(DataChunk &input, const case_insensitive_map_t<ColumnIndex> &name_to_vec, vector<entry_type> &entries);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

struct IcebergManifestEntryV2 {
	static constexpr const char *NAME = "IcebergManifest";
	static constexpr idx_t FORMAT_VERSION = 2;
	using entry_type = IcebergManifestEntry;
	static void ProduceEntries(DataChunk &input, const case_insensitive_map_t<ColumnIndex> &name_to_vec, vector<entry_type> &entries);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

} // namespace duckdb
