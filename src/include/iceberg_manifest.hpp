#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "iceberg_types.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

//! fwd declare
struct ManifestReaderInput;

//! Iceberg Manifest scan routines

struct IcebergManifestV1 {
	static constexpr idx_t FORMAT_VERSION = 1;
	using entry_type = IcebergManifest;
	static idx_t ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input, vector<entry_type> &entries);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

struct IcebergManifestV2 {
	static constexpr idx_t FORMAT_VERSION = 2;
	using entry_type = IcebergManifest;
	static idx_t ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input, vector<entry_type> &entries);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

//! Iceberg Manifest Entry scan routines

struct IcebergManifestEntryV1 {
	static constexpr idx_t FORMAT_VERSION = 1;
	using entry_type = IcebergManifestEntry;
	static idx_t ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input, vector<entry_type> &entries);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

struct IcebergManifestEntryV2 {
	static constexpr idx_t FORMAT_VERSION = 2;
	using entry_type = IcebergManifestEntry;
	static idx_t ProduceEntries(DataChunk &chunk, idx_t offset, idx_t count, const ManifestReaderInput &input, vector<entry_type> &entries);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

class AvroScan {
public:
	AvroScan(const string &scan_name, ClientContext &context, const string &path);
public:
	bool GetNext(DataChunk &chunk);
	void InitializeChunk(DataChunk &chunk);
	bool Finished() const;
public:
	optional_ptr<TableFunction> avro_scan;
	ClientContext &context;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	vector<LogicalType> return_types;
	vector<string> return_names;

	bool finished = false;
};


} // namespace duckdb
