//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"

#include "iceberg_options.hpp"

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"

#include "rest_catalog/objects/table_metadata.hpp"

#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "metadata/iceberg_partition_spec.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "metadata/iceberg_field_mapping.hpp"
#include "metadata/iceberg_manifest.hpp"

#include "storage/iceberg_transaction_data.hpp"

using namespace duckdb_yyjson;

namespace duckdb {} // namespace duckdb
