#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

//! Pending files can retain assigned lineage, but cannot inherit identities before commit.
enum class IcebergRowLineageMode : uint8_t { COMMITTED, STORED_ONLY, NONE };

} // namespace duckdb
