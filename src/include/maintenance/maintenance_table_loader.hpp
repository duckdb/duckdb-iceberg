#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

struct IcebergTableInformation;

//! Load metadata into a new table-information instance instead of reusing an
//! already-filled catalog entry.
shared_ptr<IcebergTableInformation> ReloadIcebergTableShared(ClientContext &context, const QualifiedName &table_name,
                                                             const string &function_name);

} // namespace duckdb
