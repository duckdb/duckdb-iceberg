#include "iceberg_logging.hpp"
#include "chrono"
#include "utility"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

constexpr LogLevel IcebergLogType::LEVEL;

IcebergLogType::IcebergLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

} // namespace duckdb
