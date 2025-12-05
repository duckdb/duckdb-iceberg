#include "iceberg_logging.hpp"

namespace duckdb {

constexpr LogLevel IcebergLogType::LEVEL;

IcebergLogType::IcebergLogType() : LogType(NAME, LEVEL) {
}

} // namespace duckdb
