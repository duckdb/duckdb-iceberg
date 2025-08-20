#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

struct IcebergLogType : public LogType {
	static constexpr const char *NAME = "Iceberg";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	//! Construct the log type
	IcebergLogType();

	static LogicalType GetLogType() {
		return LogicalType::VARCHAR;
	}

	template <typename... ARGS>
	static string ConstructLogMessage(const string &str, ARGS... params) {
		return StringUtil::Format(str, params...);
	}
};

} // namespace duckdb
