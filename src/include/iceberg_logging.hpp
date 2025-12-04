#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergLogType : public LogType {
	static constexpr const char *NAME = "Iceberg";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	//! Construct the log type
	IcebergLogType();

	static LogicalType GetLogType() {
		return LogicalType::STRUCT({{"msg", LogicalType::VARCHAR}});
	}

	template <typename... ARGS>
	static string ConstructLogMessage(const string &str, ARGS... params) {
		child_list_t<Value> child_list = {{"msg", StringUtil::Format(str, params...)}};

		return Value::STRUCT(std::move(child_list)).ToString();
	}
};

} // namespace duckdb
