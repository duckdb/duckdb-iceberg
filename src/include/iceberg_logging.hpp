#pragma once

#include "duckdb/logging/logger.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "chrono"
#include "utility"
#include "duckdb/logging/logging.hpp"

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

class IcebergLogging {
public:
	template <typename Func>
	static auto LogFuncTime(ClientContext &context, Func &&func, const std::string &message) -> decltype(func()) {
		auto start = std::chrono::high_resolution_clock::now();

		auto result = std::forward<Func>(func)();

		auto end = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		auto timing = duration.count();
		if (timing > 0) {
			DUCKDB_LOG(context, IcebergLogType, "{%s:'%dms'}", message, timing);
		}
		return result;
	}
};

} // namespace duckdb
