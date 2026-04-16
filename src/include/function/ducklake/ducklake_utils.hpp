#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

struct DuckLakeUtils {
	static unordered_map<LogicalTypeId, string> &GetDuckLakeTypeMap() {
		static unordered_map<LogicalTypeId, string> type_map {{LogicalTypeId::BOOLEAN, "boolean"},
		                                                      {LogicalTypeId::TINYINT, "int8"},
		                                                      {LogicalTypeId::SMALLINT, "int16"},
		                                                      {LogicalTypeId::INTEGER, "int32"},
		                                                      {LogicalTypeId::BIGINT, "int64"},
		                                                      {LogicalTypeId::HUGEINT, "int128"},
		                                                      {LogicalTypeId::UTINYINT, "uint8"},
		                                                      {LogicalTypeId::USMALLINT, "uint16"},
		                                                      {LogicalTypeId::UINTEGER, "uint32"},
		                                                      {LogicalTypeId::UBIGINT, "uint64"},
		                                                      {LogicalTypeId::UHUGEINT, "uint128"},
		                                                      {LogicalTypeId::FLOAT, "float32"},
		                                                      {LogicalTypeId::DOUBLE, "float64"},
		                                                      {LogicalTypeId::DECIMAL, "decimal"},
		                                                      {LogicalTypeId::TIME, "time"},
		                                                      {LogicalTypeId::DATE, "date"},
		                                                      {LogicalTypeId::TIMESTAMP, "timestamp"},
		                                                      {LogicalTypeId::TIMESTAMP, "timestamp_us"},
		                                                      {LogicalTypeId::TIMESTAMP_MS, "timestamp_ms"},
		                                                      {LogicalTypeId::TIMESTAMP_NS, "timestamp_ns"},
		                                                      {LogicalTypeId::TIMESTAMP_SEC, "timestamp_s"},
		                                                      {LogicalTypeId::TIMESTAMP_TZ, "timestamptz"},
		                                                      {LogicalTypeId::TIME_TZ, "timetz"},
		                                                      {LogicalTypeId::INTERVAL, "interval"},
		                                                      {LogicalTypeId::VARCHAR, "varchar"},
		                                                      {LogicalTypeId::BLOB, "blob"},
		                                                      {LogicalTypeId::UUID, "uuid"}};
		return type_map;
	}

	static pair<int64_t, string> GetSnapshots(timestamp_t begin, bool has_end, timestamp_t end,
	                                          const map<timestamp_t, DuckLakeSnapshot> &snapshots) {
		auto &begin_snapshot = snapshots.at(begin);

		string end_snapshot;
		if (has_end) {
			auto &snapshot = snapshots.at(end);
			end_snapshot = to_string(snapshot.snapshot_id.GetIndex());
		} else {
			end_snapshot = "NULL";
		}
		return make_pair<int64_t, string>(begin_snapshot.snapshot_id.GetIndex(), std::move(end_snapshot));
	}

	static LogicalType FromStringBaseType(const string &type_string) {
		auto &type_map = GetDuckLakeTypeMap();

		if (StringUtil::StartsWith(type_string, "decimal")) {
			D_ASSERT(type_string[7] == '(');
			D_ASSERT(type_string.back() == ')');
			auto start = type_string.find('(');
			auto end = type_string.rfind(')');
			auto raw_digits = type_string.substr(start + 1, end - start);
			auto digits = StringUtil::Split(raw_digits, ',');
			D_ASSERT(digits.size() == 2);

			auto width = std::stoi(digits[0]);
			auto scale = std::stoi(digits[1]);
			return LogicalType::DECIMAL(width, scale);
		}

		for (auto it : type_map) {
			if (it.second == type_string) {
				return it.first;
			}
		}
		throw InvalidInputException("Can't convert type string (%s) to DuckLake type", type_string);
	}

	static string ToStringBaseType(const LogicalType &type) {
		auto &type_map = GetDuckLakeTypeMap();

		auto it = type_map.find(type.id());
		if (it == type_map.end()) {
			throw InvalidInputException("Failed to convert DuckDB type to DuckLake - unsupported type %s", type);
		}
		return it->second;
	}

	static string ToDuckLakeColumnType(const LogicalType &type) {
		if (type.HasAlias()) {
			if (type.IsJSONType()) {
				return "json";
			}
			throw InvalidInputException("Unsupported user-defined type");
		}
		switch (type.id()) {
		case LogicalTypeId::STRUCT:
			return "struct";
		case LogicalTypeId::LIST:
			return "list";
		case LogicalTypeId::MAP:
			return "map";
		case LogicalTypeId::DECIMAL:
			return "decimal(" + to_string(DecimalType::GetWidth(type)) + "," + to_string(DecimalType::GetScale(type)) +
			       ")";
		case LogicalTypeId::VARCHAR:
			if (!StringType::GetCollation(type).empty()) {
				throw InvalidInputException("Collations are not supported in DuckLake storage");
			}
			return ToStringBaseType(type);
		default:
			return ToStringBaseType(type);
		}
	}
};

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
