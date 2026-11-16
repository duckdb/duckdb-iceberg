
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace rest_api_objects {
namespace json_utils {

inline bool IsNull(const JSONValue &value) {
	return value.IsNull();
}

inline void *GetNull(const JSONValue &value) {
	return nullptr;
}

inline bool IsString(const JSONValue &value) {
	return value.IsString();
}

inline string GetString(const JSONValue &value) {
	return value.GetString();
}

inline bool IsInteger(const JSONValue &value) {
	return value.IsInteger();
}

inline bool IsUnsignedInteger(const JSONValue &value) {
	return value.GetType() == JSONValueType::UNSIGNED_INTEGER;
}

inline bool IsBoolean(const JSONValue &value) {
	return value.GetType() == JSONValueType::BOOLEAN;
}

inline bool GetBoolean(const JSONValue &value) {
	return value.GetBoolean();
}

inline bool IsNumber(const JSONValue &value) {
	return value.IsInteger() || value.GetType() == JSONValueType::DOUBLE;
}

inline int64_t GetSignedInteger(const JSONValue &value) {
	return value.GetType() == JSONValueType::SIGNED_INTEGER ? value.GetSignedInteger()
	                                                        : static_cast<int64_t>(value.GetUnsignedInteger());
}

inline uint64_t GetUnsignedInteger(const JSONValue &value) {
	return value.GetType() == JSONValueType::UNSIGNED_INTEGER ? value.GetUnsignedInteger()
	                                                          : static_cast<uint64_t>(value.GetSignedInteger());
}

inline double GetNumber(const JSONValue &value) {
	return value.IsInteger() ? static_cast<double>(GetSignedInteger(value)) : value.GetDouble();
}

inline string GetTypeDescription(const JSONValue &value) {
	return StringUtil::Format("JSON type %d", static_cast<int>(value.GetType()));
}

} // namespace json_utils
} // namespace rest_api_objects
} // namespace duckdb
