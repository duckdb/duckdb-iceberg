#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/vector.hpp"
#include <stdexcept>

namespace duckdb {
namespace iceberg_test {

struct CatalogError : public std::runtime_error {
	CatalogError(int code, string kind, string message);
	int code;
	string kind;
};

[[noreturn]] void Invalid(const string &message);
[[noreturn]] void Unsupported(const string &message);

//! Small owning JSON tree for the test server's independent metadata model.
//! Scalar JSON is retained verbatim (including large numbers and default values).
//! DuckDB's JSON parser/writer remains responsible for parsing and escaping.
class Json {
public:
	Json();
	Json(const char *value); // NOLINT: convenient JSON construction
	Json(string value);      // NOLINT
	Json(int64_t value);     // NOLINT
	static Json Object();
	static Json Object(std::initializer_list<std::pair<const string, Json>> values);
	static Json Array();
	static Json Array(vector<Json> values);
	static Json Boolean(bool value);
	static Json Parse(const string &text);

	bool IsNull() const;
	bool IsString() const;
	bool IsObject() const;
	bool IsArray() const;
	bool Has(const string &key) const;
	const Json &At(const string &key) const;
	Json &At(const string &key);
	const Json &Get(const string &key) const;
	Json &operator[](const string &key);
	void Erase(const string &key);
	void Push(Json value);
	const vector<Json> &Items() const;
	vector<Json> &Items();
	const map<string, Json> &Members() const;
	string String() const;
	int64_t Integer() const;
	bool Bool() const;
	string Dump() const;
	bool operator==(const Json &other) const;
	bool operator!=(const Json &other) const;

private:
	static Json Read(JSONValue value);
	JSONMutableValue Write(JSONWriter &writer) const;

	JSONValueType type = JSONValueType::JSON_NULL;
	string scalar = "null";
	map<string, Json> object;
	vector<Json> array;
};

} // namespace iceberg_test
} // namespace duckdb
