#include "catalog_json.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {
namespace iceberg_test {

CatalogError::CatalogError(int code_p, string kind_p, string message)
    : std::runtime_error(std::move(message)), code(code_p), kind(std::move(kind_p)) {
}

void Invalid(const string &message) {
	throw CatalogError(400, "ValidationException", message);
}

void Unsupported(const string &message) {
	throw CatalogError(501, "UnsupportedOperationException", message);
}

Json::Json() = default;
Json::Json(const char *value) : Json(string(value)) {
}
Json::Json(string value) : type(JSONValueType::STRING), scalar(std::move(value)) {
}
Json::Json(int64_t value) : type(JSONValueType::SIGNED_INTEGER), scalar(std::to_string(value)) {
}
Json Json::Object() {
	Json result;
	result.type = JSONValueType::OBJECT;
	return result;
}
Json Json::Object(std::initializer_list<std::pair<const string, Json>> values) {
	auto result = Object();
	for (auto &entry : values) {
		result.object.emplace(entry.first, entry.second);
	}
	return result;
}
Json Json::Array() {
	return Array({});
}
Json Json::Array(vector<Json> values) {
	Json result;
	result.type = JSONValueType::ARRAY;
	result.array = std::move(values);
	return result;
}
Json Json::Boolean(bool value) {
	Json result;
	result.type = JSONValueType::BOOLEAN;
	result.scalar = value ? "true" : "false";
	return result;
}
Json Json::Parse(const string &text) {
	JSONParseError error;
	auto document = JSONDocument::TryParse(text.data(), text.size(), error, JSONReadFlags::BIGNUM_AS_RAW);
	if (!document) {
		Invalid("Invalid JSON: " + error.message);
	}
	return Read(document->GetRoot());
}
Json Json::Read(JSONValue value) {
	Json result;
	result.type = value.GetType();
	if (value.IsObject()) {
		value.IterateObject([&](const string &key, JSONValue child) {
			if (!result.object.emplace(key, Read(child)).second) {
				Invalid("Duplicate JSON member: " + key);
			}
		});
	} else if (value.IsArray()) {
		value.IterateArray([&](JSONValue child) { result.array.push_back(Read(child)); });
	} else {
		result.scalar = value.IsString() ? value.GetString() : value.ToString();
	}
	return result;
}
bool Json::IsNull() const {
	return type == JSONValueType::JSON_NULL;
}
bool Json::IsString() const {
	return type == JSONValueType::STRING;
}
bool Json::IsObject() const {
	return type == JSONValueType::OBJECT;
}
bool Json::IsArray() const {
	return type == JSONValueType::ARRAY;
}
bool Json::Has(const string &key) const {
	return IsObject() && object.count(key);
}
const Json &Json::At(const string &key) const {
	if (!Has(key)) {
		Invalid("Missing JSON member: " + key);
	}
	return object.at(key);
}
Json &Json::At(const string &key) {
	if (!Has(key)) {
		Invalid("Missing JSON member: " + key);
	}
	return object.at(key);
}
const Json &Json::Get(const string &key) const {
	static const Json null_value;
	return Has(key) ? object.at(key) : null_value;
}
Json &Json::operator[](const string &key) {
	if (!IsObject()) {
		Invalid("Expected JSON object");
	}
	return object[key];
}
void Json::Erase(const string &key) {
	if (!IsObject()) {
		Invalid("Expected JSON object");
	}
	object.erase(key);
}
void Json::Push(Json value) {
	Items().push_back(std::move(value));
}
const vector<Json> &Json::Items() const {
	if (!IsArray()) {
		Invalid("Expected JSON array");
	}
	return array;
}
vector<Json> &Json::Items() {
	if (!IsArray()) {
		Invalid("Expected JSON array");
	}
	return array;
}
const map<string, Json> &Json::Members() const {
	if (!IsObject()) {
		Invalid("Expected JSON object");
	}
	return object;
}
string Json::String() const {
	if (!IsString()) {
		Invalid("Expected JSON string");
	}
	return scalar;
}
int64_t Json::Integer() const {
	if (type != JSONValueType::SIGNED_INTEGER && type != JSONValueType::UNSIGNED_INTEGER) {
		Invalid("Expected signed 64-bit integer");
	}
	auto document = JSONDocument::Parse(scalar.data(), scalar.size());
	auto value = document->GetRoot();
	if (value.GetType() == JSONValueType::UNSIGNED_INTEGER) {
		auto number = value.GetUnsignedInteger();
		if (number > uint64_t(NumericLimits<int64_t>::Maximum())) {
			Invalid("Integer exceeds signed 64-bit range");
		}
		return int64_t(number);
	}
	return value.GetSignedInteger();
}
bool Json::Bool() const {
	if (type != JSONValueType::BOOLEAN) {
		Invalid("Expected JSON boolean");
	}
	return scalar == "true";
}
JSONMutableValue Json::Write(JSONWriter &writer) const {
	if (IsObject()) {
		auto result = writer.CreateObject();
		for (auto &entry : object) {
			result.Add(entry.first, entry.second.Write(writer));
		}
		return result;
	}
	if (IsArray()) {
		auto result = writer.CreateArray();
		for (auto &entry : array) {
			result.Append(entry.Write(writer));
		}
		return result;
	}
	if (IsString()) {
		return writer.CreateString(scalar);
	}
	auto document = JSONDocument::Parse(scalar.data(), scalar.size(), JSONReadFlags::BIGNUM_AS_RAW);
	return writer.CreateCopy(document->GetRoot());
}
string Json::Dump() const {
	JSONWriter writer;
	writer.SetRoot(Write(writer));
	return writer.ToString();
}
bool Json::operator==(const Json &other) const {
	// Object order is immaterial; signed and unsigned representations of the same integer agree.
	return Dump() == other.Dump();
}
bool Json::operator!=(const Json &other) const {
	return !(*this == other);
}

} // namespace iceberg_test
} // namespace duckdb
