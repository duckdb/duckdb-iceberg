
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/binary_type_value.hpp"
#include "rest_catalog/objects/field_statistics.hpp"
#include "rest_catalog/objects/file_format.hpp"
#include "rest_catalog/objects/primitive_type_value.hpp"

namespace duckdb {
namespace rest_api_objects {

class ContentFile {
public:
	ContentFile();
	ContentFile(const ContentFile &) = delete;
	ContentFile &operator=(const ContentFile &) = delete;
	ContentFile(ContentFile &&) = default;
	ContentFile &operator=(ContentFile &&) = default;
	class Object9 {
	public:
		Object9();
		Object9(const Object9 &) = delete;
		Object9 &operator=(const Object9 &) = delete;
		Object9(Object9 &&) = default;
		Object9 &operator=(Object9 &&) = default;

	public:
		// Deserialization
		static Object9 FromJSON(JSONValue obj);
		string TryFromJSON(JSONValue obj);

		// Copy
		Object9 Copy() const;

		// Serialization
		void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
		JSONMutableValue ToJSON(JSONWriter &writer) const;

	public:
		case_insensitive_map_t<FieldStatistics> additional_properties;
	};

public:
	// Deserialization
	static ContentFile FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	ContentFile Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	int32_t spec_id;
	vector<PrimitiveTypeValue> partition;
	string content;
	string file_path;
	FileFormat file_format;
	int64_t file_size_in_bytes;
	int64_t record_count;
	optional<BinaryTypeValue> key_metadata;
	optional<vector<int64_t>> split_offsets;
	optional<int32_t> sort_order_id;
	optional<Object9> content_stats;
};

} // namespace rest_api_objects
} // namespace duckdb
