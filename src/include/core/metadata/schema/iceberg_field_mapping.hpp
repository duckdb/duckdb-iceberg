#pragma once

#include "duckdb/common/limits.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

struct IcebergColumnDefinition;

struct IcebergFieldMapping {
public:
	static constexpr const char *PROPERTY = "schema.name-mapping.default";
	//! Parent id of the top-level fields
	static constexpr int32_t TABLE_ROOT_ID = -1;

public:
	static vector<IcebergFieldMapping> Parse(const string &mapping_json);
	//! The mapping after fields were renamed (field id -> new name) or added (parent id -> field), the way Iceberg
	//! Java keeps it: a renamed field keeps its old name and gets the new one, an added field gets an entry under its
	//! parent (with entries for its nested fields), and a name that now belongs to another field leaves the entry it
	//! was an alias of. Nothing when 'mapping_json' is not a name mapping.
	static optional<string> Update(const string &mapping_json, const unordered_map<int32_t, string> &renamed,
	                               const vector<pair<int32_t, reference<const IcebergColumnDefinition>>> &added);

public:
	//! field-id can be omitted for the root of a struct
	//! "Fields that exist in imported files but not in the Iceberg schema may omit field-id."
	int32_t field_id = NumericLimits<int32_t>::Maximum();
	//! "Fields which exist only in the Iceberg schema and not in imported data files may use an empty names list."
	case_insensitive_map_t<idx_t> field_mapping_indexes;

private:
	static void ParseFieldMappings(JSONValue obj, vector<IcebergFieldMapping> &mappings, idx_t &mapping_index,
	                               idx_t parent_mapping_index);
};

} // namespace duckdb
