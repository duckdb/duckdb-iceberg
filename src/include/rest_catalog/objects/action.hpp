
#pragma once

#include "duckdb/common/json_document.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/mask_alphanum.hpp"
#include "rest_catalog/objects/mask_to_fixed_value.hpp"
#include "rest_catalog/objects/replace_with_null.hpp"
#include "rest_catalog/objects/sha_256global.hpp"
#include "rest_catalog/objects/sha_256query_local.hpp"
#include "rest_catalog/objects/show_first_4.hpp"
#include "rest_catalog/objects/show_last_4.hpp"
#include "rest_catalog/objects/truncate_to_month.hpp"
#include "rest_catalog/objects/truncate_to_year.hpp"

namespace duckdb {
namespace rest_api_objects {

class Action {
public:
	Action();
	Action(const Action &) = delete;
	Action &operator=(const Action &) = delete;
	Action(Action &&) = default;
	Action &operator=(Action &&) = default;

public:
	// Deserialization
	static Action FromJSON(JSONValue obj);
	string TryFromJSON(JSONValue obj);

	// Copy
	Action Copy() const;

	// Serialization
	void PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const;
	JSONMutableValue ToJSON(JSONWriter &writer) const;

public:
	optional<MaskAlphanum> mask_alphanum;
	optional<MaskToFixedValue> mask_to_fixed_value;
	optional<ReplaceWithNull> replace_with_null;
	optional<ShowFirst4> show_first_4;
	optional<ShowLast4> show_last_4;
	optional<TruncateToYear> truncate_to_year;
	optional<TruncateToMonth> truncate_to_month;
	optional<Sha256Global> sha_256global;
	optional<Sha256QueryLocal> sha_256query_local;
};

} // namespace rest_api_objects
} // namespace duckdb
