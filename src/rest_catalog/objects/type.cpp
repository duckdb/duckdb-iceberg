
#include "rest_catalog/objects/type.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Type::Type() {
}

Type Type::FromJSON(JSONValue obj) {
	Type res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Type Type::Copy() const {
	Type res;
	if (variant_type.has_value()) {
		res.variant_type.emplace();
		(*res.variant_type) = (*variant_type).Copy();
	}
	if (primitive_type.has_value()) {
		res.primitive_type.emplace();
		(*res.primitive_type) = (*primitive_type).Copy();
	}
	if (struct_type.has_value()) {
		res.struct_type.emplace();
		(*res.struct_type) = (*struct_type).Copy();
	}
	if (list_type.has_value()) {
		res.list_type.emplace();
		(*res.list_type) = (*list_type).Copy();
	}
	if (map_type.has_value()) {
		res.map_type.emplace();
		(*res.map_type) = (*map_type).Copy();
	}
	return res;
}

string Type::TryFromJSON(JSONValue obj) {
	string error;
	do {
		variant_type.emplace();
		error = variant_type->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			variant_type = nullopt;
		}
		primitive_type.emplace();
		error = primitive_type->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			primitive_type = nullopt;
		}
		struct_type.emplace();
		error = struct_type->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			struct_type = nullopt;
		}
		list_type.emplace();
		error = list_type->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			list_type = nullopt;
		}
		map_type.emplace();
		error = map_type->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			map_type = nullopt;
		}
		return "Type failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

JSONMutableValue Type::ToJSON(JSONWriter &writer) const {
	if (variant_type.has_value()) {
		return variant_type->ToJSON(writer);
	} else if (primitive_type.has_value()) {
		return primitive_type->ToJSON(writer);
	} else if (struct_type.has_value()) {
		return struct_type->ToJSON(writer);
	} else if (list_type.has_value()) {
		return list_type->ToJSON(writer);
	} else if (map_type.has_value()) {
		return map_type->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
