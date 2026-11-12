
#include "rest_catalog/objects/term.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Term::Term() {
}

Term Term::FromJSON(JSONValue obj) {
	Term res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Term Term::Copy() const {
	Term res;
	if (term_reference.has_value()) {
		res.term_reference.emplace();
		(*res.term_reference) = (*term_reference).Copy();
	}
	if (transform_term.has_value()) {
		res.transform_term.emplace();
		(*res.transform_term) = (*transform_term).Copy();
	}
	return res;
}

string Term::TryFromJSON(JSONValue obj) {
	string error;
	do {
		term_reference.emplace();
		error = term_reference->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			term_reference = nullopt;
		}
		transform_term.emplace();
		error = transform_term->TryFromJSON(obj);
		if (error.empty()) {
			break;
		} else {
			transform_term = nullopt;
		}
		return "Term failed to parse, none of the oneOf candidates matched";
	} while (false);
	return "";
}

JSONMutableValue Term::ToJSON(JSONWriter &writer) const {
	if (term_reference.has_value()) {
		return term_reference->ToJSON(writer);
	} else if (transform_term.has_value()) {
		return transform_term->ToJSON(writer);
	}
	// No variant is active - return empty object
	return writer.CreateObject();
}

} // namespace rest_api_objects
} // namespace duckdb
