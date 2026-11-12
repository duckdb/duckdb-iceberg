
#include "rest_catalog/objects/action.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

Action::Action() {
}

Action Action::FromJSON(JSONValue obj) {
	Action res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

Action Action::Copy() const {
	Action res;
	if (mask_alphanum.has_value()) {
		res.mask_alphanum.emplace();
		(*res.mask_alphanum) = (*mask_alphanum).Copy();
	}
	if (mask_to_fixed_value.has_value()) {
		res.mask_to_fixed_value.emplace();
		(*res.mask_to_fixed_value) = (*mask_to_fixed_value).Copy();
	}
	if (replace_with_null.has_value()) {
		res.replace_with_null.emplace();
		(*res.replace_with_null) = (*replace_with_null).Copy();
	}
	if (show_first_4.has_value()) {
		res.show_first_4.emplace();
		(*res.show_first_4) = (*show_first_4).Copy();
	}
	if (show_last_4.has_value()) {
		res.show_last_4.emplace();
		(*res.show_last_4) = (*show_last_4).Copy();
	}
	if (truncate_to_year.has_value()) {
		res.truncate_to_year.emplace();
		(*res.truncate_to_year) = (*truncate_to_year).Copy();
	}
	if (truncate_to_month.has_value()) {
		res.truncate_to_month.emplace();
		(*res.truncate_to_month) = (*truncate_to_month).Copy();
	}
	if (sha_256global.has_value()) {
		res.sha_256global.emplace();
		(*res.sha_256global) = (*sha_256global).Copy();
	}
	if (sha_256query_local.has_value()) {
		res.sha_256query_local.emplace();
		(*res.sha_256query_local) = (*sha_256query_local).Copy();
	}
	return res;
}

string Action::TryFromJSON(JSONValue obj) {
	string error;
	auto discriminator_val = obj.GetMember("action");
	if (!discriminator_val.IsValid() || !discriminator_val.IsString()) {
		return "Action discriminator 'action' is missing or is not a string";
	}
	string discriminator = discriminator_val.GetString();
	if (discriminator == "mask-alphanum") {
		mask_alphanum.emplace();
		error = mask_alphanum->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "mask-to-fixed-value") {
		mask_to_fixed_value.emplace();
		error = mask_to_fixed_value->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "replace-with-null") {
		replace_with_null.emplace();
		error = replace_with_null->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "show-first-4") {
		show_first_4.emplace();
		error = show_first_4->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "show-last-4") {
		show_last_4.emplace();
		error = show_last_4->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "truncate-to-year") {
		truncate_to_year.emplace();
		error = truncate_to_year->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "truncate-to-month") {
		truncate_to_month.emplace();
		error = truncate_to_month->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "sha-256-global") {
		sha_256global.emplace();
		error = sha_256global->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else if (discriminator == "sha-256-query-local") {
		sha_256query_local.emplace();
		error = sha_256query_local->TryFromJSON(obj);
		if (!error.empty()) {
			return error;
		}
	} else {
		return StringUtil::Format("Action has unknown discriminator value '%s'", discriminator.c_str());
	}
	return "";
}

void Action::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	if (mask_alphanum.has_value()) {
		mask_alphanum->PopulateJSON(writer, obj);
	} else if (mask_to_fixed_value.has_value()) {
		mask_to_fixed_value->PopulateJSON(writer, obj);
	} else if (replace_with_null.has_value()) {
		replace_with_null->PopulateJSON(writer, obj);
	} else if (show_first_4.has_value()) {
		show_first_4->PopulateJSON(writer, obj);
	} else if (show_last_4.has_value()) {
		show_last_4->PopulateJSON(writer, obj);
	} else if (truncate_to_year.has_value()) {
		truncate_to_year->PopulateJSON(writer, obj);
	} else if (truncate_to_month.has_value()) {
		truncate_to_month->PopulateJSON(writer, obj);
	} else if (sha_256global.has_value()) {
		sha_256global->PopulateJSON(writer, obj);
	} else if (sha_256query_local.has_value()) {
		sha_256query_local->PopulateJSON(writer, obj);
	}
}

JSONMutableValue Action::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
