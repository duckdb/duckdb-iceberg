
#include "rest_catalog/objects/data_file.hpp"

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "rest_catalog/objects/json_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {
namespace rest_api_objects {

DataFile::DataFile() {
}

DataFile DataFile::FromJSON(JSONValue obj) {
	DataFile res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

DataFile DataFile::Copy() const {
	DataFile res;
	res.content_file = content_file.Copy();
	if (first_row_id.has_value()) {
		res.first_row_id.emplace();
		(*res.first_row_id) = (*first_row_id);
	}
	if (column_sizes.has_value()) {
		res.column_sizes.emplace();
		(*res.column_sizes) = (*column_sizes).Copy();
	}
	if (value_counts.has_value()) {
		res.value_counts.emplace();
		(*res.value_counts) = (*value_counts).Copy();
	}
	if (null_value_counts.has_value()) {
		res.null_value_counts.emplace();
		(*res.null_value_counts) = (*null_value_counts).Copy();
	}
	if (nan_value_counts.has_value()) {
		res.nan_value_counts.emplace();
		(*res.nan_value_counts) = (*nan_value_counts).Copy();
	}
	if (lower_bounds.has_value()) {
		res.lower_bounds.emplace();
		(*res.lower_bounds) = (*lower_bounds).Copy();
	}
	if (upper_bounds.has_value()) {
		res.upper_bounds.emplace();
		(*res.upper_bounds) = (*upper_bounds).Copy();
	}
	return res;
}

string DataFile::TryFromJSON(JSONValue obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_refinement_val = obj.GetMember("content");
	if (content_refinement_val.IsValid()) {
		string content_refinement;
		if (json_utils::IsString(content_refinement_val)) {
			content_refinement = json_utils::GetString(content_refinement_val);
		} else {
			return StringUtil::Format(
			    "DataFile property 'content_refinement' is not of type 'string', found %s instead",
			    json_utils::GetTypeDescription(content_refinement_val).c_str());
		}
		if (!content_refinement_val.IsNull() && content_refinement != "data") {
			return "DataFile property 'content_refinement' does not match its required const value";
		}
	} else {
		return "DataFile required property 'content' is missing";
	}
	auto first_row_id_val = obj.GetMember("first-row-id");
	if (first_row_id_val.IsValid()) {
		int64_t first_row_id_tmp;
		if (json_utils::IsInteger(first_row_id_val)) {
			first_row_id_tmp = json_utils::GetSignedInteger(first_row_id_val);
		} else if (json_utils::IsUnsignedInteger(first_row_id_val)) {
			first_row_id_tmp = json_utils::GetUnsignedInteger(first_row_id_val);
		} else {
			return StringUtil::Format("DataFile property 'first_row_id_tmp' is not of type 'integer', found %s instead",
			                          json_utils::GetTypeDescription(first_row_id_val).c_str());
		}
		first_row_id = std::move(first_row_id_tmp);
	}
	auto column_sizes_val = obj.GetMember("column-sizes");
	if (column_sizes_val.IsValid()) {
		CountMap column_sizes_tmp;
		error = column_sizes_tmp.TryFromJSON(column_sizes_val);
		if (!error.empty()) {
			return error;
		}
		column_sizes = std::move(column_sizes_tmp);
	}
	auto value_counts_val = obj.GetMember("value-counts");
	if (value_counts_val.IsValid()) {
		CountMap value_counts_tmp;
		error = value_counts_tmp.TryFromJSON(value_counts_val);
		if (!error.empty()) {
			return error;
		}
		value_counts = std::move(value_counts_tmp);
	}
	auto null_value_counts_val = obj.GetMember("null-value-counts");
	if (null_value_counts_val.IsValid()) {
		CountMap null_value_counts_tmp;
		error = null_value_counts_tmp.TryFromJSON(null_value_counts_val);
		if (!error.empty()) {
			return error;
		}
		null_value_counts = std::move(null_value_counts_tmp);
	}
	auto nan_value_counts_val = obj.GetMember("nan-value-counts");
	if (nan_value_counts_val.IsValid()) {
		CountMap nan_value_counts_tmp;
		error = nan_value_counts_tmp.TryFromJSON(nan_value_counts_val);
		if (!error.empty()) {
			return error;
		}
		nan_value_counts = std::move(nan_value_counts_tmp);
	}
	auto lower_bounds_val = obj.GetMember("lower-bounds");
	if (lower_bounds_val.IsValid()) {
		ValueMap lower_bounds_tmp;
		error = lower_bounds_tmp.TryFromJSON(lower_bounds_val);
		if (!error.empty()) {
			return error;
		}
		lower_bounds = std::move(lower_bounds_tmp);
	}
	auto upper_bounds_val = obj.GetMember("upper-bounds");
	if (upper_bounds_val.IsValid()) {
		ValueMap upper_bounds_tmp;
		error = upper_bounds_tmp.TryFromJSON(upper_bounds_val);
		if (!error.empty()) {
			return error;
		}
		upper_bounds = std::move(upper_bounds_tmp);
	}
	return "";
}

void DataFile::PopulateJSON(JSONWriter &writer, JSONMutableValue obj) const {
	// Serialize base class: ContentFile
	content_file.PopulateJSON(writer, obj);

	// Serialize: first-row-id
	if (first_row_id.has_value()) {
		auto &first_row_id_value = *first_row_id;
		auto first_row_id_json = writer.CreateSignedInteger(first_row_id_value);
		obj.Add("first-row-id", first_row_id_json);
	}

	// Serialize: column-sizes
	if (column_sizes.has_value()) {
		auto &column_sizes_value = *column_sizes;
		auto column_sizes_json = column_sizes_value.ToJSON(writer);
		obj.Add("column-sizes", column_sizes_json);
	}

	// Serialize: value-counts
	if (value_counts.has_value()) {
		auto &value_counts_value = *value_counts;
		auto value_counts_json = value_counts_value.ToJSON(writer);
		obj.Add("value-counts", value_counts_json);
	}

	// Serialize: null-value-counts
	if (null_value_counts.has_value()) {
		auto &null_value_counts_value = *null_value_counts;
		auto null_value_counts_json = null_value_counts_value.ToJSON(writer);
		obj.Add("null-value-counts", null_value_counts_json);
	}

	// Serialize: nan-value-counts
	if (nan_value_counts.has_value()) {
		auto &nan_value_counts_value = *nan_value_counts;
		auto nan_value_counts_json = nan_value_counts_value.ToJSON(writer);
		obj.Add("nan-value-counts", nan_value_counts_json);
	}

	// Serialize: lower-bounds
	if (lower_bounds.has_value()) {
		auto &lower_bounds_value = *lower_bounds;
		auto lower_bounds_json = lower_bounds_value.ToJSON(writer);
		obj.Add("lower-bounds", lower_bounds_json);
	}

	// Serialize: upper-bounds
	if (upper_bounds.has_value()) {
		auto &upper_bounds_value = *upper_bounds;
		auto upper_bounds_json = upper_bounds_value.ToJSON(writer);
		obj.Add("upper-bounds", upper_bounds_json);
	}
}

JSONMutableValue DataFile::ToJSON(JSONWriter &writer) const {
	auto obj = writer.CreateObject();
	PopulateJSON(writer, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
