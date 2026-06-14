
#include "rest_catalog/objects/data_file.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

DataFile::DataFile() {
}

DataFile DataFile::FromJSON(yyjson_val *obj) {
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
	res.content = content;
	if (has_first_row_id) {
		res.first_row_id = first_row_id;
	}
	res.has_first_row_id = has_first_row_id;
	if (has_column_sizes) {
		res.column_sizes = column_sizes.Copy();
	}
	res.has_column_sizes = has_column_sizes;
	if (has_value_counts) {
		res.value_counts = value_counts.Copy();
	}
	res.has_value_counts = has_value_counts;
	if (has_null_value_counts) {
		res.null_value_counts = null_value_counts.Copy();
	}
	res.has_null_value_counts = has_null_value_counts;
	if (has_nan_value_counts) {
		res.nan_value_counts = nan_value_counts.Copy();
	}
	res.has_nan_value_counts = has_nan_value_counts;
	if (has_lower_bounds) {
		res.lower_bounds = lower_bounds.Copy();
	}
	res.has_lower_bounds = has_lower_bounds;
	if (has_upper_bounds) {
		res.upper_bounds = upper_bounds.Copy();
	}
	res.has_upper_bounds = has_upper_bounds;
	return res;
}
string DataFile::TryFromJSON(yyjson_val *obj) {
	string error;
	error = content_file.TryFromJSON(obj);
	if (!error.empty()) {
		return error;
	}
	auto content_val = yyjson_obj_get(obj, "content");
	if (!content_val) {
		return "DataFile required property 'content' is missing";
	} else {
		if (yyjson_is_str(content_val)) {
			content = yyjson_get_str(content_val);
		} else {
			return StringUtil::Format("DataFile property 'content' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(content_val));
		}
	}
	auto first_row_id_val = yyjson_obj_get(obj, "first-row-id");
	if (first_row_id_val && !yyjson_is_null(first_row_id_val)) {
		has_first_row_id = true;
		if (yyjson_is_sint(first_row_id_val)) {
			first_row_id = yyjson_get_sint(first_row_id_val);
		} else if (yyjson_is_uint(first_row_id_val)) {
			first_row_id = yyjson_get_uint(first_row_id_val);
		} else {
			return StringUtil::Format("DataFile property 'first_row_id' is not of type 'integer', found '%s' instead",
			                          yyjson_get_type_desc(first_row_id_val));
		}
	}
	auto column_sizes_val = yyjson_obj_get(obj, "column-sizes");
	if (column_sizes_val && !yyjson_is_null(column_sizes_val)) {
		has_column_sizes = true;
		error = column_sizes.TryFromJSON(column_sizes_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto value_counts_val = yyjson_obj_get(obj, "value-counts");
	if (value_counts_val && !yyjson_is_null(value_counts_val)) {
		has_value_counts = true;
		error = value_counts.TryFromJSON(value_counts_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto null_value_counts_val = yyjson_obj_get(obj, "null-value-counts");
	if (null_value_counts_val && !yyjson_is_null(null_value_counts_val)) {
		has_null_value_counts = true;
		error = null_value_counts.TryFromJSON(null_value_counts_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto nan_value_counts_val = yyjson_obj_get(obj, "nan-value-counts");
	if (nan_value_counts_val && !yyjson_is_null(nan_value_counts_val)) {
		has_nan_value_counts = true;
		error = nan_value_counts.TryFromJSON(nan_value_counts_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto lower_bounds_val = yyjson_obj_get(obj, "lower-bounds");
	if (lower_bounds_val && !yyjson_is_null(lower_bounds_val)) {
		has_lower_bounds = true;
		error = lower_bounds.TryFromJSON(lower_bounds_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto upper_bounds_val = yyjson_obj_get(obj, "upper-bounds");
	if (upper_bounds_val && !yyjson_is_null(upper_bounds_val)) {
		has_upper_bounds = true;
		error = upper_bounds.TryFromJSON(upper_bounds_val);
		if (!error.empty()) {
			return error;
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
