
#include "rest_catalog/objects/partition_spec.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

PartitionSpec::PartitionSpec() {
}

PartitionSpec PartitionSpec::FromJSON(yyjson_val *obj) {
	PartitionSpec res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

PartitionSpec PartitionSpec::Copy() const {
	PartitionSpec res;
	res.fields.reserve(fields.size());
	for (auto &item : fields) {
		res.fields.emplace_back(item.Copy());
	}
	if (spec_id.has_value()) {
		res.spec_id.emplace();
		(*res.spec_id) = (*spec_id);
	}
	return res;
}

string PartitionSpec::TryFromJSON(yyjson_val *obj) {
	string error;
	auto fields_val = yyjson_obj_get(obj, "fields");
	if (!fields_val) {
		return "PartitionSpec required property 'fields' is missing";
	} else {
		if (yyjson_is_arr(fields_val)) {
			size_t fields_idx, fields_max;
			yyjson_val *fields_item_val;
			yyjson_arr_foreach(fields_val, fields_idx, fields_max, fields_item_val) {
				PartitionField fields_item;
				error = fields_item.TryFromJSON(fields_item_val);
				if (!error.empty()) {
					return error;
				}
				fields.emplace_back(std::move(fields_item));
			}
		} else {
			return StringUtil::Format("PartitionSpec property 'fields' is not of type 'array', found '%s' instead",
			                          yyjson_get_type_desc(fields_val));
		}
	}
	auto spec_id_val = yyjson_obj_get(obj, "spec-id");
	if (spec_id_val) {
		int32_t spec_id_tmp;
		if (yyjson_is_int(spec_id_val)) {
			spec_id_tmp = yyjson_get_int(spec_id_val);
		} else {
			return StringUtil::Format(
			    "PartitionSpec property 'spec_id_tmp' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(spec_id_val));
		}
		spec_id = std::move(spec_id_tmp);
	}
	return "";
}

void PartitionSpec::PopulateJSON(yyjson_mut_doc *doc, yyjson_mut_val *obj) const {
	if (!yyjson_mut_is_obj(obj)) {
		throw InternalException("PopulateJSON requires obj to be a JSON object");
	}

	// Serialize: fields
	yyjson_mut_val *fields_arr = yyjson_mut_arr(doc);
	for (const auto &item : fields) {
		yyjson_mut_val *item_val = item.ToJSON(doc);
		yyjson_mut_arr_append(fields_arr, item_val);
	}
	yyjson_mut_obj_add_val(doc, obj, "fields", fields_arr);

	// Serialize: spec-id
	if (spec_id.has_value()) {
		auto &spec_id_value = *spec_id;
		yyjson_mut_obj_add_int(doc, obj, "spec-id", spec_id_value);
	}
}

yyjson_mut_val *PartitionSpec::ToJSON(yyjson_mut_doc *doc) const {
	yyjson_mut_val *obj = yyjson_mut_obj(doc);
	PopulateJSON(doc, obj);
	return obj;
}

} // namespace rest_api_objects
} // namespace duckdb
