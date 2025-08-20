#include "metadata/iceberg_field_mapping.hpp"

namespace duckdb {

void IcebergFieldMapping::ParseFieldMappings(yyjson_val *obj, vector<IcebergFieldMapping> &mappings,
                                             idx_t &mapping_index, idx_t parent_mapping_index) {
	case_insensitive_map_t<IcebergFieldMapping> result;
	size_t idx, max;
	yyjson_val *val;
	yyjson_arr_foreach(obj, idx, max, val) {
		auto names = yyjson_obj_get(val, "names");
		auto field_id = yyjson_obj_get(val, "field-id");
		auto fields = yyjson_obj_get(val, "fields");

		//! Create a new mapping entry
		mappings.emplace_back();
		auto &mapping = mappings.back();

		if (!names) {
			throw InvalidInputException("Corrupt metadata.json file, field-mapping is missing names!");
		}
		auto current_mapping_index = mapping_index;

		auto &name_to_mapping_index = mappings[parent_mapping_index].field_mapping_indexes;
		//! Map every entry in the 'names' list to the entry we created above
		size_t names_idx, names_max;
		yyjson_val *names_val;
		yyjson_arr_foreach(names, names_idx, names_max, names_val) {
			auto name = yyjson_get_str(names_val);
			name_to_mapping_index[name] = current_mapping_index;
		}
		mapping_index++;

		if (field_id) {
			mapping.field_id = yyjson_get_sint(field_id);
		}
		//! Create mappings for the the nested fields
		if (fields) {
			ParseFieldMappings(fields, mappings, mapping_index, current_mapping_index);
		}
	}
}

} // namespace duckdb
