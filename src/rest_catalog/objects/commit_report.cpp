
#include "rest_catalog/objects/commit_report.hpp"

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/list.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

CommitReport::CommitReport() {
}

CommitReport CommitReport::FromJSON(yyjson_val *obj) {
	CommitReport res;
	auto error = res.TryFromJSON(obj);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return res;
}

string CommitReport::TryFromJSON(yyjson_val *obj) {
	string error;
	auto table_name_val = yyjson_obj_get(obj, "table-name");
	if (!table_name_val) {
		return "CommitReport required property 'table-name' is missing";
	} else {
		if (yyjson_is_str(table_name_val)) {
			table_name = yyjson_get_str(table_name_val);
		} else {
			return StringUtil::Format("CommitReport property 'table_name' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(table_name_val));
		}
	}
	auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
	if (!snapshot_id_val) {
		return "CommitReport required property 'snapshot-id' is missing";
	} else {
		if (yyjson_is_int(snapshot_id_val)) {
			snapshot_id = yyjson_get_int(snapshot_id_val);
		} else {
			return StringUtil::Format(
			    "CommitReport property 'snapshot_id' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(snapshot_id_val));
		}
	}
	auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
	if (!sequence_number_val) {
		return "CommitReport required property 'sequence-number' is missing";
	} else {
		if (yyjson_is_int(sequence_number_val)) {
			sequence_number = yyjson_get_int(sequence_number_val);
		} else {
			return StringUtil::Format(
			    "CommitReport property 'sequence_number' is not of type 'integer', found '%s' instead",
			    yyjson_get_type_desc(sequence_number_val));
		}
	}
	auto operation_val = yyjson_obj_get(obj, "operation");
	if (!operation_val) {
		return "CommitReport required property 'operation' is missing";
	} else {
		if (yyjson_is_str(operation_val)) {
			operation = yyjson_get_str(operation_val);
		} else {
			return StringUtil::Format("CommitReport property 'operation' is not of type 'string', found '%s' instead",
			                          yyjson_get_type_desc(operation_val));
		}
	}
	auto metrics_val = yyjson_obj_get(obj, "metrics");
	if (!metrics_val) {
		return "CommitReport required property 'metrics' is missing";
	} else {
		error = metrics.TryFromJSON(metrics_val);
		if (!error.empty()) {
			return error;
		}
	}
	auto metadata_val = yyjson_obj_get(obj, "metadata");
	if (metadata_val) {
		has_metadata = true;
		if (yyjson_is_obj(metadata_val)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(metadata_val, idx, max, key, val) {
				auto key_str = yyjson_get_str(key);
				string tmp;
				if (yyjson_is_str(val)) {
					tmp = yyjson_get_str(val);
				} else {
					return StringUtil::Format("CommitReport property 'tmp' is not of type 'string', found '%s' instead",
					                          yyjson_get_type_desc(val));
				}
				metadata.emplace(key_str, std::move(tmp));
			}
		} else {
			return "CommitReport property 'metadata' is not of type 'object'";
		}
	}
	return string();
}

} // namespace rest_api_objects
} // namespace duckdb
