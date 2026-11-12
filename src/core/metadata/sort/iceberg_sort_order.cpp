#include "core/metadata/sort/iceberg_sort_order.hpp"
#include "catalog/rest/api/catalog_utils.hpp"

namespace duckdb {

IcebergSortOrderField IcebergSortOrderField::ParseFromJson(const rest_api_objects::SortField &field) {
	IcebergSortOrderField result;
	if (field.source_id <= 0) {
		throw InvalidConfigurationException("Invalid source id %d in Iceberg sort order", field.source_id);
	}
	result.source_id = field.source_id;
	result.transform = field.transform.value;
	result.direction = field.direction.value;
	result.null_order = field.null_order.value;
	return result;
}

IcebergSortOrder IcebergSortOrder::ParseFromJson(const rest_api_objects::SortOrder &sort_order_spec) {
	if (sort_order_spec.order_id < 0) {
		throw InvalidConfigurationException("Iceberg sort order must have a non-negative 'order-id'");
	}
	IcebergSortOrder result(sort_order_spec.order_id);
	for (auto &field : sort_order_spec.fields) {
		result.fields.push_back(IcebergSortOrderField::ParseFromJson(field));
	}

	return result;
}

bool IcebergSortOrder::Equals(const IcebergSortOrder &other) const {
	if (other.fields.size() != fields.size()) {
		return false;
	}
	for (idx_t i = 0; i < other.fields.size(); i++) {
		//! Compare source ids
		if (other.fields[i].source_id != fields[i].source_id) {
			return false;
		}
		//! Compare transforms
		if (other.fields[i].transform.RawType() != fields[i].transform.RawType()) {
			return false;
		}
		//! Compare transforms
		if (other.fields[i].direction != fields[i].direction) {
			return false;
		}
		if (other.fields[i].null_order != fields[i].null_order) {
			return false;
		}
	}
	return true;
}

bool IcebergSortOrder::IsSorted() const {
	return !fields.empty();
}

JSONMutableValue IcebergSortOrderField::ToJSON(JSONWriter &writer) const {
	auto res = writer.CreateObject();
	res.AddString("transform", transform.RawType());
	//! FIXME: 'source-ids' (array) if >= V3
	res.Add("source-id", writer.CreateSignedInteger(source_id));
	res.AddString("direction", direction);
	res.AddString("null-order", null_order);
	return res;
}

JSONMutableValue IcebergSortOrder::ToJSON(JSONWriter &writer) const {
	auto res = writer.CreateObject();
	res.Add("order-id", writer.CreateSignedInteger(sort_order_id));
	auto fields_arr = writer.CreateArray();
	for (auto &field : fields) {
		fields_arr.Append(field.ToJSON(writer));
	}
	res.Add("fields", fields_arr);
	return res;
}

} // namespace duckdb
