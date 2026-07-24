#include "core/metadata/schema/iceberg_table_schema.hpp"

#include "catalog/rest/api/iceberg_create_table_request.hpp"
#include "duckdb/common/exception.hpp"
#include "common/iceberg_utils.hpp"
#include "rest_catalog/objects/list.hpp"
#include "catalog/rest/api/iceberg_type.hpp"

namespace duckdb {

namespace {

static Value GetFieldIdValue(const IcebergColumnDefinition &column) {
	auto column_value = Value::BIGINT(column.id);
	if (!column.GetChildCount()) {
		return column_value;
	}
	child_list_t<Value> values;
	values.emplace_back("__duckdb_field_id", std::move(column_value));
	for (idx_t i = 0; i < column.GetChildCount(); i++) {
		auto child = column.GetChild(i);
		values.emplace_back(child->name, GetFieldIdValue(*child));
	}
	return Value::STRUCT(std::move(values));
}

} // namespace

shared_ptr<IcebergTableSchema> IcebergTableSchema::ParseSchema(const rest_api_objects::Schema &schema) {
	auto res = make_shared_ptr<IcebergTableSchema>();
	D_ASSERT(schema.object_1.schema_id);
	res->schema_id = *schema.object_1.schema_id;
	for (auto &field : schema.struct_type.fields) {
		res->columns.push_back(IcebergColumnDefinition::ParseStructField(*field));
	}
	if (auto &identifier_field_ids = schema.object_1.identifier_field_ids) {
		res->identifier_field_ids = *identifier_field_ids;
	}
	return res;
}

rest_api_objects::Schema IcebergTableSchema::ToRESTObject() const {
	std::unique_ptr<yyjson_mut_doc, YyjsonDocDeleter> doc_p(yyjson_mut_doc_new(nullptr));
	yyjson_mut_doc *doc = doc_p.get();
	auto root_object = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root_object);
	IcebergCreateTableRequest::PopulateSchema(doc, root_object, *this);
	auto schema_json = ICUtils::JsonToString(std::move(doc_p));

	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> parsed_doc(yyjson_read(schema_json.c_str(), schema_json.size(), 0));
	if (!parsed_doc) {
		throw InternalException("Failed to parse generated schema JSON");
	}
	return rest_api_objects::Schema::FromJSON(yyjson_doc_get_root(parsed_doc.get()));
}

void IcebergTableSchema::PopulateSourceIdMap(unordered_map<uint64_t, ColumnIndex> &source_to_column_id,
                                             const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                             optional_ptr<ColumnIndex> parent) {
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &column = columns[i];

		ColumnIndex new_index;
		if (parent) {
			auto primary = parent->GetPrimaryIndex();
			auto child_indexes = parent->GetChildIndexes();
			child_indexes.push_back(ColumnIndex(i));
			new_index = ColumnIndex(primary, child_indexes);
		} else {
			new_index = ColumnIndex(i);
		}

		PopulateSourceIdMap(source_to_column_id, column->GetChildren(), new_index);
		source_to_column_id.emplace(static_cast<uint64_t>(column->id), std::move(new_index));
	}
}

const IcebergColumnDefinition &
IcebergTableSchema::GetFromColumnIndex(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                       const ColumnIndex &column_index, idx_t depth) {
	auto &child_indexes = column_index.GetChildIndexes();
	auto &selected_index = depth ? child_indexes[depth - 1] : column_index;

	auto index = selected_index.GetPrimaryIndex();
	if (index >= columns.size()) {
		throw InvalidConfigurationException("ColumnIndex out of bounds for columns (index %d, 'columns' size: %d)",
		                                    index, columns.size());
	}
	auto &column = columns[index];
	if (depth == child_indexes.size()) {
		return *column;
	}
	if (!column->GetChildCount()) {
		throw InvalidConfigurationException(
		    "Expected column to have children, ColumnIndex has a depth of %d, we reached only %d",
		    column_index.ChildIndexCount(), depth);
	}
	return GetFromColumnIndex(column->GetChildren(), column_index, depth + 1);
}

const unordered_map<uint64_t, ColumnIndex> &IcebergTableSchema::GetSourceIdMap() const {
	lock_guard<mutex> guard(source_id_map_lock);
	if (!source_id_map_populated) {
		PopulateSourceIdMap(source_to_column_id, columns, nullptr);
		source_id_map_populated = true;
	}
	return source_to_column_id;
}

optional<ColumnIndex> IcebergTableSchema::TryGetColumnIndexByFieldId(idx_t field_id) const {
	auto &source_to_column_id = GetSourceIdMap();
	auto it = source_to_column_id.find(field_id);
	if (it == source_to_column_id.end()) {
		return std::nullopt;
	}
	return it->second;
}

optional_ptr<const IcebergColumnDefinition> IcebergTableSchema::TryGetColumnByFieldId(idx_t field_id) const {
	auto column_index = TryGetColumnIndexByFieldId(field_id);
	if (!column_index) {
		return nullptr;
	}
	return GetFromColumnIndex(columns, *column_index, 0);
}

const IcebergColumnDefinition &IcebergTableSchema::GetColumnByFieldId(idx_t field_id) const {
	auto column = TryGetColumnByFieldId(field_id);
	if (!column) {
		throw InvalidInputException("Field id %d does not exist in schema with id %d", field_id, schema_id);
	}
	return *column;
}

optional_ptr<const IcebergColumnDefinition>
IcebergTableSchema::GetFromPath(const vector<Identifier> &path, optional_ptr<optional_idx> name_offset) const {
	D_ASSERT(!path.empty());

	optional_ptr<const IcebergColumnDefinition> result;
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &column = *columns[i];
		if (column.name != path[0]) {
			continue;
		}
		result = column;
	}
	if (!result) {
		return nullptr;
	}
	reference<const IcebergColumnDefinition> res(*result);
	for (idx_t i = 1; i < path.size(); i++) {
		auto &column = res.get();
		if (column.type.id() == LogicalTypeId::VARIANT) {
			if (name_offset) {
				*name_offset = i;
				return column;
			}
			throw InvalidInputException(
			    "Column path %s points to child of variant column %s - but no name_offset is provided",
			    StringUtil::Join(IdentifiersToStrings(path), "."), res.get().name);
		}
		auto next_child = column.GetChild(path[i].GetIdentifierName());
		if (!next_child) {
			return nullptr;
		}
		res = *next_child;
	}
	return res.get();
}

optional_ptr<IcebergColumnDefinition> IcebergTableSchema::GetMutableFromPath(const vector<Identifier> &path,
                                                                             optional_ptr<optional_idx> names_offset) {
	auto res = GetFromPath(path, names_offset);
	if (!res) {
		return nullptr;
	}
	InvalidateSourceIdMap();
	auto &col = *res;
	return const_cast<IcebergColumnDefinition &>(col);
}

void IcebergTableSchema::InvalidateSourceIdMap() {
	lock_guard<mutex> guard(source_id_map_lock);
	source_to_column_id.clear();
	source_id_map_populated = false;
}

shared_ptr<IcebergTableSchema> IcebergTableSchema::Copy() const {
	auto res = make_shared_ptr<IcebergTableSchema>();
	res->schema_id = schema_id;
	res->last_column_id = last_column_id;
	res->identifier_field_ids = identifier_field_ids;
	for (auto &column : columns) {
		res->columns.push_back(column->Copy());
	}
	return res;
}

shared_ptr<IcebergTableSchema> IcebergTableSchema::RemoveColumn(const string &name, optional_idx &column_id) const {
	auto res = make_shared_ptr<IcebergTableSchema>();
	res->schema_id = schema_id + 1;
	res->last_column_id = last_column_id;
	res->identifier_field_ids = identifier_field_ids;
	for (auto &column : columns) {
		if (column->name == name) {
			column_id = column->id;
			continue;
		}
		res->columns.push_back(column->Copy());
	}
	return res;
}

const LogicalType &IcebergTableSchema::GetColumnTypeFromFieldId(idx_t field_id) const {
	return GetColumnByFieldId(field_id).type;
}

void IcebergTableSchema::GetColumnNamesAndTypes(vector<string> &names, vector<LogicalType> &types) const {
	names.reserve(columns.size());
	types.reserve(columns.size());
	for (auto &column_p : columns) {
		auto &column = *column_p;
		names.push_back(column.name);
		types.push_back(column.type);
	}
}

void IcebergTableSchema::GetFieldIdValues(child_list_t<Value> &values) const {
	for (auto &column : columns) {
		values.emplace_back(column->name, GetFieldIdValue(*column));
	}
}

Value IcebergTableSchema::GetFieldIds() const {
	child_list_t<Value> values;
	GetFieldIdValues(values);
	return Value::STRUCT(std::move(values));
}

bool IcebergTableSchema::Equals(const IcebergTableSchema &other) const {
	if (columns.size() != other.columns.size()) {
		return false;
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &a = *columns[i];
		auto &b = *other.columns[i];

		if (!a.Equals(b)) {
			return false;
		}
	}
	if (identifier_field_ids.size() != other.identifier_field_ids.size()) {
		return false;
	}
	for (idx_t i = 0; i < identifier_field_ids.size(); i++) {
		if (identifier_field_ids[i] != other.identifier_field_ids[i]) {
			return false;
		}
	}

	return true;
}

} // namespace duckdb
