#include "core/metadata/schema/iceberg_field_mapping.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"

namespace duckdb {

namespace {

bool IsValidMapping(JSONValue entries) {
	if (!entries.IsArray()) {
		return false;
	}
	bool valid = true;
	entries.IterateArray([&](JSONValue entry) {
		if (!valid || !entry.IsObject()) {
			valid = false;
			return;
		}
		auto field_id = entry.GetMember("field-id");
		if (field_id.IsValid() && !field_id.IsNull()) {
			if (!field_id.IsInteger()) {
				valid = false;
				return;
			}
			if (field_id.GetType() == JSONValueType::SIGNED_INTEGER) {
				auto id = field_id.GetSignedInteger();
				if (id < NumericLimits<int32_t>::Minimum() || id > NumericLimits<int32_t>::Maximum()) {
					valid = false;
					return;
				}
			} else if (field_id.GetUnsignedInteger() > uint64_t(NumericLimits<int32_t>::Maximum())) {
				valid = false;
				return;
			}
		}
		auto names = entry.GetMember("names");
		if (!names.IsArray()) {
			valid = false;
			return;
		}
		names.IterateArray([&](JSONValue name) { valid = valid && name.IsString(); });
		auto fields = entry.GetMember("fields");
		if (fields.IsValid() && !fields.IsNull()) {
			valid = valid && IsValidMapping(fields);
		}
	});
	return valid;
}

optional<int32_t> GetFieldId(JSONValue entry) {
	auto field_id = entry.GetMember("field-id");
	if (!field_id.IsValid() || field_id.IsNull()) {
		return nullopt;
	}
	D_ASSERT(field_id.IsInteger());
	return field_id.GetType() == JSONValueType::SIGNED_INTEGER ? static_cast<int32_t>(field_id.GetSignedInteger())
	                                                           : static_cast<int32_t>(field_id.GetUnsignedInteger());
}

//! The entry of a new field: its id and name, and the entries of its nested fields (a struct's fields, a list's
//! 'element', a map's 'key' and 'value')
JSONMutableValue CreateEntry(JSONWriter &writer, const IcebergColumnDefinition &column) {
	auto entry = writer.CreateObject();
	entry.Add("field-id", writer.CreateSignedInteger(column.id));
	auto names = writer.CreateArray();
	names.AppendString(column.name);
	entry.Add("names", names);
	if (!column.GetChildren().empty()) {
		auto fields = writer.CreateArray();
		for (auto &child : column.GetChildren()) {
			fields.Append(CreateEntry(writer, *child));
		}
		entry.Add("fields", fields);
	}
	return entry;
}

vector<reference<const IcebergColumnDefinition>>
AddedUnder(const vector<pair<int32_t, reference<const IcebergColumnDefinition>>> &added, optional<int32_t> parent_id) {
	vector<reference<const IcebergColumnDefinition>> result;
	for (auto &add : added) {
		if (parent_id && add.first == *parent_id) {
			result.push_back(add.second);
		}
	}
	return result;
}

//! The entries of one struct, list or map (or of the table) after the changes. The input is already validated.
JSONMutableValue UpdateEntries(JSONWriter &writer, JSONValue entries, optional<int32_t> parent_id,
                               const unordered_map<int32_t, string> &renamed,
                               const vector<pair<int32_t, reference<const IcebergColumnDefinition>>> &added) {
	//! A name that now belongs to one of these fields leaves the entries of the others
	unordered_map<string, int32_t> assigned;
	entries.IterateArray([&](JSONValue entry) {
		auto field_id = GetFieldId(entry);
		auto rename = field_id ? renamed.find(*field_id) : renamed.end();
		if (rename != renamed.end()) {
			assigned.emplace(rename->second, *field_id);
		}
	});
	auto added_here = AddedUnder(added, parent_id);
	for (auto &column : added_here) {
		assigned.emplace(column.get().name, column.get().id);
	}

	auto result = writer.CreateArray();
	entries.IterateArray([&](JSONValue entry) {
		auto names = entry.GetMember("names");
		auto field_id = GetFieldId(entry);
		auto rename = field_id ? renamed.find(*field_id) : renamed.end();
		auto new_entry = writer.CreateObject();
		if (field_id) {
			new_entry.Add("field-id", writer.CreateSignedInteger(*field_id));
		}
		auto new_names = writer.CreateArray();
		bool has_new_name = false;
		names.IterateArray([&](JSONValue name) {
			auto name_string = name.GetString();
			auto assignment = assigned.find(name_string);
			if (assignment != assigned.end() && (!field_id || assignment->second != *field_id)) {
				return;
			}
			has_new_name = has_new_name || (rename != renamed.end() && name_string == rename->second);
			new_names.AppendString(name_string);
		});
		if (rename != renamed.end() && !has_new_name) {
			new_names.AppendString(rename->second);
		}
		new_entry.Add("names", new_names);
		auto fields = entry.GetMember("fields");
		if (fields.IsValid() && !fields.IsNull()) {
			auto new_fields = UpdateEntries(writer, fields, field_id, renamed, added);
			new_entry.Add("fields", new_fields);
		} else {
			auto added_below = AddedUnder(added, field_id);
			if (!added_below.empty()) {
				auto new_fields = writer.CreateArray();
				for (auto &column : added_below) {
					new_fields.Append(CreateEntry(writer, column));
				}
				new_entry.Add("fields", new_fields);
			}
		}
		result.Append(new_entry);
	});
	for (auto &column : added_here) {
		result.Append(CreateEntry(writer, column));
	}
	return result;
}

} // namespace

optional<string>
IcebergFieldMapping::Update(const string &mapping_json, const unordered_map<int32_t, string> &renamed,
                            const vector<pair<int32_t, reference<const IcebergColumnDefinition>>> &added) {
	JSONParseError error;
	auto doc = JSONDocument::TryParse(mapping_json.c_str(), mapping_json.size(), error);
	if (!doc || !IsValidMapping(doc->GetRoot())) {
		return nullopt;
	}
	JSONWriter writer;
	auto root = UpdateEntries(writer, doc->GetRoot(), IcebergFieldMapping::TABLE_ROOT_ID, renamed, added);
	writer.SetRoot(root);
	return writer.ToString();
}

vector<IcebergFieldMapping> IcebergFieldMapping::Parse(const string &mapping_json) {
	auto doc = JSONDocument::Parse(mapping_json.c_str(), mapping_json.size());
	vector<IcebergFieldMapping> mappings;
	mappings.emplace_back();
	idx_t mapping_index = 1;
	ParseFieldMappings(doc->GetRoot(), mappings, mapping_index, 0);
	return mappings;
}

void IcebergFieldMapping::ParseFieldMappings(JSONValue obj, vector<IcebergFieldMapping> &mappings, idx_t &mapping_index,
                                             idx_t parent_mapping_index) {
	case_insensitive_map_t<IcebergFieldMapping> result;
	obj.IterateArray([&](JSONValue val) {
		auto names = val.GetMember("names");
		auto field_id = val.GetMember("field-id");
		auto fields = val.GetMember("fields");

		//! Create a new mapping entry
		mappings.emplace_back();
		auto &mapping = mappings.back();

		if (!names.IsValid()) {
			throw InvalidInputException("Corrupt metadata.json file, field-mapping is missing names!");
		}
		auto current_mapping_index = mapping_index;

		auto &name_to_mapping_index = mappings[parent_mapping_index].field_mapping_indexes;
		//! Map every entry in the 'names' list to the entry we created above
		names.IterateArray([&](JSONValue name) { name_to_mapping_index[name.GetString()] = current_mapping_index; });
		mapping_index++;

		if (field_id.IsValid()) {
			mapping.field_id = field_id.GetType() == JSONValueType::SIGNED_INTEGER
			                       ? static_cast<int32_t>(field_id.GetSignedInteger())
			                       : static_cast<int32_t>(field_id.GetUnsignedInteger());
		}
		//! Create mappings for the the nested fields
		if (fields.IsValid()) {
			ParseFieldMappings(fields, mappings, mapping_index, current_mapping_index);
		}
	});
}

} // namespace duckdb
