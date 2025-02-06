#include "catalog_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "storage/ic_schema_entry.hpp"
#include "storage/ic_transaction.hpp"

#include <iostream>

namespace duckdb {

string ICUtils::LogicalToIcebergType(const LogicalType &input) {
	switch (input.id()) {
	case LogicalType::TINYINT:
	case LogicalType::UTINYINT:
		return "tinyint";
	case LogicalType::SMALLINT:
	case LogicalType::USMALLINT:
		return "smallint";
	case LogicalType::INTEGER:
	case LogicalType::UINTEGER:
		return "int";
	case LogicalType::BIGINT:
	case LogicalType::UBIGINT:
		return "long";
	case LogicalType::VARCHAR:
		return "string";
	case LogicalType::DOUBLE:
		return "double";
	case LogicalType::FLOAT:
		return "float";
	case LogicalType::BOOLEAN:
		return "boolean";
	case LogicalType::TIMESTAMP:
		return "timestamp";
	case LogicalType::TIMESTAMP_TZ:
		return "timestamptz";
	case LogicalType::BLOB:
		return "binary";
	case LogicalType::DATE:
		return "date";
	case LogicalTypeId::DECIMAL:
		uint8_t precision = DecimalType::GetWidth(input);
		uint8_t scale = DecimalType::GetScale(input);
		return "decimal(" + std::to_string(precision) + ", " + std::to_string(scale) + ")";
	// case LogicalTypeId::ARRAY:
	// case LogicalTypeId::STRUCT:
	// case LogicalTypeId::MAP:
	// default:
	}

	throw std::runtime_error("Unsupported type: " + input.ToString());
}

string ICUtils::TypeToString(const LogicalType &input) {
	switch (input.id()) {
	case LogicalType::VARCHAR:
		return "TEXT";
	case LogicalType::UTINYINT:
		return "TINYINT UNSIGNED";
	case LogicalType::USMALLINT:
		return "SMALLINT UNSIGNED";
	case LogicalType::UINTEGER:
		return "INTEGER UNSIGNED";
	case LogicalType::UBIGINT:
		return "BIGINT UNSIGNED";
	case LogicalType::TIMESTAMP:
		return "DATETIME";
	case LogicalType::TIMESTAMP_TZ:
		return "TIMESTAMP";
	default:
		return input.ToString();
	}
}

LogicalType ICUtils::TypeToLogicalType(const string &type_text) {
	if (type_text == "tinyint") {
		return LogicalType::TINYINT;
	} else if (type_text == "smallint") {
		return LogicalType::SMALLINT;
	} else if (type_text == "bigint") {
		return LogicalType::BIGINT;
	} else if (type_text == "int") {
		return LogicalType::INTEGER;
	} else if (type_text == "long") {
		return LogicalType::BIGINT;
	} else if (type_text == "string") {
		return LogicalType::VARCHAR;
	} else if (type_text == "double") {
		return LogicalType::DOUBLE;
	} else if (type_text == "float") {
		return LogicalType::FLOAT;
	} else if (type_text == "boolean") {
		return LogicalType::BOOLEAN;
	} else if (type_text == "timestamp") {
		return LogicalType::TIMESTAMP;
	} else if (type_text == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (type_text == "binary") {
		return LogicalType::BLOB;
	} else if (type_text == "date") {
		return LogicalType::DATE;
	} else if (type_text.find("decimal(") == 0) {
		size_t spec_end = type_text.find(')');
		if (spec_end != string::npos) {
			size_t sep = type_text.find(',');
			auto prec_str = type_text.substr(8, sep - 8);
			auto scale_str = type_text.substr(sep + 1, spec_end - sep - 1);
			uint8_t prec = Cast::Operation<string_t, uint8_t>(prec_str);
			uint8_t scale = Cast::Operation<string_t, uint8_t>(scale_str);
			return LogicalType::DECIMAL(prec, scale);
		}
	} else if (type_text.find("array<") == 0) {
		size_t type_end = type_text.rfind('>'); // find last, to deal with nested
		if (type_end != string::npos) {
			auto child_type_str = type_text.substr(6, type_end - 6);
			auto child_type = ICUtils::TypeToLogicalType(child_type_str);
			return LogicalType::LIST(child_type);
		}
	} else if (type_text.find("map<") == 0) {
		size_t type_end = type_text.rfind('>'); // find last, to deal with nested
		if (type_end != string::npos) {
			// TODO: Factor this and struct parsing into an iterator over ',' separated values
			vector<LogicalType> key_val;
			size_t cur = 4;
			auto nested_opens = 0;
			for (;;) {
				size_t next_sep = cur;
				// find the location of the next ',' ignoring nested commas
				while (type_text[next_sep] != ',' || nested_opens > 0) {
					if (type_text[next_sep] == '<') {
						nested_opens++;
					} else if (type_text[next_sep] == '>') {
						nested_opens--;
					}
					next_sep++;
					if (next_sep == type_end) {
						break;
					}
				}
				auto child_str = type_text.substr(cur, next_sep - cur);
				auto child_type = ICUtils::TypeToLogicalType(child_str);
				key_val.push_back(child_type);
				if (next_sep == type_end) {
					break;
				}
				cur = next_sep + 1;
			}
			if (key_val.size() != 2) {
				throw NotImplementedException("Invalid map specification with %i types", key_val.size());
			}
			return LogicalType::MAP(key_val[0], key_val[1]);
		}
	} else if (type_text.find("struct<") == 0) {
		size_t type_end = type_text.rfind('>'); // find last, to deal with nested
		if (type_end != string::npos) {
			child_list_t<LogicalType> children;
			size_t cur = 7;
			auto nested_opens = 0;
			for (;;) {
				size_t next_sep = cur;
				// find the location of the next ',' ignoring nested commas
				while (type_text[next_sep] != ',' || nested_opens > 0) {
					if (type_text[next_sep] == '<') {
						nested_opens++;
					} else if (type_text[next_sep] == '>') {
						nested_opens--;
					}
					next_sep++;
					if (next_sep == type_end) {
						break;
					}
				}
				auto child_str = type_text.substr(cur, next_sep - cur);
				size_t type_sep = child_str.find(':');
				if (type_sep == string::npos) {
					throw NotImplementedException("Invalid struct child type specifier: %s", child_str);
				}
				auto child_name = child_str.substr(0, type_sep);
				auto child_type = ICUtils::TypeToLogicalType(child_str.substr(type_sep + 1, string::npos));
				children.push_back({child_name, child_type});
				if (next_sep == type_end) {
					break;
				}
				cur = next_sep + 1;
			}
			return LogicalType::STRUCT(children);
		}
	}

	throw NotImplementedException("Tried to fallback to unknown type for '%s'", type_text);
	// fallback for unknown types
	return LogicalType::VARCHAR;
}

LogicalType ICUtils::ToUCType(const LogicalType &input) {
	// todo do we need this mapping?
	throw NotImplementedException("ToUCType not yet implemented");
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::LIST:
		throw NotImplementedException("PC does not support arrays - unsupported type \"%s\"", input.ToString());
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
		throw NotImplementedException("PC does not support composite types - unsupported type \"%s\"",
		                              input.ToString());
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	default:
		return LogicalType::VARCHAR;
	}
}

} // namespace duckdb
