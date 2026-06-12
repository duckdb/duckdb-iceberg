#include "storage/statistics/iceberg_variant_statistics.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Path / type extraction (ported from ducklake_variant_stats.cpp)
//===--------------------------------------------------------------------===//

//! Extract the variant field names from a stats path. The path inside the variant alternates
//! "typed_value" / <field name>, e.g. {v, typed_value, person, typed_value, age, typed_value}
//! starting at variant_field_start -> {"person", "age"}. An empty result denotes the root.
static vector<string> ExtractVariantFieldNames(const vector<string> &path, idx_t variant_field_start) {
	vector<string> field_names;
	for (idx_t i = variant_field_start; i + 1 < path.size(); i += 2) {
		if (path[i] != "typed_value") {
			throw InvalidInputException("Expected typed_value at position %d in variant stats path %s", i,
			                            StringUtil::Join(path, "."));
		}
		field_names.push_back(path[i + 1]);
	}
	return field_names;
}

//! Walk the variant shredding type to resolve the leaf type of a shredded field. The shredding type
//! nests a "typed_value" field at every layer; struct fields recurse by name and lists by "element".
static LogicalType ExtractVariantType(const LogicalType &variant_type, const vector<string> &field_names,
                                      idx_t field_idx = 0) {
	if (variant_type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("Expected variant shredding type to be a struct, found %s",
		                            variant_type.ToString());
	}
	for (auto &entry : StructType::GetChildTypes(variant_type)) {
		if (entry.first != "typed_value") {
			continue;
		}
		if (field_idx >= field_names.size()) {
			// reached the final type - this is the leaf
			return entry.second;
		}
		auto &field_name = field_names[field_idx];
		if (entry.second.id() == LogicalTypeId::LIST) {
			if (field_name != "element") {
				throw InvalidInputException("Found a list while resolving variant field %s - expected \"element\"",
				                            StringUtil::Join(field_names, "."));
			}
			return ExtractVariantType(ListType::GetChildType(entry.second), field_names, field_idx + 1);
		}
		if (entry.second.id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException("Expected struct while resolving variant field %s, found %s",
			                            StringUtil::Join(field_names, "."), entry.second.ToString());
		}
		for (auto &typed_child : StructType::GetChildTypes(entry.second)) {
			if (typed_child.first == field_name) {
				return ExtractVariantType(typed_child.second, field_names, field_idx + 1);
			}
		}
		throw InvalidInputException("Could not find variant field %s in shredding type %s",
		                            StringUtil::Join(field_names, "."), variant_type.ToString());
	}
	throw InvalidInputException("Could not find typed_value in variant shredding type %s", variant_type.ToString());
}

//! Build a normalized Iceberg JSON path from a list of field names, e.g. {"person","age"} ->
//! "$['person']['age']". Array "element" segments are omitted - per spec the path identifies the
//! array field itself (e.g. "$['tags']") and bounds cover all values within the array.
static string BuildJsonPath(const vector<string> &field_names) {
	string result = "$";
	for (auto &field_name : field_names) {
		if (field_name == "element") {
			continue;
		}
		result += "['";
		result += field_name;
		result += "']";
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Bounds serialization
//===--------------------------------------------------------------------===//

//! Serialize a bounds object (a struct keyed by JSON path) to the parquet-variant encoding by casting
//! it to VARIANT and calling variant_to_parquet_variant, then concatenating the metadata and value blobs.
static bool SerializeBoundsVariant(ClientContext &context, Value bounds_struct, string &out) {
	Value variant_value;
	if (!bounds_struct.DefaultTryCastAs(LogicalType::VARIANT(), variant_value, nullptr)) {
		return false;
	}

	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(std::move(variant_value)));

	ErrorData error;
	FunctionBinder binder(context);
	auto expr = binder.BindScalarFunction(DEFAULT_SCHEMA, "variant_to_parquet_variant", std::move(children), error);
	if (!expr) {
		return false;
	}

	Value result;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *expr, result) || result.IsNull()) {
		return false;
	}

	// parquet variant is STRUCT(metadata BLOB, value BLOB)
	auto &result_children = StructValue::GetChildren(result);
	if (result_children.size() < 2 || result_children[0].IsNull() || result_children[1].IsNull()) {
		return false;
	}
	// return metadata + value
	out = StringValue::Get(result_children[0]) + StringValue::Get(result_children[1]);
	return true;
}

//===--------------------------------------------------------------------===//
// IcebergVariantBounds
//===--------------------------------------------------------------------===//

void IcebergVariantBounds::AddStatsEntry(const vector<string> &full_path, idx_t variant_field_start,
                                         const vector<Value> &col_stats) {
	D_ASSERT(variant_field_start < full_path.size());
	auto &leaf = full_path.back();

	// the "metadata" leaf carries the variant shredding type
	if (full_path.size() == variant_field_start + 1 && leaf == "metadata") {
		for (auto &stat : col_stats) {
			auto &stat_children = StructValue::GetChildren(stat);
			if (StringValue::Get(stat_children[0]) == "variant_type") {
				variant_type_str = StringValue::Get(stat_children[1]);
				has_variant_type = true;
			}
		}
		return;
	}

	// check leafs have values. That means path is not fully shredded
	if (leaf == "value") {
		bool has_null_count = false, has_num_values = false;
		idx_t null_count = 0, num_values = 0;
		for (auto &child : col_stats) {
			auto &child_children = StructValue::GetChildren(child);
			if (StringValue::Get(child_children[0]) == "null_count") {
				null_count = StringUtil::ToUnsigned(StringValue::Get(child_children[1]));
				has_null_count = true;
			}
			if (StringValue::Get(child_children[0]) == "num_values") {
				num_values = StringUtil::ToUnsigned(StringValue::Get(child_children[1]));
				has_num_values = true;
			}
		}
		if (!has_num_values || !has_null_count || (num_values - null_count) > 0) {
			// be conservative: missing counts OR any non-null -> treat as partial
			partial_paths.push_back(ExtractVariantFieldNames(full_path, variant_field_start));
		}
	}
	// only "typed_value" leaves carry shredded bounds; ignore untyped "value" leaves for now
	if (leaf != "typed_value") {
		return;
	}

	FieldBound bound;
	bound.field_names = ExtractVariantFieldNames(full_path, variant_field_start);
	for (auto &stat : col_stats) {
		auto &stat_children = StructValue::GetChildren(stat);
		auto &name = StringValue::Get(stat_children[0]);
		if (name == "min") {
			bound.min_value = StringValue::Get(stat_children[1]);
			bound.has_min = true;
		} else if (name == "max") {
			bound.max_value = StringValue::Get(stat_children[1]);
			bound.has_max = true;
		}
	}
	if (bound.has_min || bound.has_max) {
		fields.push_back(std::move(bound));
	}
}

bool IcebergVariantBounds::HasBounds() const {
	return has_variant_type && !fields.empty();
}

bool IcebergVariantBounds::Finalize(ClientContext &context, bool &has_lower, string &lower_blob, bool &has_upper,
                                    string &upper_blob) {
	has_lower = false;
	has_upper = false;
	if (!HasBounds()) {
		return false;
	}

	LogicalType shredding_type;
	try {
		shredding_type = TransformStringToLogicalType(variant_type_str, context);
	} catch (...) {
		return false;
	}

	child_list_t<Value> lower_children;
	child_list_t<Value> upper_children;
	for (auto &field : fields) {
		bool drop_field = !partial_paths.empty();
		for (auto &partial_path : partial_paths) {
			for (idx_t i = 0; i < partial_path.size(); i++) {
				if (i >= field.field_names.size()) {
					// the partial path of an unshredded field is longer.
					// i.e a.b.c.d (unshredded) is longer than a.b which is not possible
					throw InternalException("Partial path length longer than field path length");
				}
				if (partial_path[i] != field.field_names[i]) {
					// the paths deviate
					drop_field = false;
					break;
				}
			}
		}
		if (drop_field) {
			// field was not fully shredded, so we do not include it in our stats.
			continue;
		}
		LogicalType leaf_type;
		string json_path;
		try {
			leaf_type = ExtractVariantType(shredding_type, field.field_names);
			json_path = BuildJsonPath(field.field_names);
		} catch (...) {
			// could not resolve this field in the first pass - skip it
			continue;
		}
		if (field.has_min) {
			Value typed;
			if (Value(field.min_value).DefaultTryCastAs(leaf_type, typed, nullptr)) {
				lower_children.emplace_back(json_path, std::move(typed));
			}
		}
		if (field.has_max) {
			Value typed;
			if (Value(field.max_value).DefaultTryCastAs(leaf_type, typed, nullptr)) {
				upper_children.emplace_back(json_path, std::move(typed));
			}
		}
	}

	if (lower_children.empty() && upper_children.empty()) {
		return false;
	}
	if (!lower_children.empty()) {
		has_lower = SerializeBoundsVariant(context, Value::STRUCT(std::move(lower_children)), lower_blob);
	}
	if (!upper_children.empty()) {
		has_upper = SerializeBoundsVariant(context, Value::STRUCT(std::move(upper_children)), upper_blob);
	}
	return has_lower || has_upper;
}

//===--------------------------------------------------------------------===//
// Bounds deserialization
//===--------------------------------------------------------------------===//

bool IcebergVariantBoundsReader::Deserialize(ClientContext &context, const string_t &blob, Value &result) {
	if (blob.GetSize() == 0) {
		return false;
	}

	// The parquet extension owns the canonical variant binary decoder; the iceberg extension does not link against
	// it, so we decode through the registered 'variant_bytes_to_variant' scalar function (the inverse of
	// 'variant_to_parquet_variant' that IcebergVariantBounds::Finalize uses to serialize the bounds).
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(Value::BLOB_RAW(blob.GetString())));

	ErrorData error;
	FunctionBinder binder(context);
	auto expr = binder.BindScalarFunction(DEFAULT_SCHEMA, "variant_bytes_to_variant", std::move(children), error);
	if (!expr) {
		return false;
	}

	Value variant_value;
	if (!ExpressionExecutor::TryEvaluateScalar(context, *expr, variant_value) || variant_value.IsNull()) {
		return false;
	}
	result = std::move(variant_value);
	return true;
}

//! Reverse of BuildJsonPath: "$['person']['address']['zip']" -> {"person","address","zip"}, "$" -> {}.
static vector<string> ParseJsonPathSegments(const string &path) {
	vector<string> segments;
	size_t pos = 0;
	while ((pos = path.find("['", pos)) != string::npos) {
		pos += 2;
		auto end = path.find("']", pos);
		if (end == string::npos) {
			break;
		}
		segments.push_back(path.substr(pos, end - pos));
		pos = end + 2;
	}
	return segments;
}

namespace {

//! A node in the reconstructed bounds object: either a leaf scalar bound, or a nested object whose children are
//! kept in insertion order so the resulting struct is deterministic.
struct BoundNode {
	bool is_leaf = false;
	Value leaf;
	vector<std::pair<string, unique_ptr<BoundNode>>> children;

	BoundNode &Child(const string &key) {
		for (auto &entry : children) {
			if (entry.first == key) {
				return *entry.second;
			}
		}
		children.emplace_back(key, make_uniq<BoundNode>());
		return *children.back().second;
	}
};

void InsertPath(BoundNode &root, const vector<string> &segments, const Value &value) {
	BoundNode *node = &root;
	for (auto &segment : segments) {
		node = &node->Child(segment);
	}
	node->is_leaf = true;
	node->leaf = value;
}

Value NodeToValue(const BoundNode &node) {
	if (node.is_leaf || node.children.empty()) {
		return node.leaf;
	}
	child_list_t<Value> struct_children;
	for (auto &entry : node.children) {
		struct_children.emplace_back(entry.first, NodeToValue(*entry.second));
	}
	return Value::STRUCT(std::move(struct_children));
}

} // namespace

bool IcebergVariantBoundsReader::RekeyBoundsVariant(const Value &bounds_variant, Value &result) {
	if (bounds_variant.IsNull() || bounds_variant.type().id() != LogicalTypeId::VARIANT) {
		return false;
	}

	Value struct_value;
	try {
		// Convert the VARIANT into a regular nested Value: objects become STRUCTs keyed by the variant's object
		// keys (the flat JSON paths "$['person']['address']['zip']", ...), with the leaf bounds as typed scalars.
		Vector tmp(bounds_variant);
		RecursiveUnifiedVectorFormat format;
		Vector::RecursiveToUnifiedFormat(tmp, 1, format);
		UnifiedVariantVectorData variant_data(format);
		struct_value = VariantUtils::ConvertVariantToValue(variant_data, 0, 0);
	} catch (...) {
		return false;
	}

	if (struct_value.IsNull() || struct_value.type().id() != LogicalTypeId::STRUCT) {
		return false;
	}

	// The bounds object is flat: each key is a full JSON path. Rebuild the nested structure those paths describe
	// (e.g. "$['person']['address']['zip']" -> {person: {address: {zip: <bound>}}}) so a nested filter access like
	// "variant_col.person.address.zip" resolves against it. A flat dotted key would leave the access NULL.
	auto &child_types = StructType::GetChildTypes(struct_value.type());
	auto &child_values = StructValue::GetChildren(struct_value);
	BoundNode root;
	for (idx_t i = 0; i < child_types.size(); i++) {
		InsertPath(root, ParseJsonPathSegments(child_types[i].first), child_values[i]);
	}
	if (root.children.empty()) {
		return false;
	}

	Value nested_struct = NodeToValue(root);
	Value variant_result;
	if (!nested_struct.DefaultTryCastAs(LogicalType::VARIANT(), variant_result, nullptr)) {
		return false;
	}
	result = std::move(variant_result);
	return true;
}

} // namespace duckdb
