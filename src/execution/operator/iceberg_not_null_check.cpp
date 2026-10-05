#include "execution/operator/iceberg_not_null_check.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

namespace {

//! The nullability of one Iceberg field and its descendants.
struct NotNullNode {
	//! Dotted path used in the error message, e.g. "l.element"
	string path;
	LogicalTypeId type_id;
	bool required;
	//! Whether this field or any of its descendants is required
	bool has_required;
	//! STRUCT: one per field. LIST: the element. MAP: the key and the value.
	vector<NotNullNode> children;

	bool operator==(const NotNullNode &other) const {
		return path == other.path && type_id == other.type_id && required == other.required &&
		       children == other.children;
	}
};

NotNullNode BuildNode(const IcebergColumnDefinition &column, const string &path) {
	NotNullNode node;
	node.path = path;
	node.type_id = column.type.id();
	node.required = column.required;
	node.has_required = column.required;
	switch (node.type_id) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
		for (auto &child : column.GetChildren()) {
			node.children.push_back(BuildNode(*child, path + "." + child->name));
			node.has_required |= node.children.back().has_required;
		}
		break;
	case LogicalTypeId::MAP: {
		auto &key = *column.GetChild("key");
		auto &value = *column.GetChild("value");
		auto key_node = BuildNode(key, path + ".key");
		//! Iceberg map keys are always required, but DuckDB rejects a NULL map key when the map is built, so
		//! the key itself is not checked. Fields inside a struct key still are.
		key_node.required = false;
		key_node.has_required = false;
		for (auto &child : key_node.children) {
			key_node.has_required |= child.has_required;
		}
		node.children.push_back(std::move(key_node));
		node.children.push_back(BuildNode(value, path + ".value"));
		for (auto &child : node.children) {
			node.has_required |= child.has_required;
		}
		break;
	}
	default:
		break;
	}
	return node;
}

struct NotNullCheckBindData : public FunctionData {
	NotNullCheckBindData(NotNullNode root_p, string table_name_p)
	    : root(std::move(root_p)), table_name(std::move(table_name_p)) {
	}

	NotNullNode root;
	string table_name;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<NotNullCheckBindData>(root, table_name);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<NotNullCheckBindData>();
		return root == other.root && table_name == other.table_name;
	}
};

[[noreturn]] void ThrowNotNull(const NotNullCheckBindData &bind_data, const NotNullNode &node) {
	if (bind_data.table_name.empty()) {
		throw ConstraintException("NOT NULL constraint failed: %s", node.path);
	}
	throw ConstraintException("NOT NULL constraint failed: %s.%s", bind_data.table_name, node.path);
}

//! Collects the child rows of the present lists (or maps) among `rows`.
void GetListChildRows(const UnifiedVectorFormat &format, const vector<idx_t> &rows, vector<idx_t> &child_rows) {
	auto entries = format.GetData<list_entry_t>();
	for (auto row : rows) {
		auto &entry = entries[format.sel->get_index(row)];
		for (idx_t i = 0; i < entry.length; i++) {
			child_rows.push_back(entry.offset + i);
		}
	}
}

//! Checks `node` for the given rows, whose parent is present. Recurses only into present values.
void CheckRows(const NotNullCheckBindData &bind_data, const NotNullNode &node,
               const RecursiveUnifiedVectorFormat &format, const vector<idx_t> &rows) {
	if (!node.has_required || rows.empty()) {
		return;
	}
	auto &unified = format.unified;
	//! Fast path: without a validity mask every row is present, so there is nothing to filter.
	vector<idx_t> filtered;
	const vector<idx_t> *present_ptr = &rows;
	if (!unified.validity.CannotHaveNull()) {
		filtered.reserve(rows.size());
		for (auto row : rows) {
			if (unified.validity.RowIsValid(unified.sel->get_index(row))) {
				filtered.push_back(row);
			} else if (node.required) {
				ThrowNotNull(bind_data, node);
			}
		}
		present_ptr = &filtered;
	}
	auto &present = *present_ptr;

	switch (node.type_id) {
	case LogicalTypeId::STRUCT:
		//! The fields of a struct share its row index.
		D_ASSERT(format.children.size() == node.children.size());
		for (idx_t i = 0; i < node.children.size(); i++) {
			CheckRows(bind_data, node.children[i], format.children[i], present);
		}
		break;
	case LogicalTypeId::LIST: {
		D_ASSERT(format.children.size() == 1 && node.children.size() == 1);
		vector<idx_t> child_rows;
		GetListChildRows(unified, present, child_rows);
		CheckRows(bind_data, node.children[0], format.children[0], child_rows);
		break;
	}
	case LogicalTypeId::MAP: {
		//! A map is a list of (key, value) structs; the entry structs themselves are never NULL.
		D_ASSERT(format.children.size() == 1 && format.children[0].children.size() == 2 && node.children.size() == 2);
		vector<idx_t> child_rows;
		GetListChildRows(unified, present, child_rows);
		auto &entries = format.children[0];
		CheckRows(bind_data, node.children[0], entries.children[0], child_rows);
		CheckRows(bind_data, node.children[1], entries.children[1], child_rows);
		break;
	}
	default:
		break;
	}
}

void NotNullCheckFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<NotNullCheckBindData>();
	auto &input = args.data[0];

	RecursiveUnifiedVectorFormat format;
	Vector::RecursiveToUnifiedFormat(input, format);
	vector<idx_t> rows(args.size());
	for (idx_t i = 0; i < rows.size(); i++) {
		rows[i] = i;
	}
	CheckRows(bind_data, bind_data.root, format, rows);
	result.Reference(input);
}

} // namespace

unique_ptr<Expression> IcebergNotNullCheck::Wrap(unique_ptr<Expression> input, const IcebergColumnDefinition &column,
                                                 const string &table_name) {
	auto root = BuildNode(column, column.name);
	if (!root.has_required) {
		return input;
	}
	auto type = input->GetReturnType();
	ScalarFunction function("iceberg_check_not_null", {type}, type, NotNullCheckFunction);
	//! NULL input must reach the check rather than short-circuit to a NULL result.
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetFallible();

	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(input));
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments),
	                                          make_uniq<NotNullCheckBindData>(std::move(root), table_name));
}

} // namespace duckdb
