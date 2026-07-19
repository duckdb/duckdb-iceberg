#include "common/iceberg_default.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "catalog/rest/api/iceberg_type.hpp"

namespace duckdb {

IcebergDefaultBinder::IcebergDefaultBinder(ClientContext &context)
    : context(context), binder(Binder::CreateBinder(context)), constant_binder(*binder, context, "DEFAULT") {
}

Value IcebergDefaultBinder::Evaluate(optional_ptr<const ParsedExpression> expr, const LogicalType &type) {
	if (!expr) {
		return Value(type);
	}
	auto expr_copy = expr->Copy();
	auto bound_expr = constant_binder.Bind(expr_copy, nullptr);
	auto type_id = type.id();
	switch (type_id) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::VARIANT:
	// case LogicalTypeId::GEOGRAPHY:
	case LogicalTypeId::GEOMETRY: {
		if (bound_expr->GetReturnType().id() != LogicalTypeId::SQLNULL) {
			//! SPEC: All columns of unknown, variant, geometry, and geography types must default to null. Non-null
			//! values for initial-default or write-default are invalid.
			throw InvalidInputException("Non-null DEFAULT values are not accepted for columns of type %s",
			                            IcebergTypeHelper::LogicalTypeToIcebergType(type));
		}
		break;
	}
	default:
		break;
	};

	if (!bound_expr->IsFoldable()) {
		throw NotImplementedException("Only foldable expressions are allowed as DEFAULT values");
	}
	return ExpressionExecutor::EvaluateScalar(context, *bound_expr, false).DefaultCastAs(type);
}

namespace {

//! Used to determine if the field of a struct is mapped or not
struct StructFieldMapping {
	case_insensitive_map_t<unique_ptr<StructFieldMapping>> child_mapping;
};

static Value CreateStructMapping(const LogicalType &struct_type, const string &name,
                                 case_insensitive_map_t<unique_ptr<StructFieldMapping>> &out_mapping) {
	child_list_t<Value> field_mapping;

	auto &struct_children = StructType::GetChildTypes(struct_type);
	for (auto &[field_name, field_type] : struct_children) {
		auto &child_mapping = out_mapping[field_name.GetIdentifierName()];
		if (!child_mapping) {
			child_mapping = make_uniq<StructFieldMapping>();
		}
		Value mapping;
		if (field_type.id() == LogicalTypeId::STRUCT) {
			mapping = CreateStructMapping(field_type, field_name.GetIdentifierName(), child_mapping->child_mapping);
		} else {
			mapping = Value(field_name);
		}
		field_mapping.emplace_back(field_name, mapping);
	}
	auto struct_value = Value::STRUCT(field_mapping);
	if (name.empty()) {
		//! Root column
		return struct_value;
	}
	return Value::TUPLE({Value(name), struct_value});
}

static Value CreateStructDefault(const Value &value,
                                 const case_insensitive_map_t<unique_ptr<StructFieldMapping>> &mapping = {}) {
	child_list_t<Value> field_defaults;
	auto &field_values = StructValue::GetChildren(value);
	auto &struct_children = StructType::GetChildTypes(value.type());
	for (idx_t j = 0; j < field_values.size(); j++) {
		auto &field_name = struct_children[j].first;
		auto &field_type = struct_children[j].second;
		auto &field_value = field_values[j];

		auto it = mapping.find(field_name.GetIdentifierName());
		const bool is_mapped = it != mapping.end();

		Value field_default;
		if (field_type.id() == LogicalTypeId::STRUCT) {
			if (is_mapped) {
				field_default = CreateStructDefault(field_value, it->second->child_mapping);
			} else {
				field_default = CreateStructDefault(field_value);
			}

			if (field_default.IsNull()) {
				//! All fields were skipped, no need to include this value
				continue;
			}
		} else {
			if (is_mapped) {
				continue;
			}
			field_default = field_value;
		}

		field_defaults.emplace_back(field_name, field_default);
	}
	if (field_defaults.empty()) {
		//! Skipped all fields, signal that the value should be omitted
		return Value();
	}
	return Value::STRUCT(field_defaults);
}

static Value EvaluateStructDefault(ClientContext &context, const Expression &default_expr) {
	if (!default_expr.IsFoldable()) {
		throw BinderException("Cannot resolve partial STRUCT insert with non-constant default value");
	}
	Value default_value;
	if (!ExpressionExecutor::TryEvaluateScalar(context, default_expr, default_value)) {
		throw BinderException("Cannot resolve partial STRUCT insert with non-constant default value");
	}
	return default_value;
}

} // namespace

unique_ptr<Expression> IcebergDefaultProjectionResolver::ResolveDefault(ClientContext &context,
                                                                        const LogicalType &input_type,
                                                                        const LogicalType &result_type,
                                                                        ColumnBinding binding,
                                                                        const Expression &default_expr) {
	auto default_value = EvaluateStructDefault(context, default_expr);
	if (default_value.IsNull() || input_type.id() != LogicalTypeId::STRUCT ||
	    result_type.id() != LogicalTypeId::STRUCT) {
		return make_uniq<BoundColumnRefExpression>(input_type, binding);
	}

	// Column is of type STRUCT, create a remap that fills in omitted fields from the column default.
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundColumnRefExpression>(input_type, binding));
	children.push_back(make_uniq<BoundConstantExpression>(Value(result_type)));

	case_insensitive_map_t<unique_ptr<StructFieldMapping>> mapping;
	children.push_back(make_uniq<BoundConstantExpression>(CreateStructMapping(input_type, "", mapping)));
	children.push_back(make_uniq<BoundConstantExpression>(CreateStructDefault(default_value, mapping)));
	return RemapStructFun::GetFunction().Bind(context, std::move(children));
}

} // namespace duckdb
