#include "common/iceberg_default.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "catalog/rest/api/iceberg_type.hpp"

namespace duckdb {

// This compatibility switch only exists to exercise empty STRUCT defaults in tests.
bool &IcebergDefault::InterpretStructNullAsEmpty() {
	static bool value = false;
	return value;
}

IcebergDefaultBinder::IcebergDefaultBinder(ClientContext &context)
    : context(context), binder(Binder::CreateBinder(context)), constant_binder(*binder, context, "DEFAULT") {
}

Value IcebergDefaultBinder::Evaluate(optional_ptr<const ParsedExpression> expr, const LogicalType &type) {
	if (!expr) {
		return Value(type);
	}
	auto expr_copy = expr->Copy();
	auto bound_expr = constant_binder.Bind(expr_copy, nullptr);

	if (!bound_expr->IsFoldable()) {
		throw NotImplementedException("Only foldable expressions are allowed as DEFAULT values");
	}
	auto val = ExpressionExecutor::EvaluateScalar(context, *bound_expr, false);

	auto type_id = type.id();
	switch (type_id) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::VARIANT:
	// case LogicalTypeId::GEOGRAPHY:
	case LogicalTypeId::GEOMETRY: {
		if (!val.IsNull()) {
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
	return val.CastAs(context, type);
}

namespace {

//! The fields supplied for a STRUCT
struct StructFieldMapping {
	case_insensitive_map_t<unique_ptr<StructFieldMapping>> child_mapping;
};

static Value CreateStructMapping(const LogicalType &struct_type, const string &name,
                                 case_insensitive_map_t<unique_ptr<StructFieldMapping>> &out_mapping) {
	child_list_t<Value> field_mapping;

	auto &struct_children = StructType::GetChildTypes(struct_type);
	for (auto &[field_name, field_type] : struct_children) {
		//! Only a field supplied as a STRUCT gets a node for its own fields
		auto &child_mapping = out_mapping[field_name.GetIdentifierName()];
		Value mapping;
		if (field_type.id() == LogicalTypeId::STRUCT) {
			if (!child_mapping) {
				child_mapping = make_uniq<StructFieldMapping>();
			}
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

static bool IsStructDefaultDescriptor(const Value &value) {
	if (value.type().id() != LogicalTypeId::STRUCT) {
		return false;
	}
	// Descriptors are internal typed values with two deliberately named fields. They are not valid user-facing
	// STRUCT default syntax and are never written to Iceberg metadata.
	auto &children = StructType::GetChildTypes(value.type());
	return children.size() == 2 && children[0].first == "struct_default" && children[1].first == "field_defaults";
}

static const Value &GetStructDefault(const Value &descriptor) {
	D_ASSERT(IsStructDefaultDescriptor(descriptor));
	return StructValue::GetChildren(descriptor)[0];
}

static const Value &GetStructFieldDefaults(const Value &descriptor) {
	D_ASSERT(IsStructDefaultDescriptor(descriptor));
	return StructValue::GetChildren(descriptor)[1];
}

static Value CreateStructDefault(const Value &descriptor,
                                 const case_insensitive_map_t<unique_ptr<StructFieldMapping>> &mapping = {}) {
	D_ASSERT(IsStructDefaultDescriptor(descriptor));
	child_list_t<Value> field_defaults;
	auto &field_defaults_value = GetStructFieldDefaults(descriptor);
	auto &field_values = StructValue::GetChildren(field_defaults_value);
	auto &struct_children = StructType::GetChildTypes(field_defaults_value.type());
	for (idx_t j = 0; j < field_values.size(); j++) {
		auto &field_name = struct_children[j].first;
		auto &field_value = field_values[j];

		auto it = mapping.find(field_name.GetIdentifierName());
		const bool is_mapped = it != mapping.end();

		Value field_default;
		if (IsStructDefaultDescriptor(field_value)) {
			if (is_mapped && !it->second) {
				// Supplied, but not as a STRUCT (e.g. NULL), so there are no fields to fill
				continue;
			}
			if (is_mapped) {
				// The nested STRUCT is present in the input. Recurse so that only its omitted children receive
				// field defaults.
				field_default = CreateStructDefault(field_value, it->second->child_mapping);
			} else {
				// The nested STRUCT itself is absent. Iceberg requires its whole-STRUCT default here; do not
				// synthesize a value from its child defaults.
				field_default = GetStructDefault(field_value);
			}

			if (field_default.IsNull()) {
				if (!is_mapped) {
					field_defaults.emplace_back(field_name, std::move(field_default));
				}
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

//! Whether the STRUCT or any of its fields has a non-NULL default
static bool HasNonNullDefault(const Value &descriptor) {
	if (!GetStructDefault(descriptor).IsNull()) {
		return true;
	}

	for (auto &field_default : StructValue::GetChildren(GetStructFieldDefaults(descriptor))) {
		if (IsStructDefaultDescriptor(field_default) ? HasNonNullDefault(field_default) : !field_default.IsNull()) {
			return true;
		}
	}
	return false;
}

static Value EvaluateStructDefault(ClientContext &context, const Expression &default_expr) {
	if (default_expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function_expr = default_expr.Cast<BoundFunctionExpression>();
		auto &arguments = function_expr.GetChildren();
		if (function_expr.Function().GetName() == StructExtractFun::Name && !arguments.empty() &&
		    arguments[0]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			// A STRUCT column's default: return the descriptor it carries
			auto &descriptor = arguments[0]->Cast<BoundConstantExpression>().GetValue();
			if (IsStructDefaultDescriptor(descriptor)) {
				return descriptor;
			}
		}
	}

	if (!default_expr.IsFoldable()) {
		throw BinderException("Cannot resolve partial STRUCT insert with non-constant default value");
	}

	Value default_value;
	if (!ExpressionExecutor::TryEvaluateScalar(context, default_expr, default_value)) {
		throw BinderException("Cannot resolve partial STRUCT insert with non-constant default value");
	}

	// Any other column: return its ordinary default
	return default_value;
}

} // namespace

unique_ptr<Expression> IcebergDefaultProjectionResolver::ResolveDefault(ClientContext &context,
                                                                        const LogicalType &input_type,
                                                                        const LogicalType &result_type,
                                                                        ColumnBinding binding,
                                                                        const Expression &default_expr) {
	auto default_descriptor = EvaluateStructDefault(context, default_expr);
	if (!IsStructDefaultDescriptor(default_descriptor) || !HasNonNullDefault(default_descriptor) ||
	    input_type.id() != LogicalTypeId::STRUCT || result_type.id() != LogicalTypeId::STRUCT) {
		// No defaults to fill in, so only cast the input to the column's type
		return BoundCastExpression::AddCastToType(context, make_uniq<BoundColumnRefExpression>(input_type, binding),
		                                          result_type);
	}

	// Keep the supplied fields and fill the omitted ones from their defaults. A NULL input STRUCT stays NULL.
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundColumnRefExpression>(input_type, binding));
	children.push_back(make_uniq<BoundConstantExpression>(Value(result_type)));

	case_insensitive_map_t<unique_ptr<StructFieldMapping>> mapping;
	children.push_back(make_uniq<BoundConstantExpression>(CreateStructMapping(input_type, "", mapping)));
	children.push_back(make_uniq<BoundConstantExpression>(CreateStructDefault(default_descriptor, mapping)));
	return RemapStructFun::GetFunction().Bind(context, std::move(children));
}

} // namespace duckdb
