#include "core/expression/iceberg_transform.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

#include "utf8proc_wrapper.hpp"
#include "core/expression/iceberg_hash.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "core/metadata/schema/iceberg_column_definition.hpp"

namespace duckdb {

IcebergTransform::IcebergTransform() : raw_transform() {
	type = IcebergTransformType::INVALID;
}

bool IcebergTransform::TransformFunctionSupported(const string &transform_name) {
	if (transform_name == "day" || transform_name == "month" || transform_name == "year" || transform_name == "hour" ||
	    transform_name == "truncate" || transform_name == "bucket") {
		return true;
	}
	return false;
}

IcebergTransform IcebergTransform::FromExpression(const ParsedExpression &expr, const IcebergTableSchema &schema,
                                                  vector<reference<const IcebergColumnDefinition>> &source_columns) {
	auto expr_type = expr.GetExpressionType();
	switch (expr_type) {
	case ExpressionType::COLUMN_REF: {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto column_lookup = schema.GetFromPath(colref.ColumnNames(), nullptr);
		if (!column_lookup) {
			auto column_name = StringUtil::Join(colref.ColumnNames(), ".");
			throw InvalidInputException("No column by the name '%s' exists in the current schema (id: %d)", column_name,
			                            schema.schema_id);
		}
		source_columns.push_back(*column_lookup);
		return IcebergTransform("identity");
	}
	case ExpressionType::STRUCT_EXTRACT: {
		auto &struct_extract = expr.Cast<OperatorExpression>();
		auto &children = struct_extract.GetChildren();
		if (children.size() != 2) {
			break;
		}
		auto &base = children[0];
		auto &remaining_fields = children[1];
		if (base->GetExpressionType() != ExpressionType::COLUMN_REF) {
			break;
		}
		if (remaining_fields->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			break;
		}
		auto &base_column = base->Cast<ColumnRefExpression>();
		auto &fields_constant = remaining_fields->Cast<ConstantExpression>();
		if (fields_constant.GetValue().type().id() != LogicalTypeId::VARCHAR) {
			break;
		}
		auto fields = base_column.ColumnNames();
		auto parsed_fields = QualifiedName::ParseComponents(fields_constant.GetValue().GetValue<string>());
		for (auto &parsed_field : parsed_fields) {
			fields.push_back(std::move(parsed_field));
		}
		auto column_lookup = schema.GetFromPath(fields, nullptr);
		if (!column_lookup) {
			auto column_name = StringUtil::Join(fields, ".");
			throw InvalidInputException("No column by the name '%s' exists in the current schema (id: %d)", column_name,
			                            schema.schema_id);
		}
		source_columns.push_back(*column_lookup);
		return IcebergTransform("identity");
	}
	case ExpressionType::FUNCTION: {
		auto &funcexpr = expr.Cast<FunctionExpression>();
		auto transform = funcexpr.FunctionName().GetIdentifierName();
		if (funcexpr.GetArguments().empty()) {
			throw NotImplementedException("Unrecognized transform ('%s')", transform);
		} else if (!IcebergTransform::TransformFunctionSupported(transform)) {
			throw NotImplementedException("Unrecognized transform ('%s')", transform);
		}

		auto &arguments = funcexpr.GetArguments();
		idx_t source_columns_offset = 0;
		idx_t constant_value;
		if (transform == "bucket" || transform == "truncate") {
			// Spark-compatible syntax: bucket(N, col) / truncate(W, col)
			if (arguments.size() < 2) {
				throw InvalidInputException("%s requires two arguments, e.g. %s(16, col)", transform, transform);
			}
			auto &param_expr = arguments[0].GetExpression();
			if (param_expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
				throw InvalidInputException("%s first argument must be a constant integer", transform);
			}
			auto &const_expr = param_expr.Cast<ConstantExpression>();
			auto raw_val = const_expr.GetValue().GetValue<int32_t>();
			if (raw_val <= 0) {
				throw InvalidInputException("%s requires a positive integer argument, got %d", transform, raw_val);
			}
			constant_value = const_expr.GetValue().GetValue<idx_t>();
			transform = StringUtil::Format("%s[%d]", transform, constant_value);
			//! Source columns start behind the constant argument
			source_columns_offset = 1;
		}

		//! Figure out the source id(s) of the transforms
		for (idx_t i = source_columns_offset; i < arguments.size(); i++) {
			auto &func_expr = arguments[i].GetExpression();
			auto tmp = FromExpression(func_expr, schema, source_columns);
			if (tmp.Type() != IcebergTransformType::IDENTITY) {
				throw InvalidInputException("Encountered a non-column source for the transform (%s)",
				                            func_expr.ToString());
			}
		}

		auto res = IcebergTransform(transform);
		if (res.Type() == IcebergTransformType::BUCKET || res.Type() == IcebergTransformType::TRUNCATE) {
			res.SetBucketOrTruncateValue(constant_value);
		}
		return res;
	}
	default:
		break;
	}
	throw NotImplementedException("Unsupported partition key type: %s", expr.ToString());
}

IcebergTransform::IcebergTransform(const string &transform) : raw_transform(transform) {
	if (transform == "identity") {
		type = IcebergTransformType::IDENTITY;
	} else if (StringUtil::StartsWith(transform, "bucket")) {
		type = IcebergTransformType::BUCKET;
		D_ASSERT(transform[6] == '[');
		D_ASSERT(transform.back() == ']');
		auto start = transform.find('[');
		auto end = transform.rfind(']');
		auto digits = transform.substr(start + 1, end - start);
		modulo = std::stoi(digits);
	} else if (StringUtil::StartsWith(transform, "truncate")) {
		type = IcebergTransformType::TRUNCATE;
		D_ASSERT(transform[8] == '[');
		D_ASSERT(transform.back() == ']');
		auto start = transform.find('[');
		auto end = transform.rfind(']');
		auto digits = transform.substr(start + 1, end - start);
		width = std::stoi(digits);
	} else if (transform == "year") {
		type = IcebergTransformType::YEAR;
	} else if (transform == "month") {
		type = IcebergTransformType::MONTH;
	} else if (transform == "day") {
		type = IcebergTransformType::DAY;
	} else if (transform == "hour") {
		type = IcebergTransformType::HOUR;
	} else if (transform == "void") {
		type = IcebergTransformType::VOID;
	} else {
		throw NotImplementedException("Unrecognized transform ('%s')", transform);
	}
}

LogicalType IcebergTransform::GetBoundsType(const LogicalType &input) const {
	switch (type) {
	case IcebergTransformType::IDENTITY: {
		//! Appendix A: Avro Data Type Mappings
		//! The avro reader return will return the correct identity types now
		return input;
	}
	case IcebergTransformType::BUCKET:
		return LogicalType::INTEGER;
	case IcebergTransformType::TRUNCATE:
		return input;
	case IcebergTransformType::DAY:
	case IcebergTransformType::YEAR:
	case IcebergTransformType::MONTH:
	case IcebergTransformType::HOUR:
		return LogicalType::INTEGER;
	case IcebergTransformType::VOID:
		return input;
	default:
		throw InvalidConfigurationException("Can't produce a result type for transform %s and input type %s",
		                                    raw_transform, input.ToString());
	}
}

LogicalType IcebergTransform::GetSerializedType(const LogicalType &input) const {
	switch (type) {
	case IcebergTransformType::IDENTITY:
		return input;
	case IcebergTransformType::TRUNCATE:
		return input;
	case IcebergTransformType::BUCKET:
	case IcebergTransformType::YEAR:
	case IcebergTransformType::MONTH:
	case IcebergTransformType::HOUR:
	case IcebergTransformType::DAY:
		return LogicalType::INTEGER;
	case IcebergTransformType::VOID:
		return input;
	default:
		throw InvalidConfigurationException("Can't produce a result type for transform %s and input type %s",
		                                    raw_transform, input.ToString());
	}
}

Value IcebergTransform::ApplyTransform(const Value &value) const {
	if (value.IsNull()) {
		return Value(GetSerializedType(value.type()));
	}

	switch (type) {
	case IcebergTransformType::IDENTITY:
		return IdentityTransform::ApplyTransform(value, *this);
	case IcebergTransformType::BUCKET:
		return BucketTransform::ApplyTransform(value, *this);
	case IcebergTransformType::TRUNCATE:
		return TruncateTransform::ApplyTransform(value, *this);
	case IcebergTransformType::YEAR:
		return YearTransform::ApplyTransform(value, *this);
	case IcebergTransformType::MONTH:
		return MonthTransform::ApplyTransform(value, *this);
	case IcebergTransformType::DAY:
		return DayTransform::ApplyTransform(value, *this);
	case IcebergTransformType::HOUR:
		return HourTransform::ApplyTransform(value, *this);
	case IcebergTransformType::VOID:
		return Value(GetSerializedType(value.type()));
	default:
		throw InvalidConfigurationException("Transform '%s' not implemented", RawType());
	}
}

string IcebergTransform::PartitionValueToString(const Value &partition_value) const {
	if (partition_value.IsNull()) {
		return "NULL";
	}
	switch (type) {
	case IcebergTransformType::DAY: {
		date_t d;
		d.days = partition_value.GetValue<int32_t>();
		return Date::ToString(d);
	}
	case IcebergTransformType::MONTH: {
		int32_t m = partition_value.GetValue<int32_t>();
		// Floor-divide to correctly handle months before 1970
		int32_t year = 1970 + (m >= 0 ? m : m - 11) / 12;
		int32_t month = ((m % 12) + 12) % 12 + 1;
		return StringUtil::Format("%04d-%02d", year, month);
	}
	case IcebergTransformType::YEAR: {
		return std::to_string(1970 + partition_value.GetValue<int32_t>());
	}
	case IcebergTransformType::HOUR: {
		int64_t hours = partition_value.GetValue<int32_t>();
		timestamp_t ts(hours * Interval::MICROS_PER_HOUR);
		return Timestamp::ToString(ts);
	}
	default:
		return partition_value.ToString();
	}
}

void IcebergTransform::SetBucketOrTruncateValue(idx_t value) {
	switch (type) {
	case IcebergTransformType::BUCKET:
		modulo = value;
		return;
	case IcebergTransformType::TRUNCATE:
		width = value;
		return;
	default:
		throw InvalidInputException("Cannot set bucket or modulo value for transform '%s'", raw_transform);
	}
}

Value BucketTransform::ApplyTransform(const Value &constant, const IcebergTransform &transform) {
	return IcebergHash::BucketValue(constant, static_cast<int32_t>(transform.GetBucketModulo()));
}

Value TruncateTransform::ApplyTransform(const Value &constant, const IcebergTransform &transform) {
	if (constant.IsNull()) {
		// Iceberg spec: "All transforms must return null for a null input value"
		return Value(constant.type());
	}
	return IcebergHash::TruncateValue(constant, transform.GetTruncateWidth());
}
} // namespace duckdb
