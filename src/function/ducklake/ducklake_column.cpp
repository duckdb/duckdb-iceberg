#include "function/ducklake/ducklake_column.hpp"
#include "function/ducklake/ducklake_utils.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

DuckLakeColumn::DuckLakeColumn(const IcebergColumnDefinition &column, idx_t order,
                               optional_ptr<const IcebergColumnDefinition> parent) {
	column_id = column.id;
	if (parent) {
		parent_column = parent->id;
	}
	column_order = order;
	column_name = column.name;
	column_type = DuckLakeUtils::ToDuckLakeColumnType(column.type);
	initial_default = column.initial_default ? *column.initial_default : Value(column_type);
	if (column.write_default) {
		default_value = *column.write_default;
	} else if (column.initial_default) {
		default_value = *column.initial_default;
	} else {
		default_value = Value(column_type);
	}
	nulls_allowed = !column.required;
}

bool DuckLakeColumn::IsNested() const {
	return (column_type == "struct" || column_type == "map" || column_type == "list");
}

bool DuckLakeColumn::operator==(const DuckLakeColumn &other) {
	if (column_id != other.column_id) {
		throw InternalException("Comparison between two columns that don't share the same id is not defined");
	}
	if (column_id != other.column_order) {
		return false;
	}
	if (column_name != other.column_name) {
		return false;
	}
	if (column_type != other.column_type) {
		return false;
	}
	if (nulls_allowed != other.nulls_allowed) {
		return false;
	}
	if (default_value != other.default_value) {
		return false;
	}
	if (initial_default != other.initial_default) {
		return false;
	}
	if (default_value != other.default_value) {
		return false;
	}
	return true;
}

bool DuckLakeColumn::operator!=(const DuckLakeColumn &other) {
	return !(*this == other);
}

string DuckLakeColumn::FinalizeEntry(int64_t table_id, const map<timestamp_t, DuckLakeSnapshot> &snapshots) {
	auto snapshot_ids = DuckLakeUtils::GetSnapshots(start_snapshot, has_end, end_snapshot, snapshots);
	string parent_column = this->parent_column.IsValid() ? to_string(this->parent_column.GetIndex()) : "NULL";
	auto initial_default = this->initial_default.IsNull() ? "NULL" : "'" + this->initial_default.ToString() + "'";
	auto default_value = this->default_value.IsNull() ? "NULL" : "'" + this->default_value.ToString() + "'";
	auto nulls_allowed = this->nulls_allowed ? "true" : "false";

	const auto COLUMN_SQL = R"(
		INSERT INTO {METADATA_CATALOG}.ducklake_column VALUES
			(
				%d, -- column_id
				%d, -- begin_snapshot
				%s, -- end_snapshot
				%d, -- table_id
				%d, -- column_order
				'%s', -- column_name
				'%s', -- column_type
				%s, -- initial_default
				%s, -- default_value
				%s, -- nulls_allowed
				%s, -- parent_column
				'%s', -- default_value_type
				'%s' -- default_value_dialect
			);
	)";

	return StringUtil::Format(COLUMN_SQL,
	                          //! column_id
	                          column_id,
	                          //! begin_snapshot
	                          snapshot_ids.first,
	                          //! end_snapshot
	                          snapshot_ids.second,
	                          //! table_id
	                          table_id,
	                          //! column_order
	                          column_order,
	                          //! column_name
	                          column_name,
	                          //! column_type
	                          column_type,
	                          //! initial_default
	                          initial_default,
	                          //! default_value
	                          default_value,
	                          //! nulls_allowed
	                          nulls_allowed,
	                          //! parent_column
	                          parent_column,
	                          //! default_value_type
	                          "literal",
	                          //! default_value_dialect
	                          "duckdb");
}

} // namespace ducklake

} // namespace iceberg

} // namespace duckdb
