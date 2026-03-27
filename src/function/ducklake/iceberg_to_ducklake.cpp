#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include "function/iceberg_functions.hpp"
#include "common/iceberg_utils.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/catalog_entry/iceberg_schema_entry.hpp"
#include "catalog/rest/iceberg_schema_set.hpp"
#include "catalog/rest/iceberg_table_set.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"

#include "function/ducklake/ducklake_column_stats.hpp"
#include "function/ducklake/ducklake_column.hpp"
#include "function/ducklake/ducklake_data_file.hpp"
#include "function/ducklake/ducklake_delete_file.hpp"
#include "function/ducklake/ducklake_metadata_serializer.hpp"
#include "function/ducklake/ducklake_partition_column.hpp"
#include "function/ducklake/ducklake_partition.hpp"
#include "function/ducklake/ducklake_schema.hpp"
#include "function/ducklake/ducklake_snapshot.hpp"
#include "function/ducklake/ducklake_table.hpp"
#include "function/ducklake/ducklake_utils.hpp"

namespace duckdb {

namespace iceberg {

namespace ducklake {

static void SchemaToColumnsInternal(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                    unordered_map<int64_t, DuckLakeColumn> &result,
                                    optional_ptr<const IcebergColumnDefinition> parent) {
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &column = *columns[i];
		result.emplace(column.id, DuckLakeColumn(column, i, parent));
		if (column.children.empty()) {
			continue;
		}
		auto &children = column.children;
		SchemaToColumnsInternal(children, result, column);
	}
}

static unordered_map<int64_t, DuckLakeColumn> SchemaToColumns(const IcebergTableSchema &schema) {
	unordered_map<int64_t, DuckLakeColumn> result;
	SchemaToColumnsInternal(schema.columns, result, nullptr);
	return result;
}

static string GetNumericStats(const unordered_map<int32_t, int64_t> &stats, int64_t column_id) {
	auto it = stats.find(column_id);
	if (it == stats.end()) {
		return "NULL";
	}
	return to_string(it->second);
}

struct IcebergToDuckLakeBindData : public TableFunctionData {
public:
	IcebergToDuckLakeBindData() {
	}

public:
	void AddTable(IcebergTableInformation &table_info, ClientContext &context, const IcebergOptions &options) {
		auto &metadata = table_info.table_metadata;
		if (table_names_to_skip.count(table_info.name)) {
			//! FIXME: perhaps log that the table was skipped
			return;
		}
		if (metadata.snapshots.empty()) {
			//! The table has no snapshots, so we can't assign any ducklake snapshot as its creator
			return;
		}

		map<timestamp_t, reference<IcebergSnapshot>> snapshots;
		for (auto &it : metadata.snapshots) {
			snapshots.emplace(it.second.timestamp_ms, it.second);
		}

		auto &schema_entry = table_info.schema;
		auto &schema = GetSchema(schema_entry.name);

		auto &table = GetTable(table_info);
		schema.tables.push_back(table.table_uuid);
		table.schema_name = schema.schema_name;

		//! Current schema state
		optional_ptr<IcebergTableSchema> last_schema;

		//! Current partition state
		optional_idx current_partition_spec_id;
		optional_ptr<DuckLakePartition> current_partition;

		for (auto &it : snapshots) {
			auto &snapshot = it.second.get();
			auto &ducklake_snapshot = GetSnapshot(it.first);

			if (!table.has_snapshot) {
				//! Mark the table as being created by this snapshot
				table.catalog_id_offset = ducklake_snapshot.AddTable(table_info.table_metadata.table_uuid);
				table.start_snapshot = ducklake_snapshot.snapshot_time;
				table.has_snapshot = true;
			}

			//! Process the schema changes
			auto &current_schema = *metadata.GetSchemaFromId(snapshot.schema_id);
			auto current_columns = SchemaToColumns(current_schema);
			vector<DuckLakeColumn> added_columns;
			vector<int64_t> dropped_columns;
			if (last_schema) {
				if (last_schema->schema_id != current_schema.schema_id) {
					auto existing_columns = SchemaToColumns(*last_schema);

					vector<reference<DuckLakeColumn>> new_columns;
					for (auto &it : current_columns) {
						auto existing_it = existing_columns.find(it.first);
						if (existing_it == existing_columns.end()) {
							//! This column is entirely new
							added_columns.push_back(it.second);
						}
						auto &existing_column = it.second;
						if (existing_column != it.second) {
							//! This column has been changed in the new schema
							added_columns.push_back(it.second);
						}
					}
					for (auto &it : existing_columns) {
						if (!current_columns.count(it.first)) {
							//! This column is dropped in the new schema
							dropped_columns.push_back(it.first);
						}
					}
				}
			} else {
				for (auto &it : current_columns) {
					added_columns.push_back(it.second);
				}
			}

			for (auto &column : added_columns) {
				table.AddColumnVersion(column, ducklake_snapshot);
			}
			for (auto id : dropped_columns) {
				table.DropColumnVersion(id, ducklake_snapshot);
			}
			last_schema = current_schema;

			auto iceberg_manifest_list =
			    IcebergManifestList::Load(metadata.location, metadata, snapshot, context, options);

			vector<DuckLakeDataFile> new_data_files;
			vector<string> deleted_data_files;

			vector<DuckLakeDeleteFile> new_delete_files;
			vector<string> deleted_delete_files;
			for (auto &entry : iceberg_manifest_list->GetManifestFilesConst()) {
				auto &manifest = entry.file;
				auto &entries = entry.manifest_entries;

				if (manifest.added_snapshot_id != snapshot.snapshot_id) {
					//! This is essentially an "EXISTING" manifest
					//! there just isn't a 'status' field to indicate that
					continue;
				}

				if (!current_partition_spec_id.IsValid() ||
				    static_cast<idx_t>(manifest.partition_spec_id) > current_partition_spec_id.GetIndex()) {
					auto &partition_spec = *metadata.FindPartitionSpecById(manifest.partition_spec_id);
					auto new_partition = make_uniq<DuckLakePartition>(partition_spec);
					current_partition = table.AddPartition(std::move(new_partition), ducklake_snapshot);
					current_partition_spec_id = manifest.partition_spec_id;
				}

				switch (manifest.content) {
				case IcebergManifestContentType::DATA: {
					for (auto &manifest_entry : entries) {
						auto &data_file = manifest_entry.data_file;
						D_ASSERT(data_file.content == IcebergManifestEntryContentType::DATA);
						if (manifest_entry.status == IcebergManifestEntryStatusType::EXISTING) {
							//! We don't care about existing entries
							continue;
						}
						if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
							new_data_files.push_back(
							    DuckLakeDataFile(manifest_entry, *current_partition, table.table_name));
						} else {
							D_ASSERT(manifest_entry.status == IcebergManifestEntryStatusType::DELETED);
							deleted_data_files.push_back(data_file.file_path);
						}
					}
					break;
				}
				case IcebergManifestContentType::DELETE: {
					for (auto &manifest_entry : entries) {
						auto &data_file = manifest_entry.data_file;
						if (data_file.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
							throw InvalidInputException(
							    "Can't convert a table with equality deletes to a DuckLake table");
						}
						if (manifest_entry.status == IcebergManifestEntryStatusType::EXISTING) {
							//! We don't care about existing entries
							continue;
						}
						if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
							new_delete_files.push_back(DuckLakeDeleteFile(manifest_entry, table.table_name));
						} else {
							D_ASSERT(manifest_entry.status == IcebergManifestEntryStatusType::DELETED);
							deleted_delete_files.push_back(data_file.file_path);
						}
					}
					break;
				}
				}
			}

			//! Process changes to delete files
			for (auto &delete_file : new_delete_files) {
				table.AddDeleteFile(delete_file, ducklake_snapshot);
			}
			for (auto &path : deleted_delete_files) {
				table.DeleteDeleteFile(path, ducklake_snapshot);
			}

			//! Process changes to data files
			for (auto &data_file : new_data_files) {
				table.AddDataFile(data_file, ducklake_snapshot);
			}
			for (auto &path : deleted_data_files) {
				table.DeleteDataFile(path, ducklake_snapshot);
			}
		}
	}
	void AssignSchemaBeginSnapshots() {
		//! Figure out in which snapshot the schemas were created
		for (auto &it : schemas) {
			auto &schema = it.second;
			if (schema.tables.empty()) {
				//! We can't serialize this, we have no clue when it was added
				continue;
			}
			schema.AssignEarliestSnapshot(tables, snapshots);
		}
	}

public:
	vector<string> CreateSQLStatements() {
		//! Order to process in:
		// - snapshot + schema_versions
		// - schema
		// - table
		//   - partition_info
		//     - partition_column
		//   - data_file
		//     - file_column_statistics
		//     - file_partition_value
		//   - delete_file
		// - table_stats
		// - snapshot_changes

		DuckLakeMetadataSerializer serializer;
		vector<string> sql;

		sql.push_back("BEGIN TRANSACTION;");
		sql.push_back("DELETE FROM {METADATA_CATALOG}.ducklake_table;");
		sql.push_back("DELETE FROM {METADATA_CATALOG}.ducklake_snapshot;");
		sql.push_back("DELETE FROM {METADATA_CATALOG}.ducklake_snapshot_changes;");

		//! ducklake_snapshot
		for (auto &it : snapshots) {
			auto &snapshot = it.second;

			auto values = snapshot.FinalizeEntry(serializer);
			if (snapshot.catalog_changes) {
				auto snapshot_id = snapshot.snapshot_id;
				auto schema_version = snapshot.base_schema_version;
				sql.push_back(
				    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_schema_versions VALUES (%llu, %llu);",
				                       snapshot_id.GetIndex(), schema_version));
			}
			sql.push_back(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_snapshot %s", values));
		}

		//! ducklake_schema
		for (auto &it : schemas) {
			auto &schema = it.second;

			if (schema.tables.empty()) {
				//! We can't serialize this schema, it has no entries, so we can't date it back to any snapshot
				//! FIXME: we *could* assign it to the earliest snapshot in existence???
				continue;
			}
			auto values = schema.FinalizeEntry(snapshots);
			sql.push_back(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_schema %s", values));
		}

		//! ducklake_table
		for (auto &it : tables) {
			auto &table = it.second;

			auto &schema = schemas.at(table.schema_name);
			auto schema_id = schema.schema_id.GetIndex();
			auto values = table.FinalizeEntry(schema_id, snapshots);
			sql.push_back(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table %s", values));

			int64_t table_id = table.table_id.GetIndex();
			//! ducklake_partition_info
			for (auto &partition : table.all_partitions) {
				auto values = partition->FinalizeEntry(table_id, serializer, snapshots);
				sql.push_back(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_partition_info %s", values));
				D_ASSERT(partition->partition_id.IsValid());
				auto partition_id = partition->partition_id.GetIndex();
				//! ducklake_partition_column
				for (idx_t i = 0; i < partition->columns.size(); i++) {
					auto &column = partition->columns[i];
					auto values = column.FinalizeEntry(table_id, partition_id, i);
					sql.push_back(
					    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_partition_column %s", values));
				}
			}

			//! ducklake_column
			for (auto &column : table.all_columns) {
				auto values = column.FinalizeEntry(table_id, snapshots);
				sql.push_back(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_column %s", values));
			}

			unordered_map<int32_t, DuckLakeColumnStats> column_stats;

			//! ducklake_data_file
			for (auto &data_file : table.all_data_files) {
				auto values = data_file.FinalizeEntry(table_id, snapshots);
				sql.push_back(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_data_file %s", values));

				auto data_file_id = data_file.data_file_id.GetIndex();
				auto &start_snapshot = snapshots.at(data_file.start_snapshot);

				//! ducklake_file_column_stats
				auto columns = table.GetColumnsAtSnapshot(start_snapshot);
				for (auto &it : columns) {
					auto column_id = it.first;
					auto &column = it.second.get();
					auto &manifest_entry = data_file.manifest_entry;
					auto &iceberg_data_file = manifest_entry.data_file;

					auto column_size_bytes = GetNumericStats(iceberg_data_file.column_sizes, column_id);
					auto value_count = GetNumericStats(iceberg_data_file.value_counts, column_id);

					Value lower_bound;
					Value upper_bound;
					Value null_count;

					auto lower_bound_it = iceberg_data_file.lower_bounds.find(column.column_id);
					auto upper_bound_it = iceberg_data_file.upper_bounds.find(column.column_id);
					if (lower_bound_it != iceberg_data_file.lower_bounds.end()) {
						lower_bound = lower_bound_it->second;
					}
					if (upper_bound_it != iceberg_data_file.upper_bounds.end()) {
						upper_bound = upper_bound_it->second;
					}

					LogicalType logical_type;
					if (!column.IsNested()) {
						logical_type = DuckLakeUtils::FromStringBaseType(column.column_type);
					} else {
						logical_type = LogicalType::VARCHAR;
					}

					//! Transform the stats stored in the iceberg metadata
					auto stats = IcebergPredicateStats::DeserializeBounds(lower_bound, upper_bound, column.column_name,
					                                                      logical_type);
					auto null_counts_it = iceberg_data_file.null_value_counts.find(column.column_id);
					if (null_counts_it != iceberg_data_file.null_value_counts.end()) {
						null_count = null_counts_it->second;
						stats.has_null = null_count != 0;
					}
					auto nan_counts_it = iceberg_data_file.nan_value_counts.find(column.column_id);
					if (nan_counts_it != iceberg_data_file.nan_value_counts.end()) {
						auto &nan_count = nan_counts_it->second;
						stats.has_nan = nan_count != 0;
					}

					auto contains_nan = stats.has_nan ? "true" : "false";
					auto min_value = stats.lower_bound.IsNull() ? "NULL" : "'" + stats.lower_bound.ToString() + "'";
					auto max_value = stats.upper_bound.IsNull() ? "NULL" : "'" + stats.upper_bound.ToString() + "'";
					auto values = StringUtil::Format("VALUES(%d, %d, %d, %s, %s, %s, %s, %s, %s, NULL);", data_file_id,
					                                 table_id, column_id, column_size_bytes, value_count,
					                                 null_count.ToString(), min_value, max_value, contains_nan);
					sql.push_back(
					    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_file_column_stats %s", values));

					if (!data_file.has_end && !column.has_end && !column.IsNested()) {
						//! This data file is currently active, collect stats for it
						auto file_stats_it = column_stats.find(column_id);
						if (file_stats_it == column_stats.end()) {
							file_stats_it = column_stats.emplace(column_id, column).first;
						}
						auto &file_column_stats = file_stats_it->second;
						file_column_stats.AddStats(stats);
					}
				}

				//! ducklake_file_partition_value
				auto &partition_info = data_file.manifest_entry.data_file.partition_info;
				auto &partition = data_file.partition;

				// Build a map from partition_field_id to DataFilePartitionInfo for quick lookup
				unordered_map<uint64_t, reference<const DataFilePartitionInfo>> field_id_to_info;
				for (auto &pi : partition_info) {
					field_id_to_info.emplace(pi.field_id, pi);
				}

				for (idx_t partition_key_index = 0; partition_key_index < partition.columns.size();
				     partition_key_index++) {
					auto &partition_column = partition.columns[partition_key_index];

					auto partition_it = field_id_to_info.find(partition_column.partition_field_id);
					string partition_value;
					if (partition_it == field_id_to_info.end()) {
						partition_value = "NULL";
					} else {
						partition_value = "'" + partition_it->second.get().value.ToString() + "'";
					}
					auto values = StringUtil::Format("VALUES(%d, %d, %d, %s);", data_file_id, table_id,
					                                 partition_key_index, partition_value);
					sql.push_back(
					    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_file_partition_value %s", values));
				}
			}

			//! ducklake_delete_file
			for (auto &delete_file : table.all_delete_files) {
				auto values = delete_file.FinalizeEntry(table_id, table.all_data_files, snapshots);
				sql.push_back(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_delete_file %s", values));
			}

			//! ducklake_table_stats
			idx_t record_count = 0;
			idx_t file_size_bytes = 0;

			for (auto &it : table.current_data_files) {
				auto &data_file = table.all_data_files[it.second];

				record_count += data_file.record_count;
				file_size_bytes += data_file.file_size_bytes;
			}
			for (auto &it : table.current_delete_files) {
				auto &delete_file = table.all_delete_files[it.second];

				record_count -= delete_file.record_count;
				auto &data_file = table.all_data_files[delete_file.referenced_data_file];
				D_ASSERT(!data_file.has_end);
				auto percent_deleted = double(delete_file.record_count) / (data_file.record_count / 100.00);
				file_size_bytes -= LossyNumericCast<idx_t>(double(data_file.file_size_bytes) / percent_deleted);
			}

			if (!column_stats.empty()) {
				//! FIXME: for v2 compatibility this uses the 'record_count' as the 'next_row_id'
				auto stats_values = StringUtil::Format("VALUES(%d, %d, %d, %d);", table_id, record_count, record_count,
				                                       file_size_bytes);
				sql.push_back(
				    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table_stats %s", stats_values));
			}

			//! ducklake_table_column_stats
			for (auto &it : column_stats) {
				auto column_id = it.first;
				auto &stats = it.second;

				auto contains_null = stats.contains_null ? "true" : "false";
				auto contains_nan = stats.contains_nan ? "true" : "false";
				auto min_value = stats.min_value.IsNull() ? "NULL" : "'" + stats.min_value.ToString() + "'";
				auto max_value = stats.max_value.IsNull() ? "NULL" : "'" + stats.max_value.ToString() + "'";
				auto values = StringUtil::Format("VALUES(%d, %d, %s, %s, %s, %s, NULL);", table_id, column_id,
				                                 contains_null, contains_nan, min_value, max_value);
				sql.push_back(
				    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats %s", values));
			}
		}

		//! ducklake_snapshot_changes
		for (auto &it : snapshots) {
			auto &snapshot = it.second;

			vector<string> changes;
			for (auto &schema_name : snapshot.created_schema) {
				auto escaped_name = KeywordHelper::WriteQuoted(schema_name, '"');

				changes.push_back(StringUtil::Format("created_schema:%s", escaped_name));
			}

			for (auto &table_uuid : snapshot.created_table) {
				auto &table = tables.at(table_uuid);

				auto schema_name = KeywordHelper::WriteQuoted(table.schema_name, '"');
				auto table_name = KeywordHelper::WriteQuoted(table.table_name, '"');

				changes.push_back(StringUtil::Format("created_table:%s.%s", schema_name, table_name));
			}

			for (auto &table_uuid : snapshot.inserted_into_table) {
				auto &table = tables.at(table_uuid);

				auto table_id = table.table_id.GetIndex();
				changes.push_back(StringUtil::Format("inserted_into_table:%d", table_id));
			}

			for (auto &table_uuid : snapshot.deleted_from_table) {
				auto &table = tables.at(table_uuid);

				auto table_id = table.table_id.GetIndex();
				changes.push_back(StringUtil::Format("deleted_from_table:%d", table_id));
			}

			for (auto &schema_name : snapshot.dropped_schema) {
				auto &schema = schemas.at(schema_name);
				auto schema_id = schema.schema_id.GetIndex();

				changes.push_back(StringUtil::Format("dropped_schema:%d", schema_id));
			}

			for (auto &table_uuid : snapshot.dropped_table) {
				auto &table = tables.at(table_uuid);

				auto table_id = table.table_id.GetIndex();
				changes.push_back(StringUtil::Format("dropped_table:%d", table_id));
			}

			for (auto &table_uuid : snapshot.altered_table) {
				if (snapshot.created_table.count(table_uuid)) {
					//! Table was created in this snapshot,
					//! any alters to the table made as part of that creation don't have to be recorded as
					//! 'snapshot_changes'
					continue;
				}
				auto &table = tables.at(table_uuid);

				auto table_id = table.table_id.GetIndex();
				changes.push_back(StringUtil::Format("altered_table:%d", table_id));
			}
			auto snapshot_id = snapshot.snapshot_id.GetIndex();
			auto values =
			    StringUtil::Format("VALUES(%d, '%s', NULL, NULL, NULL);", snapshot_id, StringUtil::Join(changes, ","));
			sql.push_back(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes %s", values));
		}
		sql.push_back("COMMIT TRANSACTION;");

		return sql;
	}

private:
	DuckLakeSnapshot &GetSnapshot(timestamp_t timestamp) {
		auto it = snapshots.find(timestamp);
		if (it != snapshots.end()) {
			return it->second;
		}
		auto res = snapshots.emplace(timestamp, DuckLakeSnapshot(timestamp));
		return res.first->second;
	}

	DuckLakeTable &GetTable(const IcebergTableInformation &table_info) {
		auto &metadata = table_info.table_metadata;
		auto table_uuid = metadata.table_uuid;
		auto it = tables.find(table_uuid);
		if (it != tables.end()) {
			return it->second;
		}
		auto res = tables.emplace(table_uuid, DuckLakeTable(table_uuid, table_info.name));
		return res.first->second;
	}

	DuckLakeSchema &GetSchema(const string &schema_name) {
		auto it = schemas.find(schema_name);
		if (it != schemas.end()) {
			return it->second;
		}
		auto res = schemas.emplace(schema_name, DuckLakeSchema(schema_name));
		return res.first->second;
	}

public:
	//! timestamp -> snapshot
	map<timestamp_t, DuckLakeSnapshot> snapshots;
	//! table_uuid -> table
	unordered_map<string, DuckLakeTable> tables;
	//! schema name -> schema
	unordered_map<string, DuckLakeSchema> schemas;

public:
	//! Skip these tables (should be set if a table doesn't meet the conversion criteria)
	set<string> table_names_to_skip;

public:
	//! The statements to execute on the metadata catalog
	vector<string> sql_statements;
	string ducklake_catalog;
};

static unique_ptr<FunctionData> IcebergToDuckLakeBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto ret = make_uniq<IcebergToDuckLakeBindData>();
	auto input_string = input.inputs[0].ToString();
	ret->ducklake_catalog = input.inputs[1].ToString();

	auto &catalog = Catalog::GetCatalog(context, input_string);
	auto catalog_type = catalog.GetCatalogType();
	if (catalog_type != "iceberg") {
		throw InvalidInputException("First parameter must be the name of an attached Iceberg catalog");
	}
	auto &iceberg_catalog = catalog.Cast<IcebergCatalog>();
	auto &schema_set = iceberg_catalog.GetSchemas();

	IcebergOptions options;
	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		auto &val = kv.second;
		if (loption == "allow_moved_paths") {
			options.allow_moved_paths = BooleanValue::Get(val);
		} else if (loption == "metadata_compression_codec") {
			options.metadata_compression_codec = StringValue::Get(val);
		} else if (loption == "version") {
			options.table_version = StringValue::Get(val);
		} else if (loption == "version_name_format") {
			auto value = StringValue::Get(kv.second);
			int string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
			if (string_substitutions != 2) {
				throw InvalidInputException(
				    "'version_name_format' has to contain two occurrences of '%%s' in it, found %d",
				    string_substitutions);
			}
			options.version_name_format = value;
		} else if (loption == "skip_tables") {
			auto &type = kv.second.type();
			if (kv.second.IsNull() || type.id() != LogicalTypeId::LIST) {
				throw InvalidInputException("'skip_tables' has to be provided as a list of strings");
			}
			auto &child_type = ListType::GetChildType(type);
			if (child_type.id() != LogicalTypeId::VARCHAR) {
				throw InvalidInputException("'skip_tables' has to be provided as a list of strings");
			}
			auto &tables = ListValue::GetChildren(kv.second);
			for (auto &table : tables) {
				ret->table_names_to_skip.insert(table.GetValue<string>());
			}
		}
	}

	schema_set.LoadEntries(context);
	for (auto &it : schema_set.GetEntries()) {
		auto &schema_entry = it.second->Cast<IcebergSchemaEntry>();
		auto &tables = schema_entry.tables;
		tables.LoadEntries(context);
		for (auto &it : tables.GetEntriesMutable()) {
			auto &table = it.second;
			tables.FillEntry(context, table);
			ret->AddTable(table, context, options);
		}
	}

	ret->AssignSchemaBeginSnapshots();

	ret->sql_statements = ret->CreateSQLStatements();

	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("count");
	return std::move(ret);
}

} // namespace ducklake

} // namespace iceberg

struct IcebergToDuckLakeGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	IcebergToDuckLakeGlobalTableFunctionState(unique_ptr<Connection> connection, const string &metadata_catalog)
	    : connection(std::move(connection)), metadata_catalog(metadata_catalog) {};
	virtual ~IcebergToDuckLakeGlobalTableFunctionState() {
		if (connection) {
			connection.reset();
		}
	}

public:
	void VerifyDuckLakeVersion() {
		auto version_query =
		    StringUtil::Replace("SELECT value FROM {METADATA_CATALOG}.ducklake_metadata where key = 'version'",
		                        "{METADATA_CATALOG}", metadata_catalog);
		auto result = connection->Query(version_query);
		if (result->HasError()) {
			result->ThrowError("'iceberg_to_ducklake' version verification query failed: ");
		}

		D_ASSERT(result->ColumnCount() == 1);
		auto chunk = result->Fetch();
		if (!chunk) {
			throw InvalidInputException("'iceberg_to_ducklake' version verification query failed, produced no chunks");
		}
		if (chunk->size() == 0) {
			throw InvalidInputException("Metadata catalog does not have a 'version' entry in 'ducklake_metadata'");
		}
		auto value = chunk->GetValue(0, 0);
		if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException(
			    "DuckLake version metadata is corrupt, the value can't be NULL and has to be of type VARCHAR");
		}
		auto version_string = value.GetValue<string>();
		if (!StringUtil::StartsWith(version_string, "0.4")) {
			throw InvalidInputException(
			    "'iceberg_to_ducklake' only support version 0.4 currently, detected '%s' instead", version_string);
		}
	}

	void VerifyEmptyCatalog() {
		auto query = StringUtil::Replace("SELECT max(snapshot_id) FROM {METADATA_CATALOG}.ducklake_snapshot;",
		                                 "{METADATA_CATALOG}", metadata_catalog);
		auto result = connection->Query(query);
		if (result->HasError()) {
			result->ThrowError("'iceberg_to_ducklake' verification query failed: ");
		}

		D_ASSERT(result->ColumnCount() == 1);
		auto chunk = result->Fetch();
		if (!chunk) {
			throw InvalidInputException("'iceberg_to_ducklake' verification query failed, produced no chunks");
		}
		if (chunk->size() == 0) {
			throw InvalidInputException("Couldn't get 'max(snapshot_id)', produced 0 rows");
		}
		auto value = chunk->GetValue(0, 0);
		if (value.IsNull() || value.type().id() != LogicalTypeId::BIGINT) {
			throw InvalidInputException(
			    "'max(snapshot_id)' did not produce a non-null value, or the value type is not BIGINT (int64)");
		}
		auto max_snapshot_id = value.GetValue<int64_t>();
		if (max_snapshot_id != 0) {
			throw InvalidInputException("'iceberg_to_ducklake' can only be used on empty catalogs currently");
		}
	}

public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<iceberg::ducklake::IcebergToDuckLakeBindData>();
		auto &input_string = bind_data.ducklake_catalog;

		auto &catalog = Catalog::GetCatalog(context, input_string);
		auto catalog_type = catalog.GetCatalogType();
		if (catalog_type != "ducklake") {
			throw InvalidInputException("Second parameter must be the name of an attached DuckLake catalog");
		}

		auto metadata_catalog = StringUtil::Format("__ducklake_metadata_%s", input_string);
		//! Verify the existence of the metadata catalog and that it's attached as well.
		(void)Catalog::GetCatalog(context, metadata_catalog);

		auto &db = DatabaseInstance::GetDatabase(context);
		auto connection = make_uniq<Connection>(db);
		auto res = make_uniq<IcebergToDuckLakeGlobalTableFunctionState>(std::move(connection), metadata_catalog);
		res->VerifyDuckLakeVersion();
		res->VerifyEmptyCatalog();
		return std::move(res);
	}

public:
	//! Connection used to run the SQL statements
	unique_ptr<Connection> connection;
	string metadata_catalog;
};

static void IcebergToDuckLakeFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<iceberg::ducklake::IcebergToDuckLakeBindData>();
	auto &global_state = data.global_state->Cast<IcebergToDuckLakeGlobalTableFunctionState>();

	auto &connection = *global_state.connection;
	auto &statements = bind_data.sql_statements;

	auto query = StringUtil::Join(statements, "\n");
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", StringUtil::Format("%s", global_state.metadata_catalog));
	auto result = connection.Query(query);
	if (result->HasError()) {
		result->ThrowError("'iceberg_to_ducklake' failed to commit to the DuckLake metadata catalog: ");
	}

	output.SetCardinality(0);
}

TableFunctionSet IcebergFunctions::GetIcebergToDuckLakeFunction() {
	TableFunctionSet function_set("iceberg_to_ducklake");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, IcebergToDuckLakeFunction,
	                         iceberg::ducklake::IcebergToDuckLakeBind, IcebergToDuckLakeGlobalTableFunctionState::Init);
	fun.named_parameters.emplace("skip_tables", LogicalType::LIST(LogicalTypeId::VARCHAR));
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
