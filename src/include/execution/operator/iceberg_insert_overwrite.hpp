#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"

namespace duckdb {
class ClientContext;
struct IcebergManifestEntry;
struct IcebergTable;
struct IcebergTableMetadata;
struct IcebergTransactionData;

//! Which rows an INSERT OVERWRITE replaces; the value of 'iceberg_partition_overwrite_mode'
enum class IcebergOverwriteMode : uint8_t {
	//! The rows that match the static PARTITION values, every row when there are none
	STATIC,
	//! The partitions that receive rows
	DYNAMIC
};

//! A PARTITION entry with a value: 'column = value'
struct IcebergOverwriteStaticValue {
	//! Position of the column in the table's current schema
	idx_t column_index;
	//! The value, in the column's type; NULL selects the rows whose column is NULL
	Value value;
};

//! What an overwrite read of a snapshot to find the files it replaces
struct IcebergOverwriteManifests {
	int64_t snapshot_id;
	//! The manifest list of the snapshot
	vector<IcebergManifestListEntry> manifest_list;
	//! Its manifests that can hold a replaced file, with their entries
	vector<IcebergManifestListEntry> manifests;
};

//! What an INSERT OVERWRITE replaces. An insert operator that has one replaces data, one without appends
struct IcebergOverwriteSpec {
	vector<IcebergOverwriteStaticValue> static_values;
	//! Prepared with a parameter as a PARTITION value: bound again with the value before it runs, never executed
	bool missing_static_values = false;
	//! Set by PlanStaticOverwrite
	shared_ptr<const IcebergOverwriteManifests> planned_manifests;
};

struct IcebergInsertOverwrite {
	//! Parses a value of 'iceberg_partition_overwrite_mode'
	static IcebergOverwriteMode ParseMode(const string &mode);
	//! The mode in force for the statement that is running
	static IcebergOverwriteMode GetMode(ClientContext &context);
	//! The position in the current schema of a column named in a PARTITION clause. Throws unless the current
	//! partition spec partitions by its identity
	static idx_t ResolvePartitionColumn(const IcebergTableMetadata &metadata, const string &table_name,
	                                    const string &column_name);
	//! Casts the value of a PARTITION entry to the type of its column. Throws if the cast fails or changes the value
	static Value CastPartitionValue(ClientContext &context, const Value &value, const LogicalType &type,
	                                const string &column_name);
	//! Called when the statement is planned: a file that static values cannot decide fails the statement before its
	//! query runs or writes a file. Keeps the manifests it reads in 'spec' for Stage
	static void PlanStaticOverwrite(ClientContext &context, const IcebergTable &table, IcebergOverwriteSpec &spec);
	//! Stages the snapshot of an overwrite in the transaction: it adds 'written_files' and removes the files they
	//! replace, of the table's snapshot and of earlier statements of the transaction. Static mode: the files whose
	//! rows all match the static values, every file without static values. Dynamic mode: the files of the current
	//! partition spec with the partition values of one of 'written_files'
	static void Stage(ClientContext &context, const IcebergTable &table, IcebergTransactionData &transaction_data,
	                  const IcebergOverwriteSpec &spec, vector<IcebergManifestEntry> &&written_files);
};

} // namespace duckdb
