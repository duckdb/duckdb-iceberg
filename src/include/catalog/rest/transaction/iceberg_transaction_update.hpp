#pragma once

#include <variant>

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"

namespace duckdb {

class IcebergTransaction;

//! Update a table with a regular alter
struct IcebergTransactionAlterUpdate {
public:
	IcebergTransactionAlterUpdate(IcebergTransaction &transaction);
	~IcebergTransactionAlterUpdate();

public:
	IcebergTable &CreateTable(const string &table_key, IcebergTable &&table);
	IcebergTable &GetOrInitializeTable(const IcebergTable &table);
	bool HasUpdates() const;

public:
	IcebergTransaction &transaction;
	//! All the tables touched in this atomic block
	case_insensitive_map_t<reference<IcebergTable>> updated_tables;
};

//! Drop a table
struct IcebergTransactionDeleteUpdate {
public:
	IcebergTransactionDeleteUpdate(IcebergTransaction &transaction, IcebergTable &table);
	~IcebergTransactionDeleteUpdate();

public:
	IcebergTransaction &transaction;
	reference<IcebergTable> deleted_table;
};

//! Rename a table
struct IcebergTransactionRenameUpdate {
public:
	IcebergTransactionRenameUpdate(IcebergTransaction &transaction, IcebergTable &table, IcebergTable &new_table,
	                               const string &new_name);
	~IcebergTransactionRenameUpdate();

public:
	IcebergTransaction &transaction;
	reference<IcebergTable> table;
	reference<IcebergTable> new_table;
	string new_name;
};

using IcebergTransactionUpdate = std::variant<std::monostate, IcebergTransactionAlterUpdate,
                                              IcebergTransactionDeleteUpdate, IcebergTransactionRenameUpdate>;

} // namespace duckdb
