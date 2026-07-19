#pragma once

#include <variant>

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_information.hpp"

namespace duckdb {

class IcebergTransaction;

//! Update a table with a regular alter
struct IcebergTransactionAlterUpdate {
public:
	IcebergTransactionAlterUpdate(IcebergTransaction &transaction);
	~IcebergTransactionAlterUpdate();

public:
	IcebergTableInformation &CreateTable(const string &table_key, IcebergTableInformation &&table);
	IcebergTableInformation &GetOrInitializeTable(const IcebergTableInformation &table);
	bool HasUpdates() const;

public:
	IcebergTransaction &transaction;
	//! All the tables touched in this atomic block
	case_insensitive_map_t<reference<IcebergTableInformation>> updated_tables;
};

//! Drop a table
struct IcebergTransactionDeleteUpdate {
public:
	IcebergTransactionDeleteUpdate(IcebergTransaction &transaction, IcebergTableInformation &table);
	~IcebergTransactionDeleteUpdate();

public:
	IcebergTransaction &transaction;
	reference<IcebergTableInformation> deleted_table;
};

//! Rename a table
struct IcebergTransactionRenameUpdate {
public:
	IcebergTransactionRenameUpdate(IcebergTransaction &transaction, IcebergTableInformation &table,
	                               IcebergTableInformation &new_table, const string &new_name);
	~IcebergTransactionRenameUpdate();

public:
	IcebergTransaction &transaction;
	reference<IcebergTableInformation> table;
	reference<IcebergTableInformation> new_table;
	string new_name;
};

using IcebergTransactionUpdate = std::variant<std::monostate, IcebergTransactionAlterUpdate,
                                              IcebergTransactionDeleteUpdate, IcebergTransactionRenameUpdate>;

} // namespace duckdb
