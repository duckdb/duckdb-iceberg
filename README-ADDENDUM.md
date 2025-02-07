# ADDENDUM

This fork adds proof-of-concept functionality to DuckDB iceberg extension to be able to connect to an iceberg catalog and write to iceberg tables as well as read from them.

# Requirements
You will need the following to be able to use this new version of the extension:
1. DuckDB version 1.2.0 or later
2. `httpfs` extension

Since this extension is not official yet, you will need to run duckdb in `unsigned` mode to be able to use it:
```bash
duckdb --unsigned
```

# Installation
The following steps need to be done once:
1. Download the zip from github and unzip it
2. Change directory to the directory where you unzipped the files
3. Install the extension
```sql
INSTALL './iceberg.duckdb_extension';
```
4. If you already have the official `iceberg` extension installed, you will need to force the install
```sql
FORCE INSTALL './iceberg.duckdb_extension';
```
5. Install `httpfs` extension if you don't have it installed already
```sql
INSTALL httpfs;
```

# Usage
## Load `httpfs` and `iceberg` extensions
```sql
LOAD httpfs;
LOAD iceberg;
```
## Create a secret to provide access to an iceberg catalog
```sql
CREATE SECRET (
	TYPE ICEBERG,
	CLIENT_ID '${CLIENT_ID}',
	CLIENT_SECRET '${CLIENT_SECRET}',
	ENDPOINT '${ENDPOINT}',
	AWS_REGION '${AWS_REGION}'
)
```

## Attach an iceberg catalog
```sql
ATTACH 'my_catalog' AS my_catalog (TYPE ICEBERG)
```

## Read an iceberg table
```sql
SELECT * FROM my_catalog.my_schema.table_1;
```

## Create a new iceberg table
```sql
CREATE TABLE my_catalog.my_schema.new_table (id BIGINT, name VARCHAR);
```
```sql
CREATE TABLE my_catalog.my_schema.new_table_2 AS (SELECT FROM version() as "version");
```

## Delete an existing iceberg table
```sql
DROP TABLE my_catalog.my_schema.table_1;
```

# How to build extension from source
Requirements:
* A compiler that supports C++17
* CMake version 3.28 or later
```
git clone https://github.com/fivetran/duckdb-iceberg.git
git submodule update --init --recursive
brew install ninja
GEN=ninja make {debug/release}
```

# Roadmap
## 1. Supported SQL commands
### ✅ CREATE SECRET
### ✅ ATTACH
### 🔳 USE
### ✅ SELECT
### ✅ CREATE SCHEMA
### ✅ DROP SCHEMA
### 🔳 CREATE VIEW
### 🔳 DROP VIEW
### ✅ CREATE TABLE
### ✅ CREATE TABLE AS SELECT
### 🔳 ALTER TABLE
### ✅ DROP TABLE
### 🔳 INSERT
### 🔳 UPDATE
### 🔳 DELETE

## 2. Supported [Iceberg data types](https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types) (writing)
### 🔳 boolean
### ✅ string
### 🔳 tinyint
### 🔳 smallint
### ✅ int
### ✅ long
### ✅ double
### 🔳 float
### 🔳 timestamp
### 🔳 timestamptz
### 🔳 binary
### 🔳 date
### 🔳 decimal(prec,scale)
### 🔳 array
### 🔳 map
### 🔳 struct

## 3. Miscellaneous
### 🔳 Bundle `jiceberg` statically into the extension







