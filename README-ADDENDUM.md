# ADDENDUM

This fork adds proof-of-concept functionality to DuckDB iceberg extension to be able to connect to an iceberg catalog as well as read and write iceberg tables.

You can try it out using DuckDB (>= v1.2.0) by running duckdb in unsigned mode:
```bash
duckdb --unsigned
```

# SQL commands
## Install this extension and load it. If you already have the official `iceberg` extension installed, you will need to force install this one.
```sql
INSTALL '/path/to/this/iceberg.duckdb_extension';
LOAD '/path/to/this/iceberg.duckdb_extension';
```

## Install `httpfs` extension (if you don't have it already) and load it
```sql
INSTALL httpfs;
LOAD httpfs;
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
```
git clone https://github.com/fivetran/duckdb-iceberg.git
git submodule update --init --recursive
brew install ninja
GEN=ninja make {debug/release}
```

# Roadmap
## # SQL commands
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

## # Data Types ([ref](https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types))
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

## # Miscellaneous
### 🔳 Bundle `jiceberg` statically into the extension







