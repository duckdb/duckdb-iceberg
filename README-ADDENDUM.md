# ADDENDUM

This fork adds proof-of-concept functionality to DuckDB iceberg extension to be able to connect to an iceberg catalog as well as read and write iceberg tables.

You can try it out using DuckDB (>= v1.1.3) by doing the following:

1. Start duckdb in `unsigned` mode
```bash
duckdb --unsigned
```

2. Create a secret to provide access to your iceberg catalog
```sql
INSTALL '/path/to/this/iceberg.duckdb_extension';
INSTALL httpfs;
LOAD '/path/to/this/iceberg.duckdb_extension';
LOAD httpfs;
CREATE SECRET (
	TYPE ICEBERG,
	CLIENT_ID '${CLIENT_ID}',
	CLIENT_SECRET '${CLIENT_SECRET}',
	ENDPOINT '${ENDPOINT}',
	AWS_REGION '${AWS_REGION}'
)
```

3. Attach your iceberg catalog
```sql
ATTACH 'my_catalog' AS my_catalog (TYPE ICEBERG)
```

4. Read an iceberg table
```sql
SELECT * FROM my_catalog.my_schema.table_1;
```

5. Create a new iceberg table
```sql
CREATE TABLE my_catalog.my_schema.new_table (id BIGINT, name VARCHAR);
```
```sql
CREATE TABLE my_catalog.my_schema.new_table_2 AS (SELECT FROM version());
```

6. Delete an existing iceberg table
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
