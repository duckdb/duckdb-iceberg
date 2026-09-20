# DESTRUCTIVE: only use against the dedicated upstream-test fixture, serially.
# on_init has already attached my_datalake. This runs once before each test,
# never on a load/restart within that test. No generated fixture data is retained.

# Leave the remote schema before dropping it. Discover the local database name
# so this also works after load, when the primary database is not named memory.
statement ok
SET VARIABLE fixture_cleanup_local_catalog = (
    SELECT ['"' || replace(database_name, '"', '""') || '"']
    FROM duckdb_databases()
    WHERE NOT internal AND database_name <> 'my_datalake'
    ORDER BY database_oid LIMIT 1
);

foreach local_catalog <variable:fixture_cleanup_local_catalog>

statement ok
USE {local_catalog}.main;

endloop

# Quote identifiers before substitution, including embedded double quotes.
# The current runner executes an empty foreach once. Use an explicit sentinel
# and skip it; real identifiers are quoted, so they cannot equal this sentinel.
statement ok
SET VARIABLE fixture_cleanup_views = (
    SELECT coalesce(list('"' || replace(schema_name, '"', '""') || '"."' ||
                         replace(view_name, '"', '""') || '"' ORDER BY schema_name, view_name), ['__empty__']::VARCHAR[])
    FROM duckdb_views() WHERE database_name = 'my_datalake'
);

foreach view_name <variable:fixture_cleanup_views>

skipif view_name=__empty__
statement ok
DROP VIEW my_datalake.{view_name};

endloop

statement ok
SET VARIABLE fixture_cleanup_tables = (
    SELECT coalesce(list('"' || replace(schema_name, '"', '""') || '"."' ||
                         replace(table_name, '"', '""') || '"' ORDER BY schema_name, table_name), ['__empty__']::VARCHAR[])
    FROM duckdb_tables() WHERE database_name = 'my_datalake'
);

foreach table_name <variable:fixture_cleanup_tables>

skipif table_name=__empty__
statement ok
DROP TABLE my_datalake.{table_name};

endloop

# DROP SCHEMA CASCADE is not supported. Drop objects first, then namespaces.
# Preserve the now-empty main schema: DuckDB rejects dropping DEFAULT_SCHEMA.
# Longer names first puts child namespaces before their parents. The fixture
# lists implicit parents that disappear with their last child, hence IF EXISTS.
statement ok
SET VARIABLE fixture_cleanup_schemas = (
    SELECT coalesce(list('"' || replace(schema_name, '"', '""') || '"'
                         ORDER BY length(schema_name) DESC, schema_name), ['__empty__']::VARCHAR[])
    FROM duckdb_schemas() WHERE database_name = 'my_datalake' AND schema_name <> 'main'
);

foreach schema_name <variable:fixture_cleanup_schemas>

skipif schema_name=__empty__
statement ok
DROP SCHEMA IF EXISTS my_datalake.{schema_name};

endloop

statement ok
CREATE SCHEMA IF NOT EXISTS my_datalake.main;

statement ok
USE my_datalake.main;

# Do not leave SQL variables visible to the upstream test.
statement ok
RESET VARIABLE fixture_cleanup_views;

statement ok
RESET VARIABLE fixture_cleanup_tables;

statement ok
RESET VARIABLE fixture_cleanup_schemas;

statement ok
RESET VARIABLE fixture_cleanup_local_catalog;
