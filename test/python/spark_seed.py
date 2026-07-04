from dataclasses import dataclass

from scripts.data_generators.tests.base import cleanup_generated_table, sql_statements


@dataclass(frozen=True)
class RegisteredSeedTable:
    qualified_name: str
    write_intermediates: bool | None = None


@dataclass(frozen=True)
class SparkSeedTable:
    qualified_name: str
    sql: str

    def generate(self, connection) -> None:
        cleanup_generated_table(connection, self.qualified_name)
        for statement in sql_statements(self.sql):
            connection.con.sql(statement)
