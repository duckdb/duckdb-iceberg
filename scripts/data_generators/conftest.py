from __future__ import annotations

from collections.abc import Iterator

import pytest

from scripts.data_generators.connections import IcebergConnection
from scripts.data_generators.integration_config import (
    GENERATOR_CATALOG_NAMES,
    get_spark_runtime,
    resolve_active_catalog,
    resolve_pyspark_runtime,
)


def parse_connection_args(values: list[str]) -> dict[str, str]:
    connection_args: dict[str, str] = {}
    for raw_value in values:
        if "=" not in raw_value:
            raise pytest.UsageError(
                f"Invalid --connection-arg '{raw_value}': expected key=value"
            )

        key, value = raw_value.split("=", 1)
        if not key:
            raise pytest.UsageError(
                f"Invalid --connection-arg '{raw_value}': key must not be empty"
            )
        if key in connection_args:
            raise pytest.UsageError(
                f"Duplicate --connection-arg '{key}': each key may be provided only once"
            )
        connection_args[key] = value

    return connection_args


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--connection-arg",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra keyword arguments passed to the resolved connection constructor",
    )


def pytest_configure(config: pytest.Config) -> None:
    config._connection_args = parse_connection_args(config.getoption("connection_arg"))
    try:
        config._active_catalog = resolve_active_catalog(
            allowed_catalogs=GENERATOR_CATALOG_NAMES,
            purpose="data generator tests",
        )
        if runtime_name := config._connection_args.pop("runtime", None):
            config._spark_runtime = get_spark_runtime(runtime_name)
        else:
            config._spark_runtime, _ = resolve_pyspark_runtime(purpose="data generator tests")
    except RuntimeError as exc:
        raise pytest.UsageError(str(exc)) from exc


@pytest.fixture(scope="session")
def active_catalog(pytestconfig: pytest.Config) -> str:
    return pytestconfig._active_catalog


@pytest.fixture(scope="session")
def connection_args(pytestconfig: pytest.Config) -> dict[str, str]:
    return dict(pytestconfig._connection_args)


@pytest.fixture(scope="session")
def spark_runtime(pytestconfig: pytest.Config):
    return pytestconfig._spark_runtime


@pytest.fixture
def catalog_mapping(generator_case) -> dict[str, str]:
    return dict(generator_case.catalog_mapping)


@pytest.fixture
def _generator_status(request: pytest.FixtureRequest, active_catalog: str, generator_case, spark_runtime) -> None:
    skip_reason = generator_case.skips.get(active_catalog)
    if skip_reason is not None:
        pytest.skip(skip_reason)

    supported_catalogs = generator_case.supported_catalogs
    if supported_catalogs is not None and active_catalog not in supported_catalogs:
        pytest.skip(f"{generator_case.table} does not apply to catalog '{active_catalog}'")

    missing_runtime_capabilities = generator_case.required_runtime_capabilities - spark_runtime.capabilities
    if missing_runtime_capabilities:
        missing_caps = ", ".join(sorted(missing_runtime_capabilities))
        pytest.skip(
            f"{generator_case.table} requires Spark runtime capabilities [{missing_caps}], "
            f"but runtime '{spark_runtime.name}' only provides [{', '.join(sorted(spark_runtime.capabilities))}]"
        )

    xfail_reason = generator_case.expected_failures.get(active_catalog)
    if xfail_reason is not None:
        request.node.add_marker(pytest.mark.xfail(reason=xfail_reason, strict=True))


@pytest.fixture(scope="session")
def _connection_manager() -> Iterator[dict[str, object]]:
    manager = {
        "active_connection": None,
        "active_connection_key": None,
    }
    yield manager
    connection = manager["active_connection"]
    if connection is not None:
        connection.close()


@pytest.fixture
def iceberg_connection(
    _generator_status,
    _connection_manager: dict[str, object],
    active_catalog: str,
    catalog_mapping: dict[str, str],
    connection_args: dict[str, str],
    spark_runtime,
) -> IcebergConnection:
    connection_key = catalog_mapping.get(active_catalog, active_catalog)
    resolved_connection_key = (connection_key, spark_runtime.name, tuple(sorted(connection_args.items())))
    active_connection = _connection_manager["active_connection"]
    active_connection_key: tuple[str, tuple[tuple[str, str], ...]] | None = _connection_manager["active_connection_key"]

    if active_connection_key != resolved_connection_key:
        if active_connection is not None:
            active_connection.close()

        connection_class = IcebergConnection.get_class(connection_key)
        active_connection = connection_class(runtime=spark_runtime, **connection_args)
        _connection_manager["active_connection"] = active_connection
        _connection_manager["active_connection_key"] = resolved_connection_key

    return active_connection
