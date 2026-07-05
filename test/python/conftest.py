import importlib
import importlib.util
import sys
from pathlib import Path

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.data_generators.integration_config import (
    GENERATOR_CATALOG_NAMES,
    REST_CATALOG_NAMES,
    get_rest_catalog_profile,
    resolve_active_catalog,
    resolve_pyspark_runtime,
)
from scripts.data_generators.tests import IcebergTest
from spark_seed import RegisteredSeedTable, SparkSeedTable


if importlib.util.find_spec("pyspark") is not None:
    pyspark = importlib.import_module("pyspark")
    PYSPARK_VERSION = Version(pyspark.__version__)
else:
    pyspark = None
    PYSPARK_VERSION = None


TEST_PYTHON_VERBOSITY_LEVELS = ("normal", "verbose")


def _requires_catalog_options(path: str) -> bool:
    return "cloud" not in Path(path).parts


def _selected_catalog_profile(config: pytest.Config):
    try:
        catalog = resolve_active_catalog(
            allowed_catalogs=REST_CATALOG_NAMES,
            purpose="catalog-backed test/python runs",
        )
    except RuntimeError as exc:
        raise pytest.UsageError(str(exc)) from exc
    return get_rest_catalog_profile(catalog)


def _selected_spark_runtime(config: pytest.Config):
    try:
        runtime, _ = resolve_pyspark_runtime(purpose="catalog-backed test/python runs")
    except RuntimeError as exc:
        raise pytest.UsageError(str(exc)) from exc
    return runtime


def _test_python_verbosity(config: pytest.Config) -> str:
    return config.getoption("--test-python-verbosity")


def _is_verbose_test_python_run(config: pytest.Config) -> bool:
    return _test_python_verbosity(config) == "verbose"


def _requirement_failure_message(requirement: str, catalog_profile, spark_runtime) -> list[str]:
    if requirement == "format_v3":
        failures = []
        if "format_v3" not in spark_runtime.capabilities:
            failures.append(f"Spark runtime {spark_runtime.name} does not support Iceberg format version 3")
        if "format_v3" not in catalog_profile.capabilities:
            failures.append(f"Catalog '{catalog_profile.name}' does not support Iceberg format version 3 in this suite")
        return failures
    if requirement == "row_lineage":
        if "row_lineage" not in catalog_profile.capabilities:
            return [f"Catalog '{catalog_profile.name}' does not support row-lineage coverage in this suite"]
        return []
    raise pytest.UsageError(
        f"Unknown test/python capability requirement '{requirement}'. "
        "Supported requirements: format_v3, row_lineage."
    )


def _collect_requirement_failures(item, catalog_profile, spark_runtime) -> list[str]:
    failures = []
    for marker in item.iter_markers(name="requires_capabilities"):
        for requirement in marker.args:
            if not isinstance(requirement, str):
                raise pytest.UsageError(
                    f"{item.nodeid} uses requires_capabilities with a non-string requirement: {requirement!r}"
                )
            failures.extend(_requirement_failure_message(requirement, catalog_profile, spark_runtime))
    return failures


def capability_param(value, *requirements: str, id: str | None = None):
    marks = ()
    if requirements:
        marks = (pytest.mark.requires_capabilities(*requirements),)
    return pytest.param(value, marks=marks, id=id)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires_spark(spec): require Spark version matching spec "
        "(PEP 440 specifier, e.g. '>=3.5,<4.0', '==4.0.*')",
    )
    config.addinivalue_line(
        "markers",
        "spark_seed_tables(*tables): seed catalog_connection with registered names, RegisteredSeedTable objects, "
        "or SparkSeedTable objects",
    )
    config.addinivalue_line(
        "markers",
        "requires_capabilities(*requirements): require named catalog/runtime capabilities before test setup "
        "(currently: 'format_v3', 'row_lineage')",
    )
    config.addinivalue_line(
        "markers",
        "generator_catalog(name): override the data-generator catalog used by spark_seed_tables "
        "(e.g. 'local' for local-generator-backed paired tests)",
    )


def pytest_addoption(parser):
    parser.addoption(
        "--unittest-binary",
        action="store",
        default=None,
        help="Provide the unittest binary to use for stdin-driven integration tests",
    )
    parser.addoption(
        "--print-unittest-stdin",
        action="store_true",
        default=False,
        help="Print the sqllogictest stdin transcript for stdin-driven integration tests",
    )
    parser.addoption(
        "--test-python-verbosity",
        action="store",
        choices=TEST_PYTHON_VERBOSITY_LEVELS,
        default="normal",
        help="Higher-level verbosity for test/python runs. 'verbose' prints selected environment and requirement skips.",
    )


def pytest_ignore_collect(collection_path, config):
    requested_paths = [Path(arg) for arg in config.args if not arg.startswith("-")]
    explicit_cloud_run = any("cloud" in path.parts for path in requested_paths)
    if not explicit_cloud_run and "cloud" in Path(str(collection_path)).parts:
        return True
    return False


@pytest.fixture()
def unittest_binary(request):
    custom_arg = request.config.getoption("--unittest-binary")
    if not custom_arg:
        raise ValueError(
            "Please provide a unittest binary path to the tester, using '--unittest-binary <path_to_unittest>'"
        )
    return custom_arg


@pytest.fixture()
def print_unittest_stdin(pytestconfig):
    return pytestconfig.getoption("--print-unittest-stdin")


@pytest.fixture()
def paired_sqllogic_test_path(request) -> Path:
    test_name = getattr(request.node, "originalname", None) or request.function.__name__
    sqllogic_path = Path(str(request.fspath)).with_name(f"{test_name}.test")
    if not sqllogic_path.is_file():
        raise pytest.UsageError(f"{request.node.nodeid} expects a colocated sqllogic file at {sqllogic_path}")
    return sqllogic_path


@pytest.fixture(scope="session")
def catalog_profile(pytestconfig):
    profile = getattr(pytestconfig, "_catalog_profile", None)
    if profile is None:
        raise pytest.UsageError("Catalog profile is only available for catalog-backed test/python runs")
    return profile


@pytest.fixture(scope="session")
def spark_runtime(pytestconfig):
    runtime = getattr(pytestconfig, "_spark_runtime", None)
    if runtime is None:
        raise pytest.UsageError("Spark runtime is only available for catalog-backed test/python runs")
    return runtime


@pytest.fixture(scope="session")
def unittest_test_config(catalog_profile):
    return catalog_profile.unittest_config


@pytest.fixture(scope="session")
def duckdb_catalog_init_sql(catalog_profile):
    return catalog_profile.duckdb_catalog_init_sql


@pytest.fixture(scope="session")
def rest_catalog(catalog_profile):
    pyice_rest = pytest.importorskip("pyiceberg.catalog.rest")
    return pyice_rest.RestCatalog("rest", **catalog_profile.build_pyiceberg_config())


def _find_generator_case(table_name: str):
    matches = []
    for generator_class in IcebergTest.registry:
        generator = generator_class()
        if generator.table == table_name or generator.qualified_name == table_name:
            matches.append(generator)

    if not matches:
        raise ValueError(f"No data generator registered for table '{table_name}'")
    if len(matches) > 1:
        matched_names = ", ".join(generator.qualified_name for generator in matches)
        raise ValueError(
            f"Multiple data generators match '{table_name}': {matched_names}. "
            "Use the fully qualified generator name instead."
        )
    return matches[0]


def _resolve_seed_table(table):
    if isinstance(table, str):
        return RegisteredSeedTable(table)
    if isinstance(table, RegisteredSeedTable):
        return table
    if isinstance(table, SparkSeedTable):
        return table
    raise ValueError(
        "spark_seed_tables entries must be registered table names, RegisteredSeedTable objects, "
        "or SparkSeedTable objects, "
        f"got {type(table).__name__}"
    )


def _apply_generator_expectations(request, generator_case: IcebergTest, active_catalog: str) -> None:
    skip_reason = generator_case.skips.get(active_catalog)
    if skip_reason is not None:
        pytest.skip(skip_reason)

    supported_catalogs = generator_case.supported_catalogs
    if supported_catalogs is not None and active_catalog not in supported_catalogs:
        pytest.skip(f"{generator_case.qualified_name} does not apply to catalog '{active_catalog}'")

    xfail_reason = generator_case.expected_failures.get(active_catalog)
    if xfail_reason is not None:
        request.node.add_marker(pytest.mark.xfail(reason=xfail_reason, strict=True))


def _seed_generator_catalog(item, default_catalog: str) -> str:
    marker = item.get_closest_marker("generator_catalog")
    if marker is None:
        return default_catalog
    if len(marker.args) != 1 or not isinstance(marker.args[0], str):
        raise pytest.UsageError(
            f"{item.nodeid} uses generator_catalog with invalid arguments: expected exactly one string catalog name"
        )
    catalog = marker.args[0]
    if catalog not in GENERATOR_CATALOG_NAMES:
        allowed = ", ".join(GENERATOR_CATALOG_NAMES)
        raise pytest.UsageError(
            f"{item.nodeid} uses unsupported generator_catalog '{catalog}'. Expected one of: {allowed}."
        )
    return catalog


@pytest.fixture(scope="session")
def _catalog_connection_manager(catalog_profile, spark_runtime):
    from scripts.data_generators.connections import IcebergConnection

    manager = {
        "active_connection": None,
        "active_connection_key": None,
    }

    def switch_to(connection_key: str):
        active_connection = manager["active_connection"]
        active_connection_key = manager["active_connection_key"]
        if active_connection_key != connection_key:
            if active_connection is not None:
                active_connection.close()
            active_connection = IcebergConnection.get_class(connection_key)(runtime=spark_runtime)
            manager["active_connection"] = active_connection
            manager["active_connection_key"] = connection_key
        return active_connection

    switch_to(catalog_profile.connection_key)
    manager["switch_to"] = switch_to
    yield manager

    active_connection = manager["active_connection"]
    if active_connection is not None:
        active_connection.close()


class _CatalogConnectionProxy:
    def __init__(self, manager, default_connection_key: str):
        self._manager = manager
        self._default_connection_key = default_connection_key

    def use_connection_key(self, connection_key: str):
        return self._manager["switch_to"](connection_key)

    def use_default_connection(self):
        return self.use_connection_key(self._default_connection_key)

    @property
    def active_connection(self):
        return self._manager["active_connection"]

    @property
    def default_connection(self):
        return self.use_default_connection()

    @property
    def con(self):
        return self.default_connection.con

    def _refresh_table_name(self, table_name: str) -> str:
        if table_name.count(".") >= 2:
            return table_name
        return f"{self.default_connection.catalog}.{table_name}"

    def refresh_table(self, table_name: str) -> None:
        self.default_connection.con.catalog.refreshTable(self._refresh_table_name(table_name))

    def refresh_tables(self, *table_names: str) -> None:
        for table_name in table_names:
            self.refresh_table(table_name)

    def restart(self):
        return self.default_connection.restart()

    def __getattr__(self, name):
        return getattr(self.default_connection, name)


@pytest.fixture(scope="session")
def catalog_session_connection(catalog_profile, _catalog_connection_manager):
    return _CatalogConnectionProxy(_catalog_connection_manager, catalog_profile.connection_key)


@pytest.fixture()
def catalog_connection(request, catalog_profile, catalog_session_connection):
    connection = catalog_session_connection
    seed_marker = request.node.get_closest_marker("spark_seed_tables")
    seed_names = list(seed_marker.args) if seed_marker else []
    seed_catalog = _seed_generator_catalog(request.node, catalog_profile.name)

    for table in seed_names:
        seed_table = _resolve_seed_table(table)
        if isinstance(seed_table, RegisteredSeedTable):
            generator_case = _find_generator_case(seed_table.qualified_name)
            _apply_generator_expectations(request, generator_case, seed_catalog)
            generator_case.write_intermediates = (
                seed_table.write_intermediates if seed_table.write_intermediates is not None else False
            )
            seed_table = generator_case
            connection_key = seed_table.catalog_mapping.get(seed_catalog, seed_catalog)
        elif isinstance(seed_table, IcebergTest):
            _apply_generator_expectations(request, seed_table, seed_catalog)
            connection_key = seed_table.catalog_mapping.get(seed_catalog, seed_catalog)
        else:
            connection_key = catalog_profile.connection_key
        seed_connection = connection.use_connection_key(connection_key)
        seed_table.generate(seed_connection)

    yield connection


@pytest.fixture(autouse=True)
def _apply_spark_seed_tables_marker(request):
    if request.node.get_closest_marker("spark_seed_tables") is not None:
        request.getfixturevalue("catalog_connection")


@pytest.fixture()
def spark_con(catalog_connection):
    return catalog_connection.con


def pytest_report_header(config):
    if not _is_verbose_test_python_run(config):
        return None

    return [f"test/python verbosity: {_test_python_verbosity(config)}"]


def pytest_collection_modifyitems(config, items):
    needs_catalog_options = any(_requires_catalog_options(str(item.fspath)) for item in items)
    if needs_catalog_options:
        config._catalog_profile = _selected_catalog_profile(config)
        config._spark_runtime = _selected_spark_runtime(config)
        config._requirement_skip_log = []

    for item in items:
        if needs_catalog_options and _requires_catalog_options(str(item.fspath)):
            seed_catalog = _seed_generator_catalog(item, config._catalog_profile.name)
            if seed_catalog == "local" and config._catalog_profile.name != "fixture":
                item.add_marker(
                    pytest.mark.skip(reason="Local-generator paired tests only run in the fixture test/python matrix")
                )
                continue

            failures = _collect_requirement_failures(item, config._catalog_profile, config._spark_runtime)
            if failures:
                skip_reason = "Test requirements not met: " + "; ".join(failures)
                item.add_marker(pytest.mark.skip(reason=skip_reason))
                config._requirement_skip_log.append((item.nodeid, failures))

        marker = item.get_closest_marker("requires_spark")
        if marker is None:
            continue

        spec = SpecifierSet(marker.args[0])
        if PYSPARK_VERSION is None:
            item.add_marker(pytest.mark.skip(reason=f"Requires Spark {spec}, but PySpark is not installed"))
        elif PYSPARK_VERSION not in spec:
            item.add_marker(
                pytest.mark.skip(reason=f"Requires Spark {spec}, but current PySpark version is {PYSPARK_VERSION}")
            )


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if not _is_verbose_test_python_run(config):
        return

    profile = getattr(config, "_catalog_profile", None)
    runtime = getattr(config, "_spark_runtime", None)
    if profile is not None or runtime is not None:
        terminalreporter.section("test/python environment", sep="-", blue=True, bold=False)
        if profile is not None:
            terminalreporter.line(
                f"active catalog: {profile.name} (capabilities: {', '.join(sorted(profile.capabilities)) or 'none'})"
            )
        if runtime is not None:
            terminalreporter.line(
                f"spark runtime: {runtime.name} (capabilities: {', '.join(sorted(runtime.capabilities)) or 'none'})"
            )

    requirement_skips = getattr(config, "_requirement_skip_log", [])
    if not requirement_skips:
        return

    terminalreporter.section("test/python requirement skips", sep="-", blue=True, bold=False)
    for nodeid, failures in requirement_skips:
        terminalreporter.line(nodeid)
        for failure in failures:
            terminalreporter.line(f"  - {failure}")
