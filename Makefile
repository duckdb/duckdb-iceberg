.PHONY: fixture fixture-latest gravitino lakekeeper polaris nessie

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

include make/util.mk
include make/catalogs/fixture.mk
include make/catalogs/gravitino.mk
include make/catalogs/lakekeeper.mk
include make/catalogs/nessie.mk
include make/catalogs/polaris.mk

install_requirements:
	python3 -m pip install -r scripts/requirements.txt
