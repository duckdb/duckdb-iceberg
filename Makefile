PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

start-rest-catalog: install_requirements
	./scripts/start-rest-catalog.sh

install_requirements:
	python3 -m pip install -r scripts/requirements.txt

# Custom makefile targets
data: data_clean start-rest-catalog
	python3 scripts/data_generators/generate_data.py spark-rest local

data_large: data data_clean
	python3 scripts/data_generators/generate_data.py spark-rest local

data_clean:
	rm -rf data/generated

format-fix:
	rm -rf src/amalgamation/*
	python3 scripts/format.py --all --fix --noconfirm

format-check:
	python3 scripts/format.py --all --check

format-head:
	python3 scripts/format.py HEAD --fix --noconfirm

format-changes:
	python3 scripts/format.py HEAD --fix --noconfirm

format-main:
	python3 scripts/format.py main --fix --noconfirm

format-check-silent:
	python3 scripts/format.py --all --check --silent
