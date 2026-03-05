.PHONY: data_nessie check_nessie_env

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

start-fixture-rest-catalog: install_requirements
	./scripts/start-fixture-rest-catalog.sh

install_requirements:
	python3 -m pip install -r scripts/requirements.txt

# Clone Nessie repo if folder doesn't exist
nessie:
	@if [ ! -d "nessie" ]; then \
		echo "Cloning Nessie repository..."; \
		git clone https://github.com/projectnessie/nessie.git nessie; \
	else \
		echo "Nessie repository already exists."; \
	fi

REQUIRED_NESSIE_VARS = \
	NESSIE_SERVER_AVAILABLE \
	S3_KEY_ID \
	S3_SECRET \
	S3_ENDPOINT \
	ICEBERG_CLIENT_ID \
	ICEBERG_CLIENT_SECRET \
	ICEBERG_ENDPOINT \
	OAUTH2_SERVER_URI \
	WAREHOUSE

check_nessie_env:
	@for var in $(REQUIRED_NESSIE_VARS); do \
		if [ -z "$${!var}" ]; then \
			echo "ERROR: $$var is not set."; \
			echo "Run: source scripts/nessie_env.sh"; \
			exit 1; \
		fi; \
	done

data_nessie: nessie check_nessie_env
	@echo "Starting Nessie catalog..."
	docker compose -f nessie/docker/catalog-auth-s3/docker-compose.yml up -d

	@echo "Generating data..."
	python3 -m scripts.data_generators.generate_data nessie

# Custom makefile targets
data: data_clean start-fixture-rest-catalog
	python3 -m scripts.data_generators.generate_data spark-rest local

data_large: data data_clean
	python3 -m scripts.data_generators.generate_data spark-rest local

data_clean:
	rm -rf data/generated
