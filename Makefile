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

# setup polaris server. See PolarisTesting.yml to see instructions for a specific machine.
setup_polaris_ci:
	mkdir polaris_catalog
	git clone https://github.com/apache/polaris.git polaris_catalog
	cd polaris_catalog && ./gradlew clean :polaris-quarkus-server:assemble -Dquarkus.container-image.build=true --no-build-cache
	cd polaris_catalog && ./gradlew --stop
	cd polaris_catalog && nohup ./gradlew run > polaris-server.log 2> polaris-error.log &

polaris_catalog_osx_local:
	rm -rf polaris_catalog || true
	mkdir polaris_catalog
	git clone https://github.com/apache/polaris.git polaris_catalog
	cd polaris_catalog && jenv local 21
	cd polaris_catalog && ./gradlew clean :polaris-quarkus-server:assemble -Dquarkus.container-image.build=true --no-build-cache
	cd polaris_catalog && ./gradlew --stop
	cd polaris_catalog && tmux kill-ses -t polaris || true
	cd polaris_catalog && tmux new-session -s polaris -d './gradlew run > polaris-server.log 2> polaris-error.log'
	sleep 60
	rm -rf polaris_catalog/bin || true
	python3 scripts/polaris/get_polaris_root_creds.py
	cd polaris_catalog && POLARIS_ROOT_ID=$(cat ../polaris_root_id.txt) POLARIS_ROOT_SECRET=$(cat ../polaris_root_password.txt) ../scripts/polaris/setup_polaris_catalog.sh > user_credentials.json
	python3 scripts/polaris/get_polaris_client_creds.py
	POLARIS_CLIENT_ID=$(cat polaris_client_id.txt) POLARIS_CLIENT_SECRET=$(cat polaris_client_secret.txt) python3 scripts/data_generators/generate_data.py polaris

stop_servers:
	cd scripts && docker compose kill
	cd scripts && docker compose rm -f
	tmux kill-ses -t polaris || true

data_clean:
	rm -rf data/generated

format-fix:
	rm -rf src/amalgamation/*
	python3 scripts/format.py --all --fix --noconfirm

format:
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
