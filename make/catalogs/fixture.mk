FIXTURE_ENV_FILE ?= scripts/envs/fixture.env

fixture-stop:
	@echo "Stopping apache/iceberg-rest-fixture catalog..."
	(cd scripts && docker compose down -v)

fixture: fixture-stop
	$(call stop_active_catalog)
	@echo "Starting apache/iceberg-rest-fixture catalog..."
	mkdir -p data/generated/iceberg/fixture
	mkdir -p data/generated/intermediates
	(cd scripts && docker compose up -d)
	$(call set_active_catalog,fixture)

fixture-data: fixture
	@echo "Setting up venv-spark4 and generating data..."
	mkdir -p data/generated/iceberg/fixture && \
	mkdir -p data/generated/intermediates && \
	rm -rf data/generated/iceberg/fixture/* && \
	rm -rf data/generated/intermediates/* && \
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(FIXTURE_ENV_FILE)" ]; then echo "Loading env from $(FIXTURE_ENV_FILE)"; set -a; . ./$(FIXTURE_ENV_FILE); set +a; fi && \
	python3 -m pytest scripts/data_generators/test_generate_data.py $(if $(TEST),-k $(TEST)) -vv

fixture-latest-stop:
	@echo "Stopping apache/iceberg-rest-fixture:latest catalog..."
	(cd scripts && FIXTURE_DATA_DIR=fixture-latest docker compose down -v)

fixture-latest: fixture-latest-stop
	$(call stop_active_catalog)
	@echo "Starting apache/iceberg-rest-fixture:latest catalog..."
	mkdir -p data/generated/iceberg/fixture-latest
	mkdir -p data/generated/intermediates
	(cd scripts && ICEBERG_REST_FIXTURE_IMAGE=apache/iceberg-rest-fixture:latest FIXTURE_DATA_DIR=fixture-latest docker compose up -d)
	$(call set_active_catalog,fixture-latest)

fixture-latest-data: fixture-latest
	@echo "Setting up venv-spark4 and generating data..."
	mkdir -p data/generated/iceberg/fixture-latest && \
	mkdir -p data/generated/intermediates && \
	rm -rf data/generated/iceberg/fixture-latest/* && \
	rm -rf data/generated/intermediates/* && \
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	python3 -m pip install pyspark==4.1.0 && \
	if [ -f "$(FIXTURE_ENV_FILE)" ]; then echo "Loading env from $(FIXTURE_ENV_FILE)"; set -a; . ./$(FIXTURE_ENV_FILE); set +a; fi && \
	python3 -m pytest scripts/data_generators/test_generate_data.py $(if $(TEST),-k $(TEST)) -vv

fixture-data-local: fixture
	@echo "Setting up venv-spark4 and generating data..."
	$(call set_active_catalog,local)
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(FIXTURE_ENV_FILE)" ]; then echo "Loading env from $(FIXTURE_ENV_FILE)"; set -a; . ./$(FIXTURE_ENV_FILE); set +a; fi && \
	python3 -m pytest scripts/data_generators/test_generate_data.py $(if $(TEST),-k $(TEST)) -vv
	$(call set_active_catalog,fixture)
