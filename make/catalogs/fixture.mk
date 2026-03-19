FIXTURE_ENV_FILE ?= scripts/envs/fixture.env

fixture_stop:
	@echo "Stopping apache/iceberg-rest-fixture catalog..."
	(cd scripts && docker compose down -v)

fixture_start: fixture_stop
	$(call stop_active_catalog)
	@echo "Starting apache/iceberg-rest-fixture catalog..."
	rm -rf data/generated
	mkdir -p data/generated/iceberg/spark-rest
	mkdir -p data/generated/intermediates
	(cd scripts && docker compose up -d)
	$(call set_active_catalog,fixture)

fixture_data:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(FIXTURE_ENV_FILE)" ]; then echo "Loading env from $(FIXTURE_ENV_FILE)"; set -a; . ./$(FIXTURE_ENV_FILE); set +a; fi && \
	python3 -m scripts.data_generators.generate_data spark-rest

fixture: fixture_start fixture_data

fixture_data_local:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(FIXTURE_ENV_FILE)" ]; then echo "Loading env from $(FIXTURE_ENV_FILE)"; set -a; . ./$(FIXTURE_ENV_FILE); set +a; fi && \
	python3 -m scripts.data_generators.generate_data local

fixture_local: fixture_start fixture_data_local
