GRAVITINO_ENV_FILE ?= scripts/envs/gravitino.env
GRAVITINO_COMPOSE_FILE ?= scripts/gravitino/docker-compose.yml
GRAVITINO_COMPOSE := docker compose -f $(GRAVITINO_COMPOSE_FILE)

gravitino-stop:
	@echo "Stopping Gravitino catalog..."
	@$(GRAVITINO_COMPOSE) down -v --remove-orphans

gravitino: gravitino-stop
	$(call stop_active_catalog)
	@echo "Starting Gravitino catalog..."
	@grep -q '127.0.0.1 minio' /etc/hosts || (echo "Adding minio host entry..." && echo "127.0.0.1 minio" | sudo tee -a /etc/hosts)
	@mkdir -p data/generated/iceberg/gravitino
	@$(GRAVITINO_COMPOSE) up -d
	@attempt=1; max_attempts=60; \
	until curl -sf http://127.0.0.1:9001/iceberg/v1/config >/dev/null; do \
		if [ $$attempt -ge $$max_attempts ]; then \
			echo "Gravitino failed to initialize after $$max_attempts attempts"; \
			$(GRAVITINO_COMPOSE) logs; \
			exit 1; \
		fi; \
		echo "Waiting for Gravitino (attempt $$attempt/$$max_attempts)..."; \
		sleep 2; \
		attempt=$$((attempt + 1)); \
	done
	$(call set_active_catalog,gravitino)

gravitino-data-only:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(GRAVITINO_ENV_FILE)" ]; then echo "Loading env from $(GRAVITINO_ENV_FILE)"; set -a; . ./$(GRAVITINO_ENV_FILE); set +a; fi && \
	python3 -m pytest scripts/data_generators/test_generate_data.py $(if $(TEST),-k $(TEST)) -vv

gravitino-data: gravitino gravitino-data-only
