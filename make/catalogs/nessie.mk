NESSIE_ENV_FILE ?= scripts/envs/nessie.env

nessie-clone:
	@if [ ! -d ".catalogs/nessie" ]; then \
		echo "Cloning Nessie repository..."; \
		mkdir -p .catalogs && git clone https://github.com/projectnessie/nessie.git .catalogs/nessie; \
		cd .catalogs/nessie && git checkout c2003adc99257c0d6ee2e3fa13c1dd0f79d0c60e && git apply ../../.github/patches/nessie_docker_compose.patch; \
	else \
		echo "Nessie repository exists."; \
	fi

nessie-stop:
	@echo "Stopping Nessie catalog..."
	(cd .catalogs/nessie/docker/catalog-auth-s3 && docker compose down -v)

nessie: nessie-clone nessie-stop
	$(call stop_active_catalog)
	@echo "Starting Nessie catalog..."
	(cd .catalogs/nessie/docker/catalog-auth-s3 && docker compose up -d)
	$(call set_active_catalog,nessie)

nessie-data-only:
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(NESSIE_ENV_FILE)" ]; then echo "Loading env from $(NESSIE_ENV_FILE)"; set -a; . ./$(NESSIE_ENV_FILE); set +a; fi && \
	python3 -m scripts.data_generators.generate_data nessie $(if $(TEST),--test $(TEST))

nessie-data: nessie nessie-data-only