POLARIS_ENV_FILE ?= scripts/envs/polaris.env

polaris-clone:
	@if [ ! -d ".catalogs/polaris" ]; then \
		echo "Cloning Polaris repository..."; \
		mkdir -p .catalogs && git clone https://github.com/apache/polaris.git .catalogs/polaris; \
		cd .catalogs/polaris && git checkout release/1.4.x; \
	else \
		echo "Polaris repository exists."; \
	fi

polaris-stop:
	@echo "Stopping Polaris catalog..."
	@if [ -d ".catalogs/polaris/site/content/guides/minio" ]; then \
		(cd .catalogs/polaris/site/content/guides/minio && docker compose down -v); \
	else \
		echo "Polaris minio directory not found, skipping stop."; \
	fi

polaris: polaris-clone polaris-stop
	$(call stop_active_catalog)
	@echo "Starting Polaris catalog..."
	(cd .catalogs/polaris/site/content/guides/minio && docker compose up -d)
	$(call set_active_catalog,polaris)

polaris-data: polaris
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(POLARIS_ENV_FILE)" ]; then echo "Loading env from $(POLARIS_ENV_FILE)"; set -a; . ./$(POLARIS_ENV_FILE); set +a; fi && \
	python3 -m scripts.data_generators.generate_data polaris $(if $(TEST),--test $(TEST))
