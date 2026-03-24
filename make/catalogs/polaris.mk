POLARIS_ENV_FILE ?= scripts/envs/polaris.env

polaris-clone:
	@if [ ! -d ".catalogs/polaris" ]; then \
		echo "Cloning Polaris repository..."; \
		mkdir -p .catalogs && git clone https://github.com/apache/polaris.git .catalogs/polaris; \
		cd .catalogs/polaris && git checkout c940ded0f6be44d58ac6f0b6606c41cb08b53304; \
	else \
		echo "Polaris repository exists."; \
	fi

polaris-build:
	@if [ -f ".catalogs/polaris/runtime/server/build/quarkus-app/quarkus-run.jar" ]; then \
		echo "Polaris already built, skipping. Run 'make polaris-rebuild' to force."; \
	else \
		echo "Building Polaris..."; \
		cd .catalogs/polaris && ./gradlew :polaris-server:assemble -Dquarkus.container-image.build=true && ./gradlew --stop; \
	fi

polaris-rebuild:
	@echo "Rebuilding Polaris (clean)..."
	cd .catalogs/polaris && ./gradlew clean :polaris-server:assemble -Dquarkus.container-image.build=true --no-build-cache && ./gradlew --stop

polaris-stop:
	@echo "Stopping Polaris server..."
	cd .catalogs/polaris && ./gradlew --stop

polaris: polaris-clone polaris-build polaris-stop
	$(call stop_active_catalog)
	@echo "Starting Polaris server..."
	cd .catalogs/polaris && nohup ./gradlew :polaris-server:run > polaris-server.log 2> polaris-error.log &
	@echo "Waiting for Polaris to initialize..."
	@max_attempts=50; attempt=1; \
	while ! (curl -sf http://localhost:8182/healthcheck || curl -sf http://localhost:8182/q/health); do \
		if [ $$attempt -gt $$max_attempts ]; then \
			echo "Polaris failed to initialize after $$max_attempts attempts"; \
			exit 1; \
		fi; \
		echo "Waiting for Polaris to initialize (attempt $$attempt/$$max_attempts)..."; \
		sleep 5; \
		attempt=$$((attempt + 1)); \
	done
	@echo "Polaris is healthy"
	@echo "Quick-starting Polaris catalog and extracting credentials..."
	cd .catalogs/polaris && python3 -m venv . && . bin/activate && make client-regenerate && cd client/python && python3 -m pip install . && cd ../../../../ && \
	cd .catalogs/polaris && ../../scripts/polaris/quickstart_polaris_catalog.sh > user_credentials.json && cd ../../ && \
	python3 scripts/polaris/get_polaris_client_creds.py && \
	export POLARIS_CLIENT_ID=$$(cat tmp/polaris_client_id.txt); \
	export POLARIS_CLIENT_SECRET=$$(cat tmp/polaris_client_secret.txt); \
	envsubst '$$POLARIS_CLIENT_ID $$POLARIS_CLIENT_SECRET' < test/configs/polaris.json.template > test/configs/polaris.json
	$(call set_active_catalog,polaris)

polaris-data: polaris
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	if [ -f "$(POLARIS_ENV_FILE)" ]; then echo "Loading env from $(POLARIS_ENV_FILE)"; set -a; . ./$(POLARIS_ENV_FILE); set +a; fi && \
	export POLARIS_CLIENT_ID=$$(cat tmp/polaris_client_id.txt) && \
	export POLARIS_CLIENT_SECRET=$$(cat tmp/polaris_client_secret.txt) && \
	python3 -m scripts.data_generators.generate_data polaris $(if $(TEST),--test $(TEST))