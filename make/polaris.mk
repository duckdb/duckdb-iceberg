# Clone Polaris repo if folder doesn't exist
polaris:
	@if [ ! -d "polaris_catalog" ]; then \
		echo "Cloning Polaris repository..."; \
		git clone https://github.com/apache/polaris.git polaris_catalog; \
		cd polaris_catalog && git checkout c940ded0f6be44d58ac6f0b6606c41cb08b53304; \
	else \
		echo "Polaris repository already exists."; \
	fi

REQUIRED_POLARIS_VARS = \
	POLARIS_CLIENT_ID \
	POLARIS_CLIENT_SECRET

check_polaris_env:
	@for var in $(REQUIRED_POLARIS_VARS); do \
		if [ -z "$${!var}" ]; then \
			echo "ERROR: $$var is not set."; \
			exit 1; \
		fi; \
	done

data_polaris: polaris
	@echo "Building Polaris..."
	cd polaris_catalog && \
	./gradlew clean :polaris-server:assemble -Dquarkus.container-image.build=true --no-build-cache && \
	./gradlew --stop

	@echo "Starting Polaris server..."
	cd polaris_catalog && \
	nohup ./gradlew :polaris-server:run > polaris-server.log 2> polaris-error.log &

	@echo "Waiting for Polaris initialization..."
	@max_attempts=50; \
	attempt=1; \
	while ! (curl -sf http://localhost:8182/healthcheck || curl -sf http://localhost:8182/q/health); do \
		if [ $$attempt -gt $$max_attempts ]; then \
			echo "Polaris failed to initialize after $$max_attempts attempts"; \
			exit 1; \
		fi; \
		echo "Waiting for Polaris to initialize ($$attempt/$$max_attempts)..."; \
		sleep 5; \
		attempt=$$((attempt + 1)); \
	done
	@echo "Polaris is healthy"

	@echo "Generating Polaris credentials..."
	cd polaris_catalog && ../scripts/polaris/setup_polaris_catalog.sh > user_credentials.json

	python3 scripts/polaris/get_polaris_client_creds.py
	export POLARIS_CLIENT_ID=$$(cat polaris_client_id.txt); \
	export POLARIS_CLIENT_SECRET=$$(cat polaris_client_secret.txt); \
	envsubst '$$POLARIS_CLIENT_ID $$POLARIS_CLIENT_SECRET' < test/configs/polaris.json.template > test/configs/polaris.json; \
	python3 -m scripts.data_generators.generate_data polaris;

data_polaris_clean: polaris
	@echo "Shutting down Polaris server..."
	@pkill -f "polaris-server:run" || true
	rm polaris_client_id.txt
	rm polaris_client_secret.txt
