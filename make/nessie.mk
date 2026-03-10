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

data_nessie_clean: nessie check_nessie_env
	@echo "Shutting down Nessie catalog..."
	docker compose -f nessie/docker/catalog-auth-s3/docker-compose.yml down