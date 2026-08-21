LAKEKEEPER_ENV_FILE ?= scripts/envs/lakekeeper.env
LAKEKEEPER_SPARK_LOCAL_DIR ?= /home/jovyan/.spark-local

lakekeeper-clone:
	@if [ ! -d ".catalogs/lakekeeper" ]; then \
		echo "Cloning Lakekeeper repository..."; \
		mkdir -p .catalogs && git clone https://github.com/lakekeeper/lakekeeper.git .catalogs/lakekeeper; \
		cd .catalogs/lakekeeper && git checkout f6aaa4570cf62d54943581e7a00b9f882d9c992d && git apply ../../.github/patches/lakekeeper_docker_compose.patch; \
	else \
		echo "Lakekeeper repository exists."; \
	fi

lakekeeper-stop:
	@echo "Stopping Lakekeeper catalog..."
	(cd .catalogs/lakekeeper/examples/access-control-simple && docker compose down -v)

lakekeeper-configure-auth:
	@echo "Allowing local HTTP OAuth for Lakekeeper Keycloak realm..."
	(cd .catalogs/lakekeeper/examples/access-control-simple && \
	docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh config credentials \
		--server http://localhost:8080 \
		--realm master \
		--user admin \
		--password admin && \
	docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh update realms/iceberg \
		-s sslRequired=NONE)

lakekeeper-remote-signing:
	@echo "Creating the Lakekeeper warehouse used by the remote signing tests..."
	cp scripts/lakekeeper/remote_signing_setup.py .catalogs/lakekeeper/examples/access-control-simple/notebooks/
	(cd .catalogs/lakekeeper/examples/access-control-simple && \
	docker compose run --rm --no-deps \
		-e SPARK_LOCAL_DIRS='$(LAKEKEEPER_SPARK_LOCAL_DIR)' \
		jupyter bash -lc '\
			rm -rf "$$SPARK_LOCAL_DIRS" /tmp/blockmgr-* /tmp/spark-* && \
			mkdir -p "$$SPARK_LOCAL_DIRS" && \
			python /home/jovyan/examples/remote_signing_setup.py')

lakekeeper: lakekeeper-clone lakekeeper-stop
	$(call stop_active_catalog)
	@echo "Starting Lakekeeper catalog..."
	@grep -q '127.0.0.1 minio' /etc/hosts || (echo "Adding minio host entry..." && echo "127.0.0.1 minio" | sudo tee -a /etc/hosts)
	(cd .catalogs/lakekeeper/examples/access-control-simple && docker compose up -d)
	$(MAKE) lakekeeper-configure-auth
	@echo "Bootstrapping Lakekeeper..."
	cd .catalogs/lakekeeper/examples/access-control-simple && \
	docker compose run --rm --no-deps \
		-e SPARK_LOCAL_DIRS='$(LAKEKEEPER_SPARK_LOCAL_DIR)' \
		jupyter bash -lc '\
			rm -rf "$$SPARK_LOCAL_DIRS" /tmp/blockmgr-* /tmp/spark-* && \
			mkdir -p "$$SPARK_LOCAL_DIRS" && \
			jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/01-Bootstrap.ipynb && \
			jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/02-Create-Warehouse.ipynb && \
			jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/03-01-Spark.ipynb'
	$(MAKE) lakekeeper-remote-signing
	$(call set_active_catalog,lakekeeper)

lakekeeper-data: lakekeeper
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	python3 -m pytest scripts/data_generators/test_generate_data.py $(if $(TEST),-k $(TEST)) -vv
