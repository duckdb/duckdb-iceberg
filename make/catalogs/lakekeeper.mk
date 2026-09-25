LAKEKEEPER_ENV_FILE ?= scripts/envs/lakekeeper.env
LAKEKEEPER_SPARK_LOCAL_DIR ?= /home/jovyan/.spark-local

lakekeeper-clone:
	@if [ ! -d ".catalogs/lakekeeper" ]; then \
		echo "Cloning Lakekeeper repository..."; \
		mkdir -p .catalogs && git clone https://github.com/lakekeeper/lakekeeper.git .catalogs/lakekeeper; \
		cd .catalogs/lakekeeper && git checkout 0648c99789dbaf42c23a45753f53f67db38fddd2 && git apply ../../.github/patches/lakekeeper_docker_compose.patch; \
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

lakekeeper: lakekeeper-clone lakekeeper-stop
	$(call stop_active_catalog)
	@echo "Starting Lakekeeper catalog..."
	@grep -q '127.0.0.1 silo' /etc/hosts || (echo "Adding silo host entry..." && echo "127.0.0.1 silo" | sudo tee -a /etc/hosts)
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
	$(call set_active_catalog,lakekeeper)

lakekeeper-data: lakekeeper
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	python3 -m pytest scripts/data_generators/test_generate_data.py $(if $(TEST),-k $(TEST)) -vv
