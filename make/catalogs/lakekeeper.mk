LAKEKEEPER_ENV_FILE ?= scripts/envs/lakekeeper.env

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

lakekeeper: lakekeeper-clone lakekeeper-stop
	$(call stop_active_catalog)
	@echo "Starting Lakekeeper catalog..."
	@grep -q '127.0.0.1 minio' /etc/hosts || (echo "Adding minio host entry..." && echo "127.0.0.1 minio" | sudo tee -a /etc/hosts)
	(cd .catalogs/lakekeeper/examples/access-control-simple && docker compose up -d)
	@echo "Bootstrapping Lakekeeper..."
	cd .catalogs/lakekeeper/examples/access-control-simple && \
	docker compose exec jupyter start.sh bash -lc "\
		jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/01-Bootstrap.ipynb && \
		jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/02-Create-Warehouse.ipynb && \
		jupyter nbconvert --to notebook --execute --output-dir=/tmp /home/jovyan/examples/03-01-Spark.ipynb"
	$(call set_active_catalog,lakekeeper)

lakekeeper-data: lakekeeper
	@echo "Setting up venv-spark4 and generating data..."
	python3 -m venv .venv-spark4 && \
	. .venv-spark4/bin/activate && \
	python3 -m pip install -r scripts/requirements.txt && \
	python3 -m scripts.data_generators.generate_data lakekeeper $(if $(TEST),--test $(TEST))