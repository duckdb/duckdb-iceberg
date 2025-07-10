
if test ! -f "./scripts/docker-compose.yml"
then
  # in CI
  echo "Please run from duckdb root."
  exit 1
fi

rm -rf data/generated
mkdir -p data/generated/iceberg/spark-rest
mkdir -p data/generated/intermediates

# cd into scripts where docker-compose file is.
cd scripts

# need to have this happen in the background
set -ex

docker compose kill
docker compose rm -f
docker compose up --detach
