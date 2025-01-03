
set -ex

docker-compose kill
docker-compose rm -f
docker-compose up -d
docker-compose logs -f mc

pip3 install -r requirements.txt

python3 provision.py

SQL=$(cat <<-END

CREATE SECRET (
  TYPE S3,
  KEY_ID 'admin',
  SECRET 'password',
  ENDPOINT '127.0.0.1:9000',
  URL_STYLE 'path',
  USE_SSL 0
);

SELECT count(*) FROM ICEBERG_SCAN('table_unpartitioned', catalog_type='rest', catalog_uri='127.0.0.1:8181', catalog_namespace='default');
END
)

if test -f "../build/release/duckdb"
then
  # in CI
  ../build/release/duckdb -s "$SQL"
else
  duckdb -s "$SQL"
fi
