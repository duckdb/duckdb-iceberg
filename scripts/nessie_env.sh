# NOTE: you should run this script with 'source scripts/nessie_env.sh' to set these variables in your current shell

export NESSIE_SERVER_AVAILABLE=1
export S3_KEY_ID="minioadmin"
export S3_SECRET="minioadmin"
export S3_ENDPOINT="http://127.0.0.1:9002"
export ICEBERG_CLIENT_ID="client1"
export ICEBERG_CLIENT_SECRET="s3cr3t"
export ICEBERG_ENDPOINT="http://127.0.0.1:19120/iceberg/main/"
export OAUTH2_SERVER_URI="http://127.0.0.1:8080/realms/iceberg/protocol/openid-connect/token"
export WAREHOUSE="warehouse"
