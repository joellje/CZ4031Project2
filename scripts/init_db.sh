#!/usr/bin/env bash
set -x
set -eo pipefail

DB_USER="${POSTGRES_USER:=postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD:=postgres}"
DB_NAME="${POSTGRES_DB:=postgres}"
DB_PORT="${POSTGRES_PORT:=5432}"

docker build \
  --build-arg POSTGRES_USER="$DB_USER" \
  --build-arg POSTGRES_PASSWORD="$DB_PASSWORD" \
  --build-arg POSTGRES_DB="$DB_NAME" \
  -t cz4032_project2 \
  ../db/

RUNNING_POSTGRES_CONTAINER=$(docker ps --filter 'name=postgres' --format '{{.ID}}')
if [[ -n $RUNNING_POSTGRES_CONTAINER ]]; then
  echo >&2 "there is a postgres container already running, kill it with"
  echo >&2 "    docker kill ${RUNNING_POSTGRES_CONTAINER}"
  exit 1
fi
docker run \
    -e POSTGRES_USER=${DB_USER} \
    -e POSTGRES_PASSWORD=${DB_PASSWORD} \
    -e POSTGRES_DB=${DB_NAME} \
    -p "${DB_PORT}":5432 \
    -d \
    --name "postgres_cz4032_project2_$(date '+%s')" \
    cz4032_project2

>&2 echo "Postgres is up and running on port ${DB_PORT}"
