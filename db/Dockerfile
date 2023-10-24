FROM postgres:16

ARG POSTGRES_USER=postgres
ARG POSTGRES_PASSWORD=postgres
ARG POSTGRES_DB=postgres

COPY ./sql/01-create_tables.sql /docker-entrypoint-initdb.d/
COPY ./sql/02-copy_data.sql /docker-entrypoint-initdb.d/
COPY ./data/. /data/


