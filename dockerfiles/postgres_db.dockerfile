FROM postgres:14.5-bullseye

COPY create_database.sql /docker-entrypoint-initdb.d