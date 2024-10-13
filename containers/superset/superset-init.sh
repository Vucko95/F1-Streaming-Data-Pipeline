#!/bin/bash

# create Admin user, you can read these values from env or anywhere else possible
superset fab create-admin --username "$ADMIN_USERNAME" --firstname Superset --lastname Admin --email "$ADMIN_EMAIL" --password "$ADMIN_PASSWORD"

# Upgrading Superset metastore
superset db upgrade

# setup roles and permissions
superset superset init 

superset set_database_uri --database_name PostgreSQL --uri "$SUPERSET_DATABASE_URI"


# Starting server
/bin/sh -c /usr/bin/run-server.sh &

# Wait for Superset to be healthy
./wait-for-superset.sh

tail -f /dev/null
