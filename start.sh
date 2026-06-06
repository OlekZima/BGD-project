#!/usr/bin/env bash
set -e

if [ ! -f .env ]; then
    set +H
    read -r -s -p "Enter PostgreSQL password: " PG_PWD_INPUT
    echo
    printf 'POSTGRES_PASSWORD=%s\n' "${PG_PWD_INPUT}" > .env
    echo "Password saved to .env"
fi

docker compose up -d

echo ""
echo "PostgreSQL is starting — first run initialises the schema in a few seconds."
echo "Monitor progress:    docker logs -f bgd_postgres"
echo "Connect when ready:  psql postgresql://citibike:<password>@localhost:5432/citibike"
