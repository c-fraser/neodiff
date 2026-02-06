#!/usr/bin/env bash

if [ $# -gt 0 ]; then
    echo 'Demonstrate neodiff comparing Neo4j databases.'
    echo ''
    echo 'Usage:'
    echo '  ./run.sh'
    exit 0
fi

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "$0")" >/dev/null 2>&1; pwd -P)"

function cleanup() {
  cd "$SCRIPT_DIR" && docker compose down -v --remove-orphans
}

trap cleanup EXIT

cd "$SCRIPT_DIR" || exit 1

echo 'Starting Neo4j containers...'
docker compose up -d --remove-orphans

echo 'Waiting for source database'
until docker compose exec -T source cypher-shell -u neo4j -p password 'CALL db.ping()' 2>&1; do
    docker compose logs --tail=10 source 2>&1 | sed 's/^/    [source] /'
    sleep 3
done
echo 'Source database ready'

echo 'Waiting for target database'
until docker compose exec -T target cypher-shell -u neo4j -p password 'CALL db.ping()' 2>&1; do
    docker compose logs --tail=10 target 2>&1 | sed 's/^/    [target] /'
    sleep 3
done
echo 'Target database ready'

echo 'Loading source data'
docker compose exec -T source cypher-shell -u neo4j -p password -f /init.cypher

echo 'Loading target data'
docker compose exec -T target cypher-shell -u neo4j -p password -f /init.cypher

echo ''
echo 'Databases ready:'
echo '  Source: bolt://localhost:7687 (neo4j/password)'
echo '  Target: bolt://localhost:7688 (neo4j/password)'
echo ''
echo 'Running neodiff'
echo ''

cargo run -- --source 'bolt://neo4j:password@localhost:7687' --target 'bolt://neo4j:password@localhost:7688'
