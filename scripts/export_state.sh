#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-./exports/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT_DIR"

echo "Exporting Paperless KG state to $OUT_DIR"

if command -v docker >/dev/null 2>&1; then
  docker compose exec -T neo4j neo4j-admin database dump neo4j --to-path=/tmp >/dev/null
  docker compose cp neo4j:/tmp/neo4j.dump "$OUT_DIR/neo4j.dump" >/dev/null
  docker compose exec -T pgvector pg_dump -U "${POSTGRES_USER:-kguser}" "${POSTGRES_DB:-knowledge_graph}" > "$OUT_DIR/pgvector.sql"
else
  echo "docker is required for service-local exports" >&2
  exit 1
fi

cat > "$OUT_DIR/manifest.json" <<EOF
{
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "files": ["neo4j.dump", "pgvector.sql"]
}
EOF

echo "Export complete"
