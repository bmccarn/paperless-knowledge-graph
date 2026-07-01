# Local Docker Examples

These examples are for local testing. They keep Paperless-ngx separate from the
knowledge graph app, which mirrors the normal deployment shape:

```text
Paperless-ngx -> Paperless Knowledge Graph -> Neo4j + pgvector + Redis
```

## Option 1: Run Paperless Locally

If you do not already have a Paperless-ngx instance, start a disposable local
one:

```bash
cp examples/paperless.env.example examples/paperless.env
docker compose --env-file examples/paperless.env \
  -f examples/docker-compose.paperless.yml up -d
```

Open Paperless at `http://localhost:8000` and log in with the local example
admin account from `examples/paperless.env`. Create an API token in Paperless and
use it as `PAPERLESS_TOKEN` for the KG app.

If you already have Paperless running somewhere else, skip this compose file and
set `PAPERLESS_URL` / `PAPERLESS_TOKEN` to that instance.

## Option 2: Run Only the KG Datastores

Use this when you want Neo4j, Postgres/pgvector, and Redis locally but plan to run
the KG backend directly on your host:

```bash
docker compose -f examples/docker-compose.datastores.yml up -d
```

Then set the host-mode values in your `.env`:

```env
NEO4J_URI=bolt://localhost:7687
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
REDIS_URL=redis://localhost:6379
```

## Run the Full KG App

For the usual Docker path, copy the sample KG environment to the repo root,
fill in `PAPERLESS_TOKEN` and the LiteLLM values, then start the main compose:

```bash
cp examples/kg-local.env.example .env
# edit .env
docker compose up -d
```

The main `docker-compose.yml` already includes the KG backend, frontend, Neo4j,
pgvector, and Redis. Paperless still runs separately.

Local ports:

| Service | URL |
| --- | --- |
| KG frontend | http://localhost:3001 |
| KG API | http://localhost:8484 |
| Neo4j Browser | http://localhost:7474 |
| Paperless example | http://localhost:8000 |

After startup:

```bash
curl http://localhost:8484/health
curl -X POST http://localhost:8484/sync
```

Use `/reindex` instead of `/sync` when you want to clear and rebuild the whole
graph/vector index from Paperless.
