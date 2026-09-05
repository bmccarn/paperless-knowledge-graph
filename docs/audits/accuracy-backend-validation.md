# Reproducing backend validation

Use Python 3.12 and install `requirements.lock` with `pip install --require-hashes -r requirements.lock` in an isolated environment. From the repository root, the offline suite is:

```sh
python -m unittest discover -s tests -t . -v
```

It imports real application modules, forces synthetic provider settings, and explicitly skips datastore integration unless the corresponding test URLs are supplied. The final offline container run discovered 153 tests: 128 passed and 25 datastore cases skipped.

## Complete datastore run

Create fresh disposable services on these loopback ports. Fixtures create and replace test data; use these isolated services rather than an application database.

| Service | Image used | Configuration |
| --- | --- | --- |
| Neo4j | `neo4j:5-community` (reported 5.26.30) | HTTP 17474, Bolt 17687, authentication disabled, APOC installed, empty `neo4j` database |
| PostgreSQL | `pgvector/pgvector:pg16` | Port 15432, database `kg_accuracy`, user `postgres`, password `synthetic-test-only` |
| Redis | `redis:7-alpine` | Port 16379, no authentication |

The [graph validation procedure](graph-validation.md#neo4j-browsing-regressions) includes a disposable Neo4j launch command. PostgreSQL and Redis can be started without application volumes:

```sh
docker run --rm -d --name paperless-kg-accuracy-postgres \
  -p 127.0.0.1:15432:5432 \
  -e POSTGRES_DB=kg_accuracy -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=synthetic-test-only pgvector/pgvector:pg16
docker run --rm -d --name paperless-kg-accuracy-redis \
  -p 127.0.0.1:16379:6379 redis:7-alpine
```

Wait for all three databases to finish startup, then run:

```sh
NEO4J_TEST_URL=http://127.0.0.1:17474/db/neo4j/tx/commit \
NEO4J_TEST_URI=bolt://127.0.0.1:17687 \
NEO4J_TEST_BOLT=bolt://127.0.0.1:17687 \
STORAGE_TEST_NEO4J=bolt://127.0.0.1:17687 \
STORAGE_TEST_DSN=postgresql://postgres:synthetic-test-only@127.0.0.1:15432/kg_accuracy \
FEEDBACK_TEST_DSN=postgresql://postgres:synthetic-test-only@127.0.0.1:15432/kg_accuracy \
REDIS_TEST_URL=redis://127.0.0.1:16379/0 \
python -m unittest discover -s tests -t . -v
```

The final run passed all 153 tests with no skips. Its [raw log](2026-09-04-backend-validation.log) includes expected error messages from deliberate failure cases and the independent vector comparison. PostgreSQL fixtures use `storage_test` and `feedback_test` schemas. Neo4j fixture cleanup is scoped to synthetic nodes; the graph browsing fixture also refuses a nonempty database.

Stop the disposable containers after testing. No model endpoint or Paperless archive is required for these regressions. They establish behavior under controlled inputs, not model accuracy on real documents.
