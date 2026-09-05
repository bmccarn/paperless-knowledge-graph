# Reproduce completion validation

Use Python 3.12 with `pip install --require-hashes -r requirements.lock` and Node 24 with `npm --prefix frontend ci`. This run used Python 3.12.13, Node 24.20.0, Playwright 1.62.1 and the lockfiles recorded in `results.json`. The stdlib-only UI fixture ran with the host Python 3.14.7; CI selects Python 3.12.

Run the offline backend suite with `python -m unittest discover -s tests -t . -v`. For all 170 tests, explicitly start disposable localhost datastores and set these variables (the named services and test data from this run have been removed):

```sh
export NEO4J_TEST_URL=http://127.0.0.1:17474/db/neo4j/tx/commit
export NEO4J_TEST_URI=bolt://127.0.0.1:17687
export NEO4J_TEST_BOLT=bolt://127.0.0.1:17687
export STORAGE_TEST_NEO4J=bolt://127.0.0.1:17687
export STORAGE_TEST_DSN=postgresql://postgres:synthetic-test-only@127.0.0.1:15432/kg_accuracy
export FEEDBACK_TEST_DSN="$STORAGE_TEST_DSN"
export REDIS_TEST_URL=redis://127.0.0.1:16379/0
python -m unittest discover -s tests -t . -v
```

Test services: `pgvector/pgvector:pg16`, `neo4j:5-community` with APOC/authentication disabled, and `redis:7-alpine`, bound only to localhost. The test modules enforce localhost targets and use disposable schemas/data. Never point these variables at a real application database.

Run `npm --prefix frontend test`, `npm --prefix frontend run lint`, `npm --prefix frontend run typecheck`, and `npm --prefix frontend run build`. Install Chromium from the frontend directory with `npx --no-install playwright install chromium` (Linux CI also uses `--with-deps`). Then run `npm --prefix frontend run test:browser`; it owns ephemeral localhost ports, verifies the synthetic fixture marker, tests the production standalone frontend, and stops its children. Set `UI_TEST_ARTIFACTS` to a new directory to preserve these historical artifacts.

Local images were built with `docker build --platform linux/amd64 -t paperless-backend-closure:2026-09-04 .` and `docker build --platform linux/amd64 -t paperless-frontend-closure:2026-09-04 frontend`. For network-disabled backend-image tests, mount the repository's `tests`, `scripts`, and `evals` directories read-only at their corresponding `/app/` paths and run the same unittest discovery command. Datastore tests intentionally skip in that mode. `actionlint .github/workflows/container-images.yml`, `npm --prefix frontend audit --json` and `git diff --check` complete the checks.

The red logs are historical demonstrations of incorrect behavior, not failures of the final suite. They may reference earlier test line numbers. The current tests and final source hashes are authoritative for this closure.

Recorded logs normalize line endings and trailing whitespace for repository checks; diagnostic text and test outcomes are retained.
