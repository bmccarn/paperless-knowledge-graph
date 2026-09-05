# Graph browsing validation

The recorded initial graph-rework results below are historical. The [implementation report](2026-09-04-implementation-report.md) and [browser report](accuracy-ui-validation.md) contain the combined current validation on Python 3.12 and Node 24. The current frontend suite has nine graph tests and two answer-rendering tests.

## Frontend regressions

```bash
npm --prefix frontend test
npm --prefix frontend run lint
cd frontend
npx --no-install tsc --noEmit --incremental false
```

The eight Node tests exercise graph normalization/merging, stable edge updates, distinct source support, missing/dangling identities, fallback endpoint fields, source references, direction, HTML escaping and isolation of response metadata. The first run before fixing merge behavior failed the duplicate-expansion and stable-ID-update cases. All eight pass after the fix.

The compiled test helper goes into ignored `frontend/.test-build/`. No new npm dependencies were added.

## Neo4j browsing regressions

Run against an **empty disposable database**, never an existing archive. The tests refuse a nonempty database and non-local HTTP host. They seed five synthetic nodes, execute the actual browsing methods' Cypher against Neo4j, and remove only their fixture nodes afterward.

```bash
docker run --rm -d --name paperless-kg-graph-audit-20260904 \
  -p 127.0.0.1:17474:7474 -p 127.0.0.1:17687:7687 \
  -e NEO4J_AUTH=none -e 'NEO4J_PLUGINS=["apoc"]' \
  -e NEO4J_server_memory_heap_initial__size=256m \
  -e NEO4J_server_memory_heap_max__size=512m \
  -e NEO4J_server_memory_pagecache_size=128m \
  neo4j:5-community
```

Wait for the database to finish startup (`docker logs paperless-kg-graph-audit-20260904`), then:

```bash
NEO4J_TEST_URL=http://127.0.0.1:17474/db/neo4j/tx/commit \
  python3 -m unittest discover -s tests -v
docker stop paperless-kg-graph-audit-20260904
```

The container has no application volume mounts, listens only on loopback, and is removed when stopped. The audit used image digest `sha256:037cf5756f0135cbfd66b739b6df7c7c4bb100f9ce11602f6f9538e17e02c74d`; use `neo4j@sha256:037cf5756f0135cbfd66b739b6df7c7c4bb100f9ce11602f6f9538e17e02c74d` instead of the tag to repeat with that image.

The six checks cover:

1. A document without a UUID expands with valid endpoints and its recorded source.
2. Initial loading and expansion return stable, unique relationship IDs.
3. A missing APOC procedure falls back to the requested one-hop neighborhood.
4. The fallback retains an isolated document with zero edges.
5. Other database errors propagate rather than being hidden by fallback.
6. Node details report actual incoming/outgoing direction and resolve Paperless IDs.

The first run against the original browsing code failed one assertion and raised three errors. All six passed after the fix. The missing-APOC test substitutes an absent procedure name so Neo4j itself returns the missing-procedure error, then executes the application's real fallback query. Authorization-error propagation uses a controlled injected error.

The test now imports the real `app.graph` module with synthetic client settings and uses a small HTTP driver adapter. Install the locked backend dependencies on Python 3.12 before running it. This validates method/query behavior against a real database; separate tests cover routing, Bolt storage, and lifespan. Without `NEO4J_TEST_URL`, these integration tests are explicitly skipped.

## Historical initial check results

The final checks passed: eight frontend regressions, six real-Neo4j browsing regressions, frontend lint with zero errors/warnings, TypeScript checking, the production build (all ten static pages generated), Python parsing for 23 application/script/test files, and patch whitespace checking. Both audit probe scripts still reproduce the open backend defects.

The database reported Neo4j 5.26.30. Local tools were Python 3.14.7 and Node 26.8.1; deployment's Python 3.12/Node 20 runtimes were not exercised. The production build emitted a Node deprecation warning but completed successfully. Temporary fixture/frontend processes and the disposable Neo4j container were stopped after validation.

## Interactive UI fixture

Run these in separate terminals from the repository root:

```bash
python3 frontend/tests/graph_fixture.py
```

```bash
BACKEND_URL=http://127.0.0.1:8485 \
  npm --prefix frontend run dev -- --hostname 127.0.0.1 --port 3100
```

Open `http://127.0.0.1:3100/graph`. The fixture contains only invented names and document records. It implements graph endpoints only; dashboard health and document detail requests are intentionally outside its scope. Source links are checked for their correct destination, not full document-page functionality. Stop both processes when done.

Manual acceptance checks performed:

- Start in 2D; labels are readable, documents appear as squares, entities as circles.
- Search for Example Utility; inspect its incoming links and recorded source references.
- Inspect Alex Example; its relationship is outgoing. Expand repeatedly; count stays at four nodes / three relationships.
- Switch to 3D and back; filter to documents, showing two nodes / zero edges.
- Browse and inspect a document at 390 × 844; the inspector fits and its close/source controls remain accessible.
- Inferred connections are labeled and source descriptions render literal markup as text.
- A nonexistent search shows an empty result; `fixture-error` returns a synthetic search failure and the UI can recover with another search.

The browser checks use a small synthetic graph. No production document, model call, corpus-scale timing, dense-graph frame-rate claim, or actual mobile touch-device test is implied. The optional 3D renderer was checked on the available Chrome/WebGL setup, not across devices.
