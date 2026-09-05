# Completion review reproduction artifacts

These probes record defects in the reviewed snapshot. **They are not passing acceptance tests.** Python probes print observed state; the browser probes assert the currently defective behavior. A zero exit status means the probe ran successfully, not that the application is correct. After repair, convert their scenarios into regression assertions for the intended behavior.

Run from the repository root using Python 3.12 with the hashed backend dependencies installed. All Python probes call `configure_test_environment()` before importing configured application modules. The five defect probes use controlled dependencies and require no network or production data:

```sh
python docs/audits/completion-review/summary-source-probe.py
python docs/audits/completion-review/entity-review-race-probe.py
python docs/audits/completion-review/metadata-sync-probe.py
python docs/audits/completion-review/reindex-postprocess-probe.py
python docs/audits/completion-review/cache-hit-race-probe.py
```

Expected defective observations are recorded in the corresponding `*-result.json` files and explained in the [completion review](../2026-09-04-completion-review.md).

## Browser races

Run the synthetic backend and actual frontend in separate terminals:

```sh
python3 frontend/tests/accuracy_fixture.py --port 8487
```

```sh
BACKEND_URL=http://127.0.0.1:8487 NEXT_TELEMETRY_DISABLED=1 \
  npm --prefix frontend run dev -- --hostname 127.0.0.1 --port 3006
```

Use Node 24 with Playwright 1.62.1 and its installed Chromium. The [earlier browser instructions](../accuracy-ui-validation.md#reproduce) show how to install optional tooling outside this repository. Then run:

```sh
PLAYWRIGHT_MODULE=/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs \
  node docs/audits/completion-review/conversation-switch-race.mjs
PLAYWRIGHT_MODULE=/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs \
  node docs/audits/completion-review/document-reindex-search-race.mjs
PLAYWRIGHT_MODULE=/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs \
  node docs/audits/completion-review/graph-initial-selection-race.mjs
```

Each browser probe rejects non-localhost destinations and requires the synthetic fixture identity. `UI_TEST_BASE_URL` overrides the default `http://127.0.0.1:3006`; `UI_TEST_SCREENSHOT` overrides its temporary screenshot path. Matching captured PNGs are retained here. The probes control request completion order instead of depending on arbitrary network delays. Stop both servers afterward.

## Enabled provider adapter check

```sh
python docs/audits/completion-review/enabled-model-adapter-probe.py
```

This positive check enables the actual Strands/LiteLLM adapter and starts a temporary HTTP server on a random loopback port. It verifies the OpenAI-compatible streaming transport and parsed JSON result. The server supplies an invented response and shuts down afterward. No real provider or model-quality evaluation is involved.

For the 153-test run against disposable Neo4j/PostgreSQL/Redis, follow the [backend validation procedure](../accuracy-backend-validation.md). The new review's raw output is in `backend-datastore-tests.log`.
