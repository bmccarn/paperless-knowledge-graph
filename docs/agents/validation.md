# Validation and audit workflow

## Local checks

The backend image uses Python 3.12 and the frontend uses Node 24 LTS. Package manifests and lockfiles define the exact dependency versions.

- Install backend dependencies: `python -m pip install --require-hashes -r requirements.lock` in an isolated Python 3.12 environment.
- Offline backend behavior regressions: `python -m unittest discover -s tests -t . -v`. Tests explicitly configure synthetic local-only clients; database integration requires opt-in disposable datastore URLs.

- Parse Python without importing application modules or opening connections: `python3 -c 'import ast,pathlib; files=[*pathlib.Path("app").glob("*.py"),*pathlib.Path("scripts").glob("*.py")]; [ast.parse(p.read_text(),filename=str(p)) for p in files]; print(f"Parsed {len(files)} Python files")'`.
- Install frontend dependencies from the lockfile: `npm --prefix frontend ci`.
- Lint: `npm --prefix frontend run lint`.
- Graph data regressions: `npm --prefix frontend test`.
- Type check: `npm --prefix frontend run typecheck`.
- Build: `npm --prefix frontend run build`. The layout downloads Google Fonts, so a build needs network access unless fonts have been made local.
- Production browser contracts: after building, install Chromium with `cd frontend && npx --no-install playwright install chromium`, then run `npm --prefix frontend run test:browser` from the repo root. The runner owns a localhost-only synthetic backend and the standalone frontend, closes them on exit, and stores evidence under `frontend/.browser-artifacts/` (override with `UI_TEST_ARTIFACTS`). CI installs Chromium's Linux dependencies too. The suite covers conversation isolation, document refresh, graph loading, source inspection, feedback and domain browsing; it does not call production models.
- Check patch whitespace: `git diff --check`.

Graph browsing has frontend regression tests and an opt-in real-Neo4j suite in `tests/test_graph_browser.py`; see [graph validation](../audits/graph-validation.md). Query delivery, extraction, ingestion, review decisions, feedback and cache behavior have offline regression tests. Those controlled fixtures validate acceptance rules and failure handling; measuring model accuracy requires a reviewed corpus. Record failing checks with their existing error counts; do not silently weaken rules to make them pass.

Pull requests and `main` pushes run backend behavior checks offline and against disposable local Neo4j/APOC, pgvector PostgreSQL and Redis services, plus frontend regressions, lint, type checks, advisory checks, a production build and production browser contracts. The identity policy matrix can also run without application dependencies: `python3 -m unittest tests.test_entity_policy -v`. These CI services use synthetic fixtures only, never production credentials or workloads. Image publication on `main` requires both validation jobs. See [dependency and delivery validation](../specs/dependency-and-delivery-validation.md) for the checked dependency refresh and local validation evidence, and [completion closure](../audits/2026-09-04-completion-closure.md) for the C01–C08 regression results.

## Integration checks

Use disposable local datastores and Paperless from `examples/README.md`. Importing backend modules constructs configured clients; Redis I/O is lazy and async callers offload it. Application startup initializes databases. Tests set `KG_ENV_FILE` empty and use synthetic configuration. Inspect the target configuration before running the application.

- `python3 scripts/api_smoke_tests.py --base-url http://localhost:8484` checks status endpoints and posts a quick query by default, so it may incur model usage. `--mutating` also starts a sync.
- `python3 scripts/kg_exact_drift_audit.py --base-url http://localhost:8484` checks exact ID drift. `--repair` mutates derived state.
- `python3 scripts/delete_stale_entity_embeddings.py --postgres-dsn <dsn> --neo4j-uri <uri>` lists entity embedding rows whose entity UUID is no longer in Neo4j. Run it once to remove rows that earlier versions left behind. `--apply` deletes the listed rows. Read the passwords from `PGPASSWORD` and `NEO4J_PASSWORD`.
- `python3 scripts/eval_harness.py --base-url http://localhost:8484` runs query evaluations, requires the expected document corpus, and may incur model usage.

## Audit evidence

Start with recent history and follow the affected data paths through callers, storage, and frontend consumers. For each finding, record the trigger, practical consequence, exact source location, recommended change, and a behavior check. Report measured timings separately from performance hypotheses.

Prioritize data integrity and incorrect answers, then reliability, performance, and maintainability. Capture architecture candidates using `improve-codebase-architecture` and `codebase-design`; use `code-review` for a specified commit/branch diff and a known spec.

## Screenshot and accessibility checks

The browser runner's final stage uses `playwright/test` against the same synthetic
backend and frontend. `tests/visual.spec.mjs` captures one main state per existing
page in desktop dark, desktop light, phone dark and phone light. Dialogs, expanded
rows and final answers are additional captures without screenshot gates. Add the
component gallery to this matrix in #113. Each capture attaches its axe violations,
incomplete results and visible text below 12 px to the HTML comparison report.

CI runs in `mcr.microsoft.com/playwright:v1.65.1-noble`. Screenshots use a fixed
clock, UTC, en-US, device scale factor 1, loaded fonts, disabled animation and
seeded graph randomness. Do not create application baselines on a developer machine.

Screenshot comparison stays capture-only until #111 merges. In the first PR
after #111, generate baselines with the manual workflow, commit the approved
images and set `UI_VISUAL_GATE` to `"1"`. Comparison uses zero differing pixels
and zero per-pixel threshold. Missing baselines fail without writing files.

To update baselines, dispatch **Container Images** on the ticket branch with
`update_visual_baselines` selected. Download the `visual-baselines` artifact,
review the `browser-comparison` artifact, and copy approved images into
`frontend/tests/baselines/`. Commit the images in the ticket PR. Run ordinary CI
again after the commit. The manual update never publishes application images.
The `browser-comparison` artifact includes `visual/comparison/index.html`, captures,
expected images and differences. CI uploads it even when a browser check fails.

`frontend/tests/accessibility-exceptions.json` records existing violations by
rule, route, state, affected targets and issue number. New targets and violations
fail. Remove a page's exceptions when its redesign fixes them; do not disable a
whole axe rule. `font-size` is the visible-text check and has the same exception
policy while the existing pages are converted. Incomplete axe results are reported
for review and do not fail the run.
