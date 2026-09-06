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
- `python3 scripts/eval_harness.py --base-url http://localhost:8484` runs query evaluations, requires the expected document corpus, and may incur model usage.

## Audit evidence

Start with recent history and follow the affected data paths through callers, storage, and frontend consumers. For each finding, record the trigger, practical consequence, exact source location, recommended change, and a behavior check. Report measured timings separately from performance hypotheses.

Prioritize data integrity and incorrect answers, then reliability, performance, and maintainability. Capture architecture candidates using `improve-codebase-architecture` and `codebase-design`; use `code-review` for a specified commit/branch diff and a known spec.
