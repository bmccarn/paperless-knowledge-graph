# Dependency refresh and publication gates

The audited baseline included known vulnerable frontend versions and used `npm install` in its Dockerfile. Container publication had no prerequisite behavior, lint, type, or build validation. This implementation refreshes both dependency locks and adds those prerequisites.

## Chosen change

- Upgrade the existing Next.js 16 line and matching lint configuration to the current stable 16.3.4 release, preserving the application's router and deployment model. Refresh React 19 to 19.2.8 and compatible transitive dependencies; inspect remaining advisories rather than forcing incompatible major upgrades.
- Use Node 24 LTS locally for validation, in CI, and in all frontend image stages. Install the committed frontend lock with `npm ci`; add a package typecheck script.
- Run backend offline behavior tests on Python 3.12 using the hashed lockfile, and frontend tests/lint/typecheck/build on pull requests and pushes. Publication depends on both jobs and is restricted to `main`; PR validation receives read-only repository permissions and never registry credentials.
- Keep database integration explicitly opt-in and model responses controlled. Passing local checks does not claim a hosted Actions run or container publication occurred.

## Current primary-source verification

On 2026-09-04 the [npm Next.js manifest](https://registry.npmjs.org/next/latest) reports 16.3.4 and the [React manifest](https://registry.npmjs.org/react/latest) reports 19.2.8. The [Node.js release table](https://nodejs.org/en/about/previous-releases) identifies Node 24 as LTS and Node 20 as end of life.

The maintainer's [AVIF optimization advisory](https://github.com/vercel/next.js/security/advisories/GHSA-2xp9-vwfh-vxw4) is patched in Next.js 16.3.3; the [rewrite hostname advisory](https://github.com/vercel/next.js/security/advisories/GHSA-p9j2-gv94-2wf4) is patched in 16.2.11. These describe conditional attack paths, not evidence that this installation was exploited. The selected stable release includes those fixes.

## Acceptance

1. Lockfile installation succeeds with Node 24 and introduces no unresolved production advisories in a fresh npm scan.
2. Frontend graph regressions, lint, type checking and production build pass using the refreshed dependencies.
3. Both validation jobs are prerequisites of image publication; deliberately failing regression commands return nonzero, and workflow syntax validates locally.
4. Existing ordinary/streaming API proxy behavior and graph browsing are checked through the local synthetic fixture when the new build is ready.

The backend dependency pinning, behavioral implementation and datastore integration are documented in the main accuracy specification.

## Local validation results

- Checksum-verified official Node 24.20.0 was installed in a temporary directory. `npm ci`, frontend lint, type checking, eleven graph/answer-rendering regressions, and the Next.js production build passed.
- A fresh full dependency advisory scan reports **zero** affected entries, down from 24 (17 high, four moderate, three low). The [post-update JSON](../audits/2026-09-04-npm-audit-after.json) records the scanner result. A second targeted compatible update moved `brace-expansion` to 1.1.18 after the general compatible refresh left it behind; no forced major overrides were used.
- `docker build --platform linux/amd64 -t paperless-frontend-audit:2026-09-04 frontend` passed with the actual Node 24 Alpine stages, including locked installation and production compilation. This created only a local validation image. Its build context was approximately 797 kB with dependencies, build outputs and environment files excluded.
- `actionlint .github/workflows/container-images.yml` passes. The workflow makes both validation jobs prerequisites of publication. Real failing backend regression commands returned nonzero during the entity red-before-green work; no hosted Actions run or branch-protection configuration is claimed here.

### Backend image verification

The local `paperless-backend-audit:2026-09-04` image builds successfully for `linux/amd64` from `python:3.12-slim`. Its actual interpreter is **Python 3.12.14** on x86_64. `pip install --require-hashes` installed all **87** locked packages with no version mismatch, and `pip check` reports no broken requirements. The tested lock SHA-256 is `18cd87b71fa2be2f85aa9c8cf3a4c758b8c370abfb029bcc6d29c6c08ad2415e`.

Representative installed versions are FastAPI 0.141.1, Starlette 1.6.0, Pydantic 2.13.5, Neo4j driver 6.3.0, asyncpg 0.31.0, pgvector 0.5.0, OpenAI 2.54.0, LiteLLM 1.96.0, Strands Agents 1.54.0, and NumPy 2.5.2. Imports and controlled lifespan startup/shutdown pass with Docker networking disabled, synthetic client settings, memory cache, disabled periodic work, and mocked datastore initialization. All 45 routes register and the background tasks drain on normal shutdown. This does not claim real datastore startup was exercised in that check.

The new root `.dockerignore` limits the backend build context to its Dockerfile, lock and application source, excluding environment files and bytecode. The measured context was approximately 781 kB. A separate Ruff `F` check identified unused imports/locals and a duplicate graph method; those findings were sent to their owning workstreams without reformatting unrelated code.

After refreshing both images from the frozen code, including complete synthesis windows, complete audit conversation history, and graph support presentation, the offline backend suite passes **128 tests**, with **25 explicit datastore-integration skips** (153 discovered). Docker networking was disabled; only `tests`, `scripts`, and `evals` were mounted read-only. All 87 locked package versions still match and `pip check` remains clean. The test command was:

```sh
docker run --rm --network none --platform linux/amd64 --entrypoint python \
  --mount type=bind,src="$PWD/tests",dst=/app/tests,readonly \
  --mount type=bind,src="$PWD/scripts",dst=/app/scripts,readonly \
  --mount type=bind,src="$PWD/evals",dst=/app/evals,readonly \
  paperless-backend-audit:2026-09-04 \
  -m unittest discover -s tests -t . -q
```

Final image identities, recorded after the frozen-source rebuild:

| Local tag | Image ID | Platform | Reported size |
| --- | --- | --- | --- |
| `paperless-backend-audit:2026-09-04` | `sha256:ee5d50a1fd2cf64d220e28058bc8ce861465e27abd55331f39dbb8cdfc459f82` | `linux/amd64` | 282,181,545 bytes |
| `paperless-frontend-audit:2026-09-04` | `sha256:f8641dd701ea40b40e8479031833f6e10a1a40343f957488273994e28b537c03` | `linux/amd64` | 71,370,864 bytes |

The standalone frontend image reports Node **24.20.0**, Next.js **16.3.4**, and React **19.2.8** in a network-disabled import check. The final production Docker rebuild also includes hub pagination, retry, and stale-request handling. Both tags remain local; neither image was published or connected to production.

The optional Ruff `F` check still exits nonzero with eight findings: unused imports in conversations/query/query-quality, two unused legacy pipeline locals, and an unnecessary extractor f-string prefix. The duplicate graph method and resolver unused import have been removed. No blanket formatter or automated unsafe fix was applied.

The browser fixture checks and final combined backend/UI validation are coordinated in the main accuracy workstream. Audit absence establishes the registry's current dependency result, not universal absence of vulnerabilities.
