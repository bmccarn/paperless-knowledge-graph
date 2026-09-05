# Accuracy UI validation — September 4, 2026

Sixteen browser scenarios passed using the actual Next.js application, its runtime API proxy, and a synthetic backend fixture: twelve in the main run, one targeted graph-support check, and three targeted domain-hub checks. All runs recorded zero browser page runtime errors. They used Node 24.20.0, Playwright 1.62.1 and Chromium 151.0.7922.34. CUA could not start its native connection, so validation used the authorized Playwright fallback.

The fixture contains 303 indexed documents and 120 pageable graph-search entities. It simulates feedback state, task completion, accepted answers, interrupted streams, and hostile markup. These are frontend contract checks. They do not measure real model quality, production answer accuracy, or datastore transaction behavior. The separate Python suites exercise those backend acceptance and storage paths with controlled dependencies and disposable databases.

| Scenario | Observed behavior |
| --- | --- |
| Complete document pagination | Last page reaches document 9300; display says 301–303 of 303. |
| Global type filter | Medical filter returns 150 matches across six server pages. |
| Global document search | Finds `Synthetic archive 300`, beyond the former 200-document subset. |
| Failed document search | Backend failure detail appears with a retry action. |
| Graph search pagination | Page three exposes entity 119 of 120 matching entities and opens its inspector. |
| Graph expansion and source text | Repeated expansion remains four nodes/three relationships; outgoing/inferred labels are visible and HTML-looking source text remains literal. |
| Relationship source support | Expanding document 101 support decodes the backend's nested JSON evidence spans and displays quotes, rationale, and explicit inferred status; hostile quote markup remains literal. |
| Feedback lifecycle | Premature resolution is rejected; successful reindex leaves the report open; explicit review resolves it while original OCR remains available. |
| Accepted answer and source navigation | Supported claim status and complete quote render; the Document 101 citation opens document inspection. |
| Incomplete stream | Provisional unsupported text disappears after the stream ends without completion. |
| Explicit stream error | Provisional unsupported text disappears after a verifier error event. |
| Hostile answer markup | Image/script/event-handler text renders literally and does not execute. |
| Mobile document pagination | At 390 × 844, visible controls reach the second server page. |
| Hub pagination and domain selection | Shows all 303 indexed matches, reaches page 26 and document 9300, and retains the selected page when returning to a domain. |
| Hub error and retry | A controlled 503 shows backend detail; retry returns the full matching count and document page. |
| Hub stale-request handling | Independently delayed old success and error responses cannot replace the current domain, publish its error, or clear its loading state while its own request remains pending. |

During setup, the new ledger's `status`/`evidence_quote` fields exposed an outdated frontend field mapping. The browser run also caught `apiFetch` discarding useful backend error details, hiding the feedback workflow's reindex instruction. Both were corrected and the final run passed. [Before the error-detail fix](accuracy-ui/before-error-detail-fix.png) and [after the fix](accuracy-ui/documents-error-detail.png) preserve that evidence.

## Artifacts

- [Machine-readable outcomes](accuracy-ui/results.json)
- [Targeted relationship-support outcome](accuracy-ui/graph-support-results.json) and [expanded support screenshot](accuracy-ui/graph-relationship-support.png)
- [Targeted hub outcomes](accuracy-ui/hubs-results.json), [last page](accuracy-ui/hubs-last-page.png), [error detail](accuracy-ui/hubs-error-detail.png), and [current domain after stale responses](accuracy-ui/hubs-stale-request-guard.png)
- [Supported answer and its claim ledger](accuracy-ui/query-supported.png)
- [Graph search, third page and inspector](accuracy-ui/graph-search-last-page.png)
- [Resolved document review](accuracy-ui/document-feedback-resolved.png)
- [Last document page](accuracy-ui/documents-last-page.png)
- [Mobile pagination](accuracy-ui/documents-mobile.png)
- [Interrupted stream](accuracy-ui/query-interrupt.png), [explicit stream error](accuracy-ui/query-stream-error.png), and [literal hostile markup](accuracy-ui/query-safe-markup.png)

## Reproduce

Use the repository's Node 24 environment and installed frontend dependencies. Run the fixture and frontend in separate terminals:

```bash
python3 frontend/tests/accuracy_fixture.py --port 8485
```

```bash
BACKEND_URL=http://127.0.0.1:8485 NEXT_TELEMETRY_DISABLED=1 \
  npm --prefix frontend run dev -- --hostname 127.0.0.1 --port 3002
```

Install optional browser tooling outside the repository and run the browser contracts:

```bash
npm install --prefix /private/tmp/paperless-ui-validation playwright@1.62.1
node /private/tmp/paperless-ui-validation/node_modules/playwright/cli.js install chromium
PLAYWRIGHT_MODULE=/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs \
  node frontend/tests/accuracy-ui.mjs
PLAYWRIGHT_MODULE=/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs \
  node frontend/tests/graph-support-ui.mjs
PLAYWRIGHT_MODULE=/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs \
  node frontend/tests/hubs-ui.mjs
```

The runner checks an explicit fixture identity before any mutation, resets only synthetic state, and rejects non-localhost destinations. `UI_TEST_BASE_URL` and `UI_TEST_ARTIFACTS` can override the default local URL and artifact directory. Stop the fixture and development server after testing.
