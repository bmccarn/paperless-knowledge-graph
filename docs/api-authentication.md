# API authentication

Send one `X-KG-API-Key` header. Admin keys also satisfy read scope. Only GET /health and /readyz are public. Enforce returns 401 for missing, empty, duplicate or invalid keys, and 403 for read keys on admin operations. Authentication runs before handlers and SSE streams start.

## Configuration

| Variable | Process | Default | Purpose |
|---|---|---|---|
| `KG_AUTH_MODE` | Backend | off | off, warn or enforce |
| `KG_READ_API_KEYS` | Backend | empty | Comma-separated read keys |
| `KG_ADMIN_API_KEYS` | Backend | empty | Comma-separated admin keys |
| `KG_API_KEY` | Frontend server and CLI | empty | One key sent to the backend |
| `BACKEND_URL` | Frontend server | http://app:8000 | Backend destination |

Configured keys are trimmed and empty entries ignored. Comparison uses constant-time byte comparison across every configured key. Use separate randomly generated keys for consumers and store them in a secret manager. Never put keys in URLs, command arguments, public frontend variables or build arguments. Settings hide key lists from their representation.

Mode off skips authorization and is the default. Deploying the backend image alone does not enable authentication. Mode warn allows requests but logs would-be rejections with route template, scope and status, without keys, query strings or path parameter values. Mode enforce rejects unauthorized requests. Empty lists in enforce deny all protected requests. Restart processes after environment changes. Rotate keys by overlapping old and new backend keys while callers switch.

## Frontend boundary

The browser already used Next.js route handlers. Both the catch-all and query SSE handler now inject server-only `KG_API_KEY`. Browser calls always use same-origin /api; `NEXT_PUBLIC_API_URL` no longer bypasses the proxy. The catch-all streams /logs/stream, so EventSource needs no credential. Caller keys and cookies are not forwarded. Responses never include the injected key, and redirects are not followed.

The UI is a privileged client, not a user authentication system. An admin key gives everyone who can reach the frontend admin capabilities, including repairs, deletion and conversation persistence. Keep the entire frontend behind existing identity-aware access control and restrict direct service access. Do not expose it as a public read API. A read key permits browsing and stateless queries, but the current chat UI cannot create or save conversations; admin controls return 403.

When `KG_API_KEY` is configured, frontend mutations require an Origin matching the request Host. Without a frontend key, the proxy preserves legacy off-mode behavior. Cross-site and same-site-but-not-same-origin browser requests are rejected before key injection. The ingress must preserve Host and enforce HTTPS. These checks mitigate browser CSRF; they do not authenticate non-browser clients. CLI clients should use the backend, not spoof Origin to borrow the UI credential.

## Consumers

| Consumer | Required change or scope |
|---|---|
| Next.js ordinary requests, query SSE and log SSE | Server-only `KG_API_KEY`; admin for the full UI |
| scripts/api_smoke_tests.py | `KG_API_KEY`; read normally, admin with --mutating |
| scripts/eval_harness.py | `KG_API_KEY`; read for stateless queries |
| scripts/kg_exact_drift_audit.py | `KG_API_KEY`; read normally, admin with --repair |
| homelab-health-alerts | External change required: read key header on /ops/guardrails |
| Merlin's kg.sh | External change required: read key header |
| MCP adapter and other machine clients | Dedicated read key and direct backend URL |
| Kubernetes and Compose probes | No key for /readyz or /health |
| In-process schedulers and workers | Call Python functions rather than HTTP; no new credential |
| Synthetic browser/evaluation fixtures | Local fixture servers; no production key |

Current homelab ingress sends all paths, including /api, to the frontend. Machine clients using that URL receive the frontend's privilege, not their own read scope. In-cluster consumers must use http://paperless-kg-api.tools.svc.cluster.local:8000. Off-cluster automation needs an approved direct backend route or private connection preserving its key. Never point a read-only adapter at the admin-key UI proxy.

## Rollout through separate infrastructure PRs

1. Merge this application PR, build images, and deploy full commit-SHA tags with `KG_AUTH_MODE`=off. This PR changes no cluster resources.
2. Create separate read-consumer and frontend-admin keys in 1Password. Extend existing secret sync and paperless-kg-env to supply backend `KG_READ_API_KEYS` and `KG_ADMIN_API_KEYS`. Add frontend `KG_API_KEY` through `valueFrom.secretKeyRef`, preferably from a separate frontend-only Secret. Do not give the frontend the full backend Secret.
3. Keep frontend `BACKEND_URL` pointed at the API Service. Give health-alerts a dedicated read-key secret reference and add `X-KG-API-Key` to /ops/guardrails requests without logging it. Update kg.sh and other callers to load keys privately and use direct backend destinations. Keep probes anonymous. Verify frontend access control and HTTPS.
4. Set backend `KG_AUTH_MODE`=warn. Check auth warnings for unconverted consumers. Exercise reads, conversation writes, query SSE and log SSE through intended callers. No warnings alone does not prove coverage.
5. Set `KG_AUTH_MODE`=enforce once secrets and consumers are ready. Verify missing/invalid keys return 401 and read keys return 403 on admin routes. Use safe fixtures for mutation tests, not production repair jobs. Roll back to warn if a caller was missed.

No migration is needed. Ship backend and frontend together before enforce. Streams are authorized when opened; rotating a key does not revoke an already-open stream. Running-job restart behavior is unchanged.

## Exact route scopes

These are backend paths without the frontend /api prefix. Each route declares a scope dependency. The route-walking test fails on unclassified routes, including documentation routes.

Queries have one scope refinement: nonempty `conversation_id` also requires admin because both query handlers append conversation messages. Stateless queries remain read-scoped. This closes a conversation-write bypass for read keys. Direct documentation requests require a header-capable client; the protected frontend proxy loads them without exposing its credential.

### public

- GET `/readyz`
- GET `/health`

### read

- GET `/config`
- GET `/status`
- GET `/freshness`
- GET `/ops/guardrails`
- GET `/document/{doc_id}/detail`
- GET `/document/{doc_id}/feedback`
- GET `/task/{task_id}`
- GET `/conversations`
- GET `/conversations/{conv_id}`
- GET `/models`
- POST `/query`
- POST `/query/stream`
- GET `/graph/search`
- GET `/documents`
- GET `/graph/node/{node_uuid}`
- GET `/graph/neighbors/{node_uuid}`
- GET `/graph/initial`
- GET `/entity-review/candidates`
- GET `/openapi.json`
- GET `/docs`
- GET `/docs/oauth2-redirect`
- GET `/redoc`

### admin

- POST `/sync`
- POST `/reindex`
- POST `/reindex/{doc_id}`
- POST `/freshness/repair`
- POST `/document/{doc_id}/feedback`
- POST `/document/{doc_id}/feedback/{feedback_id}/resolve`
- DELETE `/document/{doc_id}`
- POST `/conversations`
- PATCH `/conversations/{conv_id}`
- DELETE `/conversations/{conv_id}`
- POST `/generate-title`
- POST `/task/{task_id}/cancel`
- POST `/resolve-entities`
- POST `/entity-review/steward`
- POST `/entity-review/steward/task`
- POST `/entity-review/ignore`
- POST `/entity-review/split`
- POST `/entity-review/merge`
- GET `/logs`
- GET `/logs/stream`
- POST `/create-indexes`
