# Issue 16: survive a transient extraction-provider outage

Status: complete. Independently reviewed PR29 merged as 5b3bb59 and deployed through GitOps PR147; the fix remains in the final cbdc610 release. Targeted recovery and final corpus acceptance passed.

## Reproduced failure

On 2026-09-07 at 23:32 UTC, the existing LiteLLM proxy reached its configured 10,000-request recycle threshold and restarted. Its logs explicitly report the request limit, orderly shutdown, and subsequent readiness roughly 55 seconds later. The KG API stayed ready without restarting. Four migration documents failed during that interval with incomplete extraction coverage; the pipeline retained their previous indexes. Requests and completed extraction windows resumed after the proxy recovered.

`app/extractor.py::_extract_json_with_retry` makes its three allowed attempts immediately after connection failures. The captured first failure exhausted all three attempts in milliseconds, before the proxy could recover. This is a transport-recovery defect, separate from the removed output-token limits.

## Intended change

- Keep exactly three total attempts shared by transport, parsing, and required-envelope validation. SDK retries remain disabled.
- Before an available second or third attempt, wait 20 or 40 seconds respectively for typed OpenAI connection/timeout failures and transient HTTP statuses 429, 500, 502, 503, or 504. The third attempt can therefore reach a proxy that recovers after the observed brief outage. Do not add a nested retry loop or wait after the final failed attempt.
- Keep malformed JSON/envelope retries immediate, and keep actual completion truncation on the existing immediate adaptive-split path. Permanent HTTP errors do not gain a recovery delay. Exception-message keywords must not authorize the transport delay.
- Preserve cancellation during both an invocation and a recovery wait. Retain finite native connection/read/write/pool deadlines, source coverage, exact provenance, all validation gates, and the existing models and policies. This timing-only repair does not change source-audit-v10 or review-admission-v4, or require reprocessing documents already accepted on that fingerprint.
- Log only operation, attempt, exception class, and bounded wait duration; never include provider/request bodies.

## Validation

Exercise the actual AsyncOpenAI request boundary with a synthetic transport and a virtual clock: an endpoint unavailable for 55 seconds must succeed on the third attempt, with three requests total and waits of 20 and 40 seconds. Verify exhaustion stays at three attempts, no final wait occurs, and cancellation prevents further requests. Cover transient HTTP responses, permanent errors, malformed JSON/envelopes, and truncation. Preserve existing payload/no-output-cap and complete-coverage regressions.

Run focused red-to-green checks, then the full offline and actual disposable-datastore suites. Obtain independent Standards and Spec reviews of the final source and exact-head CI before release.

## Live sequence

Let the single admitted ordinary sync finish and record all failed document IDs from its final result. Verify its post-sync Steward child and actual database writers are drained before any deployment or repair. Publish the reviewed immutable images and deploy through GitOps with the staged 0/0/2 controls and 90/120-second audit settings intact.

After runtime/history checks, recover only the individually identified failed or demonstrably stale documents through the ordinary per-document processing endpoint, recording each task ID once. Verify exact source/fingerprint/coverage and preservation afterward. Continue the full corpus/vector/history checks, the retained large-source query, and the fresh original insurance-question UI/source acceptance. Restore normal schedules through a newly verified GitOps diff only after all gates pass; the prepared restoration PR must retain the newly tested image pins.

## Evidence

- Private original task and proxy shutdown/recovery receipts are retained in the operator workspace; public artifacts contain only aggregate findings.
- The synthetic 55-second outage and typed transient-status regressions failed before implementation. The actual SDK-boundary recovery now succeeds at virtual request times 0, 20, and 60 seconds; exhaustion and cancellation stay within the existing three-attempt budget.
- All 53 focused extraction/recovery/output-limit tests pass. The full offline suite passes 365 tests with 46 expected datastore skips; all 365 tests pass against disposable Neo4j, pgvector PostgreSQL, and Redis with zero skips. These are controlled validation results, not a production-recovery claim.
- Independent reviews and exact-head CI passed before release. The single original sync ended with 879 successes and 9 failures; it was not resubmitted. Targeted processing recovered those 9 failures plus 12 newly arrived documents, with all 21 individually accepted. Two final stable snapshots accept all 903 current documents. Final source queries, bounded vector reconciliation and GitOps schedule restoration also passed; see the [completed closure record](issue16-acceptance-closure.md).
