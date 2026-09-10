# Source opportunity acquisition

Status: acquisition implemented and independently reviewed; resource measurement
and reader-retention diagnosis remain open. Not admitted for native qualification. This is slice B of [large-evidence query execution](large-evidence-query-execution.md).

## Chosen design

Three independent designs were considered: exhaustive transfer of discovered
opportunities, an iterative acquisition session, and catalog/facet-guided discovery.
Use an explicit opportunity inventory with prioritized execution. Its first
implementation has one collection operation rather than repeated answer/audit
cycles. Catalog hints and ranking may determine order; they cannot silently erase
discovered IDs or source sections. This avoids making a new model exclusion verdict
responsible for recall, while leaving iterative discovery as a later measured
optimization.

The inventory covers declared discovery operations and their observed leads. It
does not assert that every relevant document in Paperless was discovered. A broad
person/entity provenance edge can create many leads; that cost must be visible,
not hidden by a new document ceiling. A narrow explicit-document request retains
its original scope, but inferred genre/date/category assignments cannot authorize
exclusion.

## Interface and integration

Introduce a request-local acquisition module with one operation:

```python
bundle = await acquisition.collect(request, discovery, execution)
# immutable evidence_pack and acquisition receipt
```

`request` binds the original question, bounded original conversation context,
untrusted model-resolved conversational reference, mode,
evaluation date, source-date order and corpus generation. `discovery` contains all
declared query proposals, completed/pending discovery outcomes and direct index /
graph leads. The ordinary QueryEngine caller does not select document quotas,
source windows, semantic exclusions or source date meanings. `execution` uses the
existing request owner and bounded concurrency. Native evaluation supplies its
remaining absolute monotonic deadline and call budget; acquisition cannot reset
them. Ordinary QueryEngine currently has stage timeouts, not an overall deadline.
Its execution owner explicitly supplies `deadline=None`: finite enumeration,
existing adapter timeouts and external cancellation apply without introducing an
arbitrary overall question time limit. Invalid controls fail before I/O.

Initially reuse the existing query proposal and search adapters, but remove their
question-pipeline-only silent proposal truncation. Every emitted gap query must be
executed or recorded pending/failed. Keep legacy disabled-pipeline behavior outside
this change. Do not carry the legacy topic-specific injected query into the new
pipeline's discovery policy.

The module unions positive integer Paperless IDs from vector/keyword results,
historical discovery and the explicitly typed source-provenance fields of entity /
graph results. Do not scrape arbitrary numbers out of generated prose as document
IDs. Capture graph IDs before vector chunk lookup and graph presentation limits;
record per-organization graph sampling as sampled discovery. Reuse datastore
operations that preserve errors, not wrappers that turn failures into empty hits.
Every lead records its discovery path. Exact ID deduplication is safe;
inferred duplicate documents, supersession and irrelevant classifications cannot
remove a lead.

Add stable keyset pagination to lexical per-document discovery rather than raising
the existing 500-result limit. Query page size controls transport only. Retain
cursor progression, reported totals, query identity and completion/error state.
Repeated/nonprogressing cursors, inconsistent identities or a missing page cannot
produce finished enumeration. Vector top-k remains explicitly sampled discovery;
do not present its successful response as exhaustive enumeration.

Full collection replaces the inactive pipeline's eight-document historical
reservation and ranked chunk/window transfer path. Preserve discovery rank as a
dispatch priority only. The collector uses original Paperless content for evidence,
not generated summaries or index envelopes. Existing index contents remain useful
for locating documents.

## Original transfer and evidence identity

For every discovered eligible document, record one of `supplied`, `unavailable`,
`feedback_blocked`, `unindexed`, `stale`, `ineligible`, or `pending`. Record
deterministic eligibility exclusions and their reasons; do not delete their leads.
Every nonsupplied eligible state blocks acquisition completeness. Check indexed
completion, open feedback, original OCR hash and ingestion fingerprint (including
eligibility metadata) before accepting text. Read original
metadata as metadata; it does not establish issue dates, active status or source
authority. Existing generation/delivery checks remain mandatory.

Compile the whole fetched OCR through the existing original-source provenance and
citation-window functions. An original can be one evidence item with multiple
reference windows; no initial/top-ranked/last-ten window rule may discard its tail.
Keep exact original offsets, source hashes, table/header context and overlap. No
synthetic index header is prepended. If the citation-safe representation cannot
cover exactly the original extent `[0, len(original)]` using the union of actual
post-filter span intervals, record unavailable transfer and its missing intervals.
Offsets use Python Unicode code points. Initial transfer performs no normalization
or semantic selection; interior gaps, long tokens, repeated text and tables receive
the same mechanical coverage check as the final interval.

The immutable receipt binds the request, discovery operations, every discovered ID
and path, transfer state, original content digest, original extent, admitted span
inventory and corpus identity. It carries measured counts/bytes/windows and known
pending/failed work. Recompute its binding when preparing QuestionEvidence; do not
trust a caller-supplied `complete` boolean. Bind a separately retained acquisition
inventory digest outside the mutable receipt, following the existing reader
inventory anchor pattern. Deleting a pending document or tail interval and then
recomputing the receipt/public coverage must still fail validation. The reader must receive exactly the
bound span inventory. Changing an original, removing its final interval or altering
a document state invalidates the receipt.

Acquisition completeness means that every declared operation and discovered eligible
original was transferred. It is distinct from reader execution, fact conservation,
requested-meaning coverage and archive completeness. Known acquisition gaps prevent
complete question coverage even when all available facts are supported. Available
facts may retain their verified partial answer under the existing finalization
contract; a missing original is never converted into a negative factual finding.
Restoration revalidates the acquisition binding before showing complete coverage.

## Ownership, errors and cost

Validate request, adapters and execution controls before I/O. The collector owns
its worker tasks, not datastore/model clients. Reuse the existing clients injected
at the composition root; never initialize schemas, start background jobs, ingest,
repair or change production caches. PostgreSQL/Neo4j adapters have local disposable
test implementations; Paperless has production GET and exact in-memory adapters.
Model proposal adapters remain externally mocked for orchestration tests.

Use a shared iterator with bounded workers. Preserve completed and pending transfer
states when a task fails. External cancellation stops dispatch and joins workers
before returning control. The enclosing capture owner retains diagnostic state;
do not catch cancellation and emit a success-shaped answer. Generation/content
inconsistency invalidates the snapshot. Transport failure and resource exhaustion
are explicit incomplete acquisition, not silently successful pagination or a
truncated evidence pack. No automatic native reruns or new output-token cap.

If all 261 observed historical candidates are distinct, eligible, fresh and
admitted, they imply at least 261 calls under the existing document-local reader,
before extra graph leads or audits; blocked documents do not incur reader calls. Audit input also
scales with total originals multiplied by audit batches. Therefore the former
300-call live admission cannot be reused. Measure candidate counts, original bytes,
reference windows and projected stage shapes read-only before proposing a new
bounded diagnostic. Measure maximum serialized reader, composition and audit
payload sizes and retained-copy memory, including high-degree person graphs. Worker
limits do not bound model context. Report known capacity incompatibility and unknown
provider capacity explicitly; do not silently partition source authority or truncate
payloads. Retain actual calls, tokens, elapsed time, concurrency, peak
memory and pending work. Do not claim an optimization before measuring it.

## Required implementation and validation sequence

1. Write red interface regressions for more than 500 paged matches, more than eight
   subject/period groups, graph-only provenance, five proposed searches, late source
   sections, repeated high-rank revisions and unknown metadata. Use fabricated
   unrelated domains and contextual/focused/history/comparison requests. Assert
   transferred identities and original intervals, not a particular ranking.
2. Implement paginated discovery and the collector with exact original binding.
   Add failure controls for missing/nonprogressing pages, stale/blocked/unindexed
   originals, duplicate IDs, partial progress, cancellation and receipt tampering.
   Exercise the same interface against disposable real stores before claiming
   production adapter integration.
3. Integrate the inactive QueryEngine, QuestionEvidence snapshot, final coverage
   and saved/cache restoration. Require all declared search proposals and graph
   provenance to enter the receipt. Preserve existing negative-audit and source-
   authority controls. Change pipeline identity and bind this spec in evaluation.
4. Independently review implementation and measure the acquisition distribution
   without native model calls. Publish only aggregate diagnostics; originals and
   source-level work records remain private.
5. Separately preregister a bounded original-reader diagnostic before execution.
   Compare the retained truncated/source-window condition with complete-original
   input using the unchanged reader, across the retained omitted qualification and
   cross-domain positive/negative controls. Freeze original-based required meanings,
   repetitions, attempts, model/runtime and budgets; retain every outcome. This is
   a diagnosis of meaning retention, not a replacement for whole-query qualification.
6. If the unchanged reader still omits supplied material meaning, keep that failure
   open and review a separate interpretation-recovery design before implementation.
   A possible append-only source reread may propose additional observations, but
   cannot remove existing ones or certify facts; every addition would still require
   the ordinary independent audit. This is a candidate, not an accepted fix.
7. Only after the demonstrated acquisition and reader losses are addressed, freeze
   the whole candidate for fresh Strict/all-mode/live evaluation and the remaining
   held-out, matched-baseline, browser and GitOps release gates. Do not splice prior
   passes into that run or claim that read coverage proves semantic completeness.

## Implementation observations (2026-09-10)

The request-local collector and strict store discovery now preserve all observed
keyword samples, paginated lexical matches and graph provenance before presentation
limits. Complete-original interval and reader-context bindings precede reading.
Acquisition identity participates in mandatory coverage and conservation bindings;
removing outer acquisition metadata cannot restore an incomplete answer as complete.
Evaluation owns the inherited absolute deadline and captures joined-worker progress.

Offline backend run: 940 tests passed with 58 opt-in skips. The real PostgreSQL
regression transferred 519 of 521 synthetic late-match documents while retaining
one feedback-blocked and one unindexed document as incomplete acquisition. Two
built-browser cases passed desktop/mobile/source/reload checks. Visual inspection
then corrected missing evidence-count metadata; targeted tests and both browser
cases passed again. The rebuilt UI also explains incomplete source searches/reads; both browser
cases passed with that explanation after reload. These checks establish transfer and delivery behavior only.

Model-free measurements report first document-local reader input bytes and the
whole-source inventory size. Full composition/audit payloads also depend on model
reading output and candidate units and remain explicitly unmeasured at this point.
Provider context capacity is unknown. Do not treat source-byte floors as total
model context or repeat a large native query before separate admission.

Production resource measurement is pending: automatic approval review rejected
both an initial helper retaining private originals and a revised aggregate/hash-only
helper because the cluster code upload and production document reads require more
specific authorization. Neither helper ran. Local retained-original B2 preparation
and offline validation are unaffected.

## Follow-up: exact boundary-context cost

The approved production sizing attempt transferred 919 discovered records (903
supplied, nine connection timeouts, seven citation interval gaps), then failed to
finish packaging/sizing. No measurement or native accuracy result was produced.
Local profiling reproduced repeated whole-prefix/suffix normalization for every
citation window. This is a performance defect, separate from unresolved interval
gaps and the packaging deadline boundary.

Replace repeated full-range normalization used only for boundary guards with
adaptively expanded edge reads. Required invariant: byte-for-byte identical date
and scalar boundary strings, including arbitrary whitespace, Unicode, removed
presentation markers, and markers crossing the requested edge. Expand until the
retained boundary cannot depend on omitted interior text; never cap source content
or approximate a guard. Exact original spans, source identities, reference
validation, and reader prompts remain unchanged. Differential tests must compare
with the original whole-range transforms, and deterministic work counters must
show ordinary long-text edges avoid scanning the full document. Reprofile the
same synthetic input after validation. This does not qualify the failed sizing
attempt or establish production throughput.

Packaging must recheck the inherited deadline before returning admitted originals.
If it expires during synchronous packaging, return an unadmitted, incomplete bundle
while retaining transfer progress. This closes late admission; it does not claim
asyncio can preempt CPU work or provide a hard wall-time kill. A process-owned
execution watchdog remains necessary for that stronger resource guarantee.
