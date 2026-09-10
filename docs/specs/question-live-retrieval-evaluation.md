# Live retrieval qualification of the inactive question pipeline

Status: inactive implementation under review. All-mode development admission remains pending;
this document does not authorize production activation or count as a passing run.

## Question and evidence selection

Before native execution, freeze a small cross-domain set of six concrete questions
and mode/history inputs using the actual eligible corpus inventory. Include the
original insurance history/current-record question, a focused factual lookup, a
comparison, a historical question outside insurance and a contextual follow-up.
Cover all four modes across this set; the separate fixed-originals all-mode run
already measures every development case in every mode.

Independently inspect original documents before grading answers. Record known
required source-supported aspects and date/value/action roles, expected uncertainty
and relevant document identities privately. Use metadata and original text to find
known older and newer relevant records; generated summaries cannot define truth.
Do not infer archive completeness or real-world active status from the inventory.
This corpus is development evidence, not the sealed independent holdout. Freeze
questions, originals/metadata hashes and the rubric before calling the candidate.

## Proposed concrete requests

These requests are preparation inputs, not frozen admissions. Two reviewers must
approve their private original-based rubrics before execution. Required meaning
must follow the question; incidental blanks, wording and verbosity are not mandatory.

| Order | Mode | Scenario |
| --- | --- | --- |
| 1 | strict | Original insurance history and most-current-record question |
| 2 | quick | Policy declaration term, coverage limits and deductible lookup |
| 3 | deep | Year-over-year utility usage and bill comparison with arithmetic |
| 4 | timeline | Multi-year service-plan and recurring-charge history, including partial periods |
| 5 | deep | Prior-year service invoice comparison using a frozen conversational antecedent |
| 6 | strict | Current bill charges versus prior payment and a scheduled future draft |

Freeze exact request strings and the follow-up history in the private manifest.
The follow-up's context describes source-listed charges from one invoice; its answer
must independently retrieve the earlier invoice and treat conversation text only
as antecedent context, never source proof. Private originals also supply the later
invoice, so any comparison must be independently supported on both sides.

Private preparation captured 128 original documents matching the inventory's OCR
hash and modification timestamp: insurance-tagged records, Starlink records and
the two Duke bills. These are rubric inputs only, never a retrieval allowlist or
preloaded candidate evidence. Keep unrelated corpus documents searchable. Evaluate
retrieval against known necessary records or equivalent original evidence; an
alternative valid source is not a failure merely for having a different document ID.

The initial inventory had 929 eligible documents, with 925 aligned indexed IDs and
four newer unindexed documents. Recheck exact freshness at execution. Do not repair
or reprocess documents for this experiment. If the selected question depends on an
unindexed or changed original, stop preparation and explicitly revise the admission
scope before any candidate result is seen. Surface actual incompleteness in output;
never silently mark this snapshot fully synchronized.

## Isolation and boundaries

Use the exact reviewed application and locked dependency runtime in a separate
process. Construct the candidate QueryEngine with the inactive flag enabled only
there; do not change serving configuration or deploy it. Exercise actual retrieval,
planning, gap filling, source reading, composition, audit, delivery snapshot checks
and final public result assembly. Keep real date, corpus generation and source
freshness checks. An index change invalidates that run rather than weakening checks.

Do not call application startup or datastore init methods: embeddings init executes
schema statements and graph init creates constraints/indexes. Attach a PostgreSQL
pool with session-level default_transaction_read_only enabled, and a graph driver
for the existing reviewed retrieval operations without schema initialization.
Use request-local evaluation caches instead of production cache reads/writes;
continue reading the real corpus generation. Do not start ingestion, reindex,
repair, review mutations, conversation persistence or background jobs. Close all
owned clients/pools on success, failure and cancellation.

Capture native application-stage inputs/outputs and transport usage when available,
including query/retrieval model calls outside the new Strands stages. Disable
explicit SDK/client retries for the evaluation; report any unobserved upstream
attempts as unknown. Do not log credentials, authorization headers or environment
contents. Retain personal documents, prompts and results only in owner-only private
artifacts; publish aggregate outcomes and code pointers.

## Execution and acceptance

Require passing all-mode development and independent review of this concrete
execution plan/harness before native calls. Admission binds the exact all-mode
manifest, all 48 case results and complete raw input/output hash inventories, plus
both independent grade receipts for every result. Revalidate these immutable bytes,
all scheduled identities and passing outcomes before each live case. Require exact
candidate application, locked dependencies and runtime equality to that all-mode
run. The only additional files are the separately reviewed live harness, its tests
and this spec; no application or existing evaluation-harness difference is admitted.
Test rejection for missing grades, changed raw artifacts, different code/runtime
and failed or incomplete prerequisites before opening production readers.
Use a separate immutable manifest with
code, runtime, mode/history, corpus and question/rubric hashes. Initial bounded run:
six questions once, at most 300 application-level model calls and 3,600 active
seconds. These are experiment controls, not output-token limits. Retain every
attempt; no automatic rerun or silent exclusion of failed cases.

Grade one result before the next. Require complete planning and a successfully
restored coverage receipt with status complete or partial; valid unavailable
coverage and coarse planning do not qualify. Both independent reviewers must check
raw audit approvals, delivered facts, required-aspect coverage and every additional
assertion against originals. Require zero false-complete assessments. Bind both
passing grades to the exact result SHA and its complete input/output attempt-hash
inventory; continuation revalidates those bytes and the scheduled request identity.
Compare retrieval against the known relevant documents: report
whether a lost answer originated in retrieval, reading, composition, audit or
coverage. Missing critical supported facts, false approvals, unavailable execution,
invalid bindings or incorrect Timeline projections fail the candidate. Conservative
coverage labels are reported separately from factual errors.

Use the built frontend to submit the exact candidate requests and inspect actual
rendered answers, source panels and Timeline controls, including narrow layouts.
Exercise the real HTTP/SSE route handlers with serving startup disabled in the
isolated process. Bind listeners to loopback and enforce an explicit route/method
allowlist. Deny ingestion, sync, repair, reindex, review and every unscheduled model
route even though they remain registered on the imported application. Supply a
deterministic no-model title adapter for the UI's automatic title request; it must
not call the serving title model. Conversation routes may access only the private
adapter, and source endpoints may serve only already captured read-only originals.
Test allowed browser requests and rejected mutation/unscheduled model requests
before attaching production readers. Only the frozen request may start a model run; duplicate submits
must not silently consume extra attempts. Use a private in-memory conversation
adapter for the frozen follow-up and delivery inspection, with no production
conversation writes. Verify final SSE payload conservation against the captured
engine result; HTTP serialization can be checked against that same retained result
without representing replay as a second independent model run. Own and close all
listeners, browser processes and route background tasks, including error paths.
Distinguish public-result/browser delivery from replayed history and separately
validated real conversation persistence. Retained native-output replay does not
qualify live retrieval. The independent holdout and exact-head GitOps release gate
remain required after development; no passing subset closes the production issue.


## Executable assembly plan

The boundary modules are composed by `scripts/live_query_command.py` on the remote
runtime and `scripts/live_query_local.py` on the local browser host. Native admission
and the release gate remain pending. The implementation follows these slices:

1. Add separate prepare/run-case commands. Bind independently reviewed private
   requests, rubric and originals, all-mode prerequisites, conservative admission
   when present, and the built frontend/source/lock identities. Revalidate prior
   live grades and raw artifacts before any next case; preserve existing/failed
   directories and enforce the aggregate call/time budget.
2. Use a fresh isolated process for each invocation. Static admission and route
   checks precede reader attachment. Read the real corpus generation and inventory
   through an independent client; compare date, configuration and source/index
   snapshot to the frozen manifest before admitting browser submission. Imported
   embedding clients are closed by reader teardown and must not be reused across
   cases.
3. Own the direct engine client, native stage capture, read-only readers, delivery
   session, loopback server, standalone frontend and Chromium in nested lifetimes.
   Stop the browser/server and join route/query workers before detaching dependencies.
   All model paths share the remaining experiment deadline and capture budget.
   Preserve failure artifacts even if cleanup also fails.
4. Submit each frozen request exactly once through the built frontend. Select its
   exact mode/model and private history. Inspect actual desktop/mobile answer,
   source drawers and Timeline controls; retained history inspection cannot start
   a second query. Do not substitute rubric originals for live retrieval.
5. Require conserved SSE/engine final payloads, complete captures, unchanged
   post-delivery corpus and fetched originals, no denied store operations, restored
   coverage and complete planning. Retain model/stage captures, fetched originals,
   SSE bytes, private history and screenshots with exact result hash inventories.
   Both grades record any conservative duplicate rejection and surviving target.
6. Before production readers, run assembled offline controls for changed admission,
   duplicate submission, incomplete/truncated model output, corpus drift, stream
   cancellation, browser startup failure, tampered delivery, and aggregate budget
   exhaustion. Include a built-frontend success exercising source inspection,
   contextual history and mobile restoration. Independent review precedes native
   execution; this assembly plan grants no release approval.

### Remote runtime and local browser ownership

The locked Python runtime and read-only stores are reached from an isolated process
in the existing API pod; Node and the built standalone frontend run on the Mac.
The remote owner exposes only a dynamically selected loopback listener. Its readiness
message binds the frozen manifest hash, pod UID and a per-case nonce. The local
owner forwards that exact pod/port on 127.0.0.1 and sets the frontend's existing
BACKEND_URL to that local forwarding endpoint. Bind the actual frontend build,
source, lockfile, Node and browser identities in the live manifest. No frontend
application change or replacement query route is required.

Use the shared native-stage capture with reader_inventory=True so retired filtering
stages fail before dispatch. Browser submission remains subject to the exact
SingleRequest boundary. Capture remote engine/SSE output and local received SSE,
then compare them; neither forwarding nor history replay constitutes another run.

A private control channel carries readiness and an explicit nonce-bound stop.
Control-channel EOF, malformed control, deadline expiry or local browser/forwarding
failure must cancel the remote owner and drain its nested server, route and query
workers before readers/model capture close. Only then may it acknowledge closure.
The local owner keeps forwarding alive while awaiting that acknowledgment, then
joins its frontend/browser/forwarding children. This wait is bounded: if the
control channel dies and acknowledgment cannot arrive, record remote cleanup as
unconfirmed and the run as failed, then join local children. Remote EOF independently
initiates its own bounded shutdown; never infer remote success from local exit.
Browser disconnection alone is insufficient:
the current frontend stream proxy does not forward its request cancellation signal.
Test control EOF, explicit stop, mismatched identity, startup failure and teardown
ordering using synthetic resources before opening production readers.


### Concrete command inputs

The remote `prepare --preparation PATH --manifest PATH` command writes a new private
manifest only after all-mode and original-input reviews pass. Preparation JSON names
`dataset`, `initial_output`, `all_mode_output`, `inputs`, `configuration`,
`corpus_snapshot` and `evaluated_at`; an optional conservative admission is retained
only when explicitly applicable to the frozen candidate. Configuration includes
explicit non-secret actual runtime settings, the local build/browser snapshot, and
the effective public source URL. Runtime rechecks the actual corpus and date before
browser readiness; supplied preparation values alone never establish freshness.

Each private input review names its axis and independent reviewer, records `pass`,
and binds the complete private-input hash map. Neither original-review receipt is
passed to the candidate as evidence. Local options identify the exact manifest,
private inputs, output directory, case index, repository frontend and Node, cluster
context/namespace/pod/UID, and staged remote interpreter/code/preparation/manifest/
output paths. Transfer is exclusive or byte-identical; changed artifacts stop the
run. The local command collects final artifacts and writes result.json only after
runtime, browser, corpus and ownership checks succeed. It never writes a grade.
