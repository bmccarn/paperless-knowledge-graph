# Live retrieval qualification of the inactive question pipeline

Status: preparation proposal. All-mode development admission remains pending;
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
