# Held-out query release protocol

Status: both independent protocol reviews clear; implementation and concrete
custody/schedule admission pending. No held-out content has been opened for this
work and no G5 execution is admitted.

## Problem and boundary

The development runners deliberately reject held-out datasets. The existing
compact custody record also predates the question pipeline and lacks the complete
stress, end-to-end and resource contract required by
[query reliability evaluation](query-reliability-evaluation.md). Relabelling that
dataset as development or calling a lower-level runner would not establish G5.

The recorded custodian subsequently contributed candidate implementation. This is
a role-separation gap, not evidence that hidden content influenced implementation.
Preserve the original artifact and its custody history. Obtain fresh independent
custody and independent label review for the release decision; do not silently
replace the old artifact or continue describing its original separation as intact.

This work adds evaluation admission and execution only. It must not change the
application, prompts, routes, defaults or the frozen development qualification.
The original release thresholds remain unchanged.

## Custody and preregistration

1. Freeze the complete candidate before creating fresh hidden sources. The
   custodian and separate label reviewer must not have implemented that candidate.
   Candidate implementers may inspect this protocol, runner code and synthetic
   runner controls, but not hidden sources, questions, labels or expected aspects.
2. Keep source families, paired assertions and all presentation variants together.
   Record independent original-based labels, required positive meanings and reasons
   for negative labels before generating any candidate score. Resolve disputes
   before admission; retain the disagreement history.
3. The hidden assertion slice contains at least sixteen assertions, balanced
   positive/negative across eight families and four unrelated domains. Include
   compact and representative larger-context conditions. Derive context sizes,
   repeated-record structure and split identifying/table/date passages from the
   measured development/live capture distribution, not generic filler alone.
   Freeze a matrix covering state versus selected change, request versus completion,
   referenced versus actual instrument/record roles, signature versus event dates,
   quantity/sign/unit/header association, material conditions, same-date competing
   alternatives and source-order changes. Every retained semantic failure pattern
   needs a negative control and an answerable positive counterpart in the matrix.
4. The hidden question slice contains at least eight answerable questions: history
   and current/documented-state questions in each of four domains. Assign modes
   before scoring so all four modes are represented. Freeze required meanings,
   legitimate unresolved status and forbidden inferences. Labels are never prompt
   inputs and rubric originals never gain authority over the actual source text.
   These questions exercise actual production retrieval through HTTP/SSE delivery
   over an isolated synthetic corpus. Supplied-originals post-retrieval execution
   may diagnose a failure but cannot qualify this end-to-end slice.
5. Predeclare three repetitions per condition, with SDK retries disabled and
   request-scoped proxy cache bypass. Preserve the provider independence limit:
   unverifiably independent repetitions are dependent observations, never three
   independent samples. Do not add prompt nonces or clear shared caches.
6. Before execution, independently review the concrete schedule and resource
   estimate derived from the final case count/context sizes. Proposed outer
   ceilings are 900 native calls and 5,400 active seconds, with an estimated eight
   million tokens and no output token cap, including fixture preparation as well
   as query execution. A concrete manifest may narrow these
   ceilings; any expansion requires a new reviewed admission before consumption.

## Admission and execution interface

Implement a separate `prepare` / `run-case` interface for the held-out partition.
Keep both development guards intact. Importing or preparing the runner must open
no application readers or models. Use public synthetic fixtures for its tests.

The end-to-end fixture uses separate disposable PostgreSQL, Neo4j and cache state,
plus a Paperless-compatible source service containing only the hidden synthetic
originals. A separately owned fixture-build command validates a preregistered
preparation manifest before opening disposable stores or models. Populate derived
state with the frozen production ingestion path before
query admission, without labels, expected answers or hand-selected retrieval packs.
Capture preparation/model calls separately from query usage. Freeze every original,
metadata value, adapter/build/configuration, index generation/completion state and
derived-store snapshot. Keep relevant alternatives and unrelated distractors
searchable; deny production datastore access and all query-time ingestion writes.
Require a stable completed synthetic index before and after each case. A failed
preparation remains evidence and cannot be silently edited using expected answers.
Separate real-corpus browser qualification still remains required.

Preparation binds candidate application/prompt bytes, locked runtime and model
configuration, all runner/schema/grader code, this protocol, source/label hashes,
custody and independent label receipts, exact schedule, repetition/cache policy,
budgets and original release thresholds. Require complete exact-candidate Strict,
all-mode and live-browser development admission. Freeze measured stress derivation
and a custodian attestation of access history and no prior consumption.

Run one scheduled case per invocation in an isolated process. Revalidate all bound
bytes and actual runtime before model construction. Carry one schedule identity
through inputs, plans, audit, finalization and receipts. Use production assertion
audit/finalization for assertion cases, and actual QueryEngine retrieval, question
stages and delivery handlers for the question slice. Do not fabricate intermediate
approvals or substitute gold labels.

Capture every native input/output, retry/repair, terminal status, original-source
binding, final candidate and restored receipt in exclusive private files. A failed
attempt consumes its schedule position and remains evidence. Every next invocation
requires both exact-result independent grades and intact raw capture hashes for
all predecessors. Enforce aggregate calls/time before dispatch; count every stage,
including unsuccessful repairs. A missing capture, timeout, unavailable required
stage or failed factual grade stops qualification. A protocol correction that the
unchanged production path successfully completes within the same owned invocation
is retained and counted as a native attempt but is not a second scheduled case or
rerun. It remains eligible only when
all raw factual approvals, required-stage outputs and terminal/capture contracts
pass. An uncaught timeout, incomplete required stage or failed semantic verdict
cannot be reclassified as a recoverable protocol correction. Never skip or reroll
a failure.

## Grading and consumption

Grade raw semantic approvals separately from delivered claims. Require zero
critical false approvals, zero missing required positive meanings, correct source
attribution, and valid delivery/coverage/timeline bindings. Under-reported coverage
may be recorded separately only when original-based reviewers confirm all required
meaning remains delivered; it cannot excuse missing facts or false completeness.

The assertion slice has its own positive acceptance check: every labelled
positive assertion must be source-accepted, including a positive ancillary to the
proposed answer. No raw unsupported factual approval is permitted, even if a
deterministic guard or later repair removes it. Report false rejections separately
by semantic, reference, protocol and transport cause. Whole-question coverage
qualifications cannot excuse an assertion false rejection. For the question slice,
all required meanings must survive in the final answer and every raw approval and
delivered assertion must have correct original support.

Only the independent custodians may inspect case content before completion. Root
receives predeclared aggregate results and artifact hashes. Record any later
case-level exposure. Any adaptation after held-out evaluation consumes the set,
regardless of verdict or the trigger for adaptation. A failed set remains regression
evidence; another release decision needs a fresh independent set.

Passing this held-out component does not alone pass G5. Matched-baseline semantic
improvement, reviewed resource comparison and exact-candidate engineering validation
must also satisfy the parent release criteria. Final real-history/current-policy
browser acceptance and reviewed immutable GitOps activation remain operational
release gates. No production ingestion rebuild is part of this protocol.

## Implementation and verification plan

1. Independently review this protocol against the accepted release contract and
   resolve custody, stress/schedule and budget questions without reading hidden data.
2. Implement the standalone manifest/admission layer and production-stage owner.
   Keep shared utility reuse below explicit partition-specific admission.
3. Test with disposable synthetic sources: wrong partition, altered code/runtime,
   source/label replacement, missing custody review, repeated/changed case identity,
   dropped raw approval, false grade, timeout/cancellation and exhausted budget.
   Invalid admission must fail before any native call or output-attempt creation.
4. Review both code axes and run appropriate offline plus actual controlled
   transport checks. Do not treat fake-provider tests as model qualification.
5. After development gates pass, fresh custodians create/review/freeze the hidden
   cases and invoke the reviewed runner. Publish aggregate counts, dependence limits,
   timings and final disposition; retain all private evidence.
