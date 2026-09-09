# Document-local evidence interpretation and query reliability

Status: reviewed experiment required before production implementation. Parent:
[query reliability evaluation](query-reliability-evaluation.md), issue #33.
Starting revision: `4e4582c528e0f697816bbd1578b898e404f5d663`.

## Objective and evidence

Answer the user's requested aspects using original documents, preserving useful
supported facts and distinguishing unresolved parts. The design must apply across
subjects and query modes. Accuracy outranks latency; measure both. Prior grouped
and broad candidate-blind source-reading experiments failed the retained negative
in all three runs. Notes themselves sometimes made the unsupported inference.
That is evidence against enabling the broad reader, not proof that a smaller
reader will succeed. Production remains unchanged until qualification.

## Ranked falsifiable hypotheses

1. Unrelated document context interferes with source interpretation. Reading each
   document in isolation, with identical source windows and reader instructions,
   should remove the retained false inference while preserving supported facts.
2. The broad verifier overrides correct local readings. If local notes are correct
   but final verdicts remain wrong, move the next experiment to claim-local original
   evidence verification and explicit cross-document reconciliation.
3. The model misinterprets the source even alone. If isolated notes still make the
   inference, compare model capability on frozen local inputs before adding prompt
   rules or more orchestration. Do not select a new model from unmeasured claims.

## First experiment: isolate the reader's document scope

Add constructor-only `document_local` alongside unchanged `flat`, `grouped`, and
`source_first`. Use the same reader prompt/schema and original grouped windows as
`source_first`, but make one fresh call per document. Whitelist only the question,
reference date/date order and that document's windows. No candidate, correction,
conversation, other source or another reader's notes reaches the reader. Preserve
all original fields/ordinals, including disjoint passages without splicing them.
This test reads the complete supplied windows, not necessarily a complete document.
No title or generated text gains certifying authority.

Bound live calls by the existing shared semaphore and task count by the configured
concurrency. Return readings in original document order regardless of completion
order. Validate each reading against exactly its own document and handles. Any
invalid/unavailable reading makes the audit unavailable; cancel/drain sibling tasks
before return, and propagate caller cancellation. No partial note pack or silent
fallback. The verifier receives all original windows and untrusted reading notes,
with the existing verifier prompt, schema, parsing and finalizer unchanged.
No new source/output-token cap, cache, persistent state or production setting.

Expose a manifest-bound evaluator-only SDK retry policy. Both arms explicitly
use one SDK attempt and the existing OpenAI client `max_retries=0`. Record native
invocations; do not claim proxy/provider-internal attempts are observable. Test
constructor configuration and restoration on failure. This retry choice is shared
by both arms and differs from earlier runs, so rerun the matched broad baseline.

### Frozen measurement and admission

After independent plan and implementation review, freeze code, runtime, datasets
and manifests. Use the existing runtime/model/destination and request-scoped proxy
cache bypass; no global changes. Two arms: `source_first` and `document_local`.
Use the already authorized source-valid retained four-claim/183-window/36-document
case, three repetitions. Each arm: ceiling 240 native invocations (including native
protocol correction), 1,800 seconds, estimated 1.5M total tokens (not an output cap
or billing guarantee). Preserve current 90-second call/120-second audit deadlines
and concurrency four. If fan-out exceeds deadlines, record failure; do not secretly
raise them. Private originals/captures remain owner-only outside the repository.

Also run the unchanged four-domain, 64-assertion development set three repetitions
per arm: ceiling 480 native invocations, 1,800 seconds, estimated 1M total tokens.
Keep SDK/runtime identical between arms; installed production SDK is an isolated
experiment dependency, not a deployment. Report actual version and do not transfer
accuracy claims to the updated dependency lock. Admission must verify current
user authorization, frozen bounds, exact destination and payload before execution.

All scheduled assertions remain in denominators. Report raw false approvals,
normalized/finalized verdicts, positive losses, missing data, failures, timing,
usage and unknown telemetry. Repetitions do not prove statistical independence.
Inspect intermediate readings only after scoring; label interpretive findings as
such, never alter gold after a run. No convenient rerolls count as success.
The candidate must reject every retained negative, preserve critical positives,
and show an advantage over broad reading to justify more stages. Compact controls
cannot establish representative accuracy. If it fails, classify the responsible
stage using the ranked hypotheses and specify the next bounded experiment rather
than deploy or call the problem solved.

## Conditional complete design

Only after the central assumption passes, freeze a reviewed design for:

1. Question requirements: explicit requested aspects and evidence gaps, across
   history/current-state/comparison/detail queries, without assuming archive completeness.
2. Original-document reading: coherent original sections with provenance, exact
   references, record/action/date roles and unresolved interpretation. Retrieved
   snippets that omit necessary context trigger additional original-source retrieval.
3. Reconciliation: compare source-backed observations across records and identify
   conflicts, changes and latest documented state. Newness alone cannot prove a
   prior arrangement ended or establish current real-world status.
4. Claim-local verification: compose facts and verify each against its relevant
   originals. Generated observations guide navigation, never replace source proof.
   Missing references, competing records and uncertain applicability stay explicit.
5. Useful partial delivery: preserve independently re-audited supported units and
   expose unresolved requested aspects. Existing exact-candidate ledger contracts,
   omission counts and source ownership remain enforced in every mode, including Quick.

Fix the independently reproduced unknown-handle correction and unused-comparison
metadata defects in a separate reviewed change with native regression cases, so
semantic and protocol effects remain distinguishable. Do not bundle them into the
first scope experiment. No source cap or insurance-specific exception is allowed.

## Qualification and release

Meaningful mechanics regressions precede implementation. Include isolation and
conservation, out-of-order completion, bounded concurrency, malformed/foreign
readings, cancellation/draining and unchanged default behavior. Run matched native
controls before representative stress, adversarial false-note controls, independent
holdout and all-mode end-to-end tests. Verify real rendered UI, original source
links, partial answers, saved history and failure states. Native browser attachment
must actually work before claiming interactive acceptance. Review immutable image
pins and deploy only through homelab GitOps after all gates pass; preserve indexed
state and ingestion history. This spec is a path with decision gates, not a promise
that the first candidate succeeds.
