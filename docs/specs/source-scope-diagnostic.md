# Matched source-scope diagnostic

Status: implemented with independently cleared Standards and Spec reviews; no native executions. Exact remote-runtime preflight and package admission remain outstanding. Parent: [source-grounded interpretation](source-grounded-interpretation-contract.md), G2 in [query reliability evaluation](query-reliability-evaluation.md). The diagnostic-only application prototype is `d0fa079` on draft PR #50. Both previous model-route grades remain FAIL.

## Hypothesis and decision

A verified declaration of the supplied original's extent, combined with explicit passage versus whole-original references, can recover justified original-local absence observations and their sufficient citations without inventing world absence or losing positive facts. The diagnostic tests this mechanism across the retained operations, billing, measurement and application records. It does not qualify a production query route.

For a route's mechanism improvement, the scoped profile must correctly approve every preregistered true original-local absence with sufficient selected citations in all repetitions, improve at least one matched control failure, preserve supported positives and required meaning, and introduce no false approval. World-absence and partial-original traps must be rejected. Fewer protocol errors alone do not satisfy the hypothesis. Remaining unrelated factual, precision, numerical or completeness failures still prevent overall candidate qualification.

Grade raw semantic decisions, selected citation sufficiency, deterministic rejections, reader meaning retention and final delivery separately. A complete-original supply receipt alone earns no citation credit: use the interval union of actually selected/resolved source windows in either profile. All scheduled work needed for a mechanism pass must complete; failures and unobserved opportunities receive no pass credit. Grade initial, corrected and subset attempts separately: later success cannot erase a false approval or turn an uninterpretable earlier semantic outcome into a clean pass. Preserve original disagreements and uncertainty labels. Do not repair gold from candidate output or retrospectively narrow denominators.

## Frozen comparison

Use the six unchanged originals/questions from the completed two-route diagnostic, with its fixed observations and reviewed labels. Add two exact partial-source variants from those originals. Both profiles use the same newly frozen code, including the date-spacing repair. Old model outputs are provenance, never the newly measured control.

The schedule has 48 matched pairs, 96 executions: eight cases, three repetitions, two existing routes, control and scoped profiles. Counterbalance profile order within pairs and preserve the prior case rotation and fixed-versus-fresh order. Each execution contains a fixed-candidate finalizer, fresh document-local reader and fresh-candidate finalizer. Both audits receive empty prior-reading notes, keeping source interpretation from becoming an additional uncontrolled variable. Scoped fresh readings retain validated selection receipts separately.

The partial variants supply exactly `[0,119)` of the 188-character conditional-hours original and `[0,147)` of the 263-character refund-action original. The full originals remain privately bound for validation; neither hidden suffix reaches a partial model request. Validate the retained complete package first, then these exact derivations. Do not weaken the existing full-source validator. Generate valid new passage handles from each partial evidence pack and bind their coordinates to the admitted complete original.

Freeze a reviewed challenge appendix before calls: true original-local absences, false world-absence claims, partial-original traps and visible-positive controls. Exact strings, labels, source reasoning and availability are private artifacts. Questions, candidate order and supplied source bytes must match within every pair. Keep the 66 full-case required opportunities per route/profile separate from the partial controls and their explicit available/withheld opportunities.

## Partial-source construction and coordinate binding

The existing complete-original context path is unsuitable for these controls: it derives boundary and structural metadata from the withheld suffix. Do not use that path and strip fields later. The canonical partial evidence pack contains the exact admitted prefix as its content/source_content and has neither `source_context` nor `_source_document_content`. Existing span construction and finalization then use only supplied text. The two prefixes begin at original offset zero and end after a complete sentence and whitespace; no clipped-token or unknown-start parsing claim is introduced.

Extend the pure constructor with an explicit optional binding:

```python
SourceScope.bind(originals, spans, *, chunks=())
# each admitted chunk: document_id, content_digest, start, end
```

The chunk digest must equal the digest of the exact admitted full original slice. IDs and offsets are strict integers; the interval must be nonempty and inside that original. Each chunk key `(document_id, content_digest)` must be unique, belong to an admitted original and be used by at least one supplied span. Reject duplicates, ambiguity, malformed, foreign and unused bindings. Reject a chunk binding that also attempts to redefine a span's existing `source_context` authority.

For a span without `source_context`, an explicit matching chunk binding maps its local offsets into the original after validating its entire chunk digest, exact local range and exact span text. Without a binding, retain the existing full-original digest rule or unknown-original behavior. A supplied binding must never fail into an unknown/legacy fallback. Coverage comes only from the mapped span union, never the chunk extent alone. The immutable scope retains the original identity and actual mapped windows; no hidden text or whole-original structure enters the model payload or finalizer.

This constructor extension certifies coordinates and supplied coverage only. It grants no parsing-context knowledge to legacy chunks; nonzero-start chunks must retain existing conservative continuation behavior. The diagnostic admits only the two reviewed prefix constructions above. Validate exact regenerated span equality and the complete serialized request, including boundary/date/value/structural metadata, against that prefix-only construction in both profiles before native admission.

## Runner seam and invariants

Reuse `run_model_comparison.run(manifest_path, output)` with a closed profile selection. Only exact kind `source-scope-diagnostic-v1` selects the trusted `scripts.source_scope_diagnostic` implementation; the existing diagnostic kind retains its current behavior. No arbitrary import paths or mutable monkeypatch replacement of protocol functions.

The profile exposes immutable prepared executions from `prepare(manifest, frozen)`, which validates source derivations, schedule and labels without requiring a preflight report. `validate_admission(manifest, frozen, prepared)` separately requires the completed actual-SDK report and exact wires, plus all shared admission checks. There is no skip-validation switch. Both preflight and native dispatch use `execute(prepared, orchestrator, capture, directory)`. Preserve the existing capture, route configuration, absolute deadlines, aggregate attempt accounting, cancellation/cleanup, terminal receipts and two independent admission reviews. Do not duplicate or relax the runner's admission/ownership loop.

Admission binds exact code, all input/output inventories, SDK/runtime identities, provider destinations, reviewed labels and limits. Both routes remain the previously authorized Google Gemini and OpenAI destinations behind the same LiteLLM aliases; reverify actual routing before dispatch. No new model route or whole-corpus transfer is authorized by this diagnostic.

Before admission, pinned-SDK localhost MockTransport preflight covers all executions: first and corrected reader requests, every fixed initial audit batch, and synthetic fresh/subset/protocol-correction paths. Inspect typed and legacy wire schemas, supplied source views, raw selections, resolutions and complete exact known SDK bodies. Dynamic native bodies remain captured and charged. No model is called during preflight.

Retain provisional ceilings of 128 attempts/3,600 seconds per execution and 1,536 attempts/14,400 seconds aggregate, subject to measured preflight review before freezing. Existing per-call timeouts, no output token cap, SDK retry disabling and cache-bypass controls remain. Provider-side cache hits or dependence are recorded rather than called independent trials. Exhaustion means incomplete; never truncate sources, claims, raw attempts or denominator entries to fit.

## Work sequence

1. Review this protocol and the private challenge/partial-availability artifact independently.
2. Implement the closed runner profile and immutable prepared inputs; test source matching, profile isolation, private suffix exclusion, admission mismatch, capture and cleanup failures.
3. Complete actual-SDK preflight, freeze the package and obtain two exact admission reviews.
4. Execute the bounded matched comparison once; preserve failures and unobserved opportunities.
5. Independently grade and reconcile every raw/final assertion and selected citation scope. Update the single evaluation record and choose the next step against the preregistered criterion.

No production activation, ingestion replay or GitOps deployment is part of this diagnostic. Whole-query testing, fresh held-out evaluation, visual UI verification and release gates remain separate obligations.


## Implementation validation

The constructor extension and closed runner profile are implemented. Both reviews
cleared exact prefix regeneration, private coordinate binding, unchanged legacy
execution, candidate-blind reading, raw scope receipts, empty audit notes and shared
ownership/cleanup. The five new SDK/profile tests and five legacy comparison tests
pass. The full backend suite passed 1,038 tests with 58 expected skips; the final
preflight coverage hardening additionally passed the focused profile suite.

The local pinned-SDK probe exercised all 96 executions with 744 localhost requests,
largest 78,215 bytes and total 19,086,210 bytes, with zero provider calls. It covers
first/corrected reading, all fixed initial batches and synthetic fresh initial,
correction and supported-subset audits. Review exposed a corrupted-preflight case
where empty dynamic unit sets could masquerade as path coverage. Admission now
requires exact candidate batches, matching expected IDs and ledger-derived subsets;
synchronized SDK/model/stage/hash corruption regressions reject that case. Both
reviewers cleared the implementation; the stricter checker accepts all unchanged
probe captures. This probe is not native admission and does not measure accuracy.

The ten-challenge private v2 artifact was independently reviewed against all six
retained originals, exact prefix packs, source/reference hashes and separate
availability denominators. Citation credit uses the same actual interval-union rule
in both profiles; typed-handle uptake is descriptive. Existing route grades remain
FAIL. The remote runtime and two existing route destinations were reverified before
packaging. Native admission will require a freshly generated preflight in that exact
runtime, rather than relaxing local/remote identity differences.
