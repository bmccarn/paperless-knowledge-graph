# Query reliability: evaluation before implementation

Status: reviewed plan; G0 complete and G1 in progress. Production implementation and deployment remain unstarted and must satisfy the gates below.

Requested September 9, 2026. Tracker: [issue #33](https://github.com/bmccarn/paperless-knowledge-graph/issues/33). Frozen starting revision: `809046021573a5c0e9eeb65866f5e49dcfe6ae5b`, answer policy `source-audit-v25`, delivered through GitOps PR164. This document replaces the execution sequence and stale status matrix in [the earlier reliability spec](evidence-query-reliability.md). That document remains a record of existing contracts, implementation history and failed experiments.

## Outcome and scope

Ordinary questions about a person's documents should receive useful, source-faithful answers. A history question should preserve meaningful earlier observations and the latest documented observations for each relevant subject. A current-state question should distinguish what dated records establish from unresolved present-world status. When an independent fact cannot be established, retain a useful, independently verified answer where possible and explain the specific remaining gap.

The problem is general: interpreting forms, action stages, date roles, quantities, relationships and competing records while maintaining exact source attribution. Insurance is a representative live case. No production rule may depend on its document IDs, names, dates, policy types or exact question wording. Fixtures must include unrelated operational, employment, billing and measurement records.

Success requires both factual accuracy and task coverage. Abstaining from every answer, dropping the requested earlier/latest subjects, or assigning a high support score to a failed answer does not satisfy this spec. A structured model verdict is not proof of entailment.

This planning pass produces only this spec and its independent review. It starts no evaluation calls, application implementation, model/configuration changes or deployment. Future work must follow the gates below; no gate inherits a pass from an earlier release.

## What the evidence establishes

| Finding | Evidence and limit |
| --- | --- |
| Code checks alone have not predicted live success. | The v25 implementation passed 562 backend tests, both code reviews and CI, but its first live history query still withheld the answer. These checks establish their exercised contracts, not model accuracy. |
| A false semantic approval remains. | In retained v24 and v25 cases, an existing-state amount was described as a new election on the signing date although the form selected a different change. Complete evidence was available; the v25 semantic facets all approved the assertion. |
| Native source handles are unconstrained. | At the starting revision, `app/source_audit.py:44` permits any string and its parser accepts invented handles. The finalizer correctly rejects them. The v25 saved ledger contains unknown-handle rejections despite complete source admission. |
| Comparison applicability can reject a non-comparison. | A local native-adapter/public-finalizer reproduction passes a historical observation with null comparison scope and an empty ID list; adding an otherwise unused comparison-document ID alone causes rejection (`app/source_audit.py:121`). The saved live ledger lacks the raw optional metadata, so this is not proof of its precise live trigger. |
| Citation coverage can still lose values. | The v25 repaired answer asserted vehicle years missing from its chosen reference. This proves inadequate chosen-reference support; it does not prove those facts are absent from the archive. |
| The synthetic semantic controls were too easy to demonstrate improvement. | The same eight controls passed both v24 and v25. They are useful controls, but neither reproduce the live semantic failure nor justify another deployment. |
| The failure is not currently an output-token cap. | The latest retained run included a completed audit response above 24,000 output tokens. No cap was added in v25. Large inputs and successful transport do not establish correct interpretation. |

Private captures contain the exact cases, original source snapshots, versions and failed results. Do not copy OCR, personal identifiers, conversations or raw provider responses into GitHub. Do not infer unrecorded model reasoning from a saved status or reconstruct discarded fields as though they were captured.

## Contracts that remain mandatory

- Certifying facts come from original source text with exact document identity, intervals and digests. Generated summaries, graph hints, semantic explanations and earlier answers cannot acquire source authority.
- Check the complete assertion: subject, relationship, record/field role, selected options, action stage, quantities and temporal meaning. Existing state is not a dated transition; a request is not completion; a referenced instrument is not the current document.
- Keep numeric/date precision, conflicting-evidence, feedback and corpus-snapshot checks. Do not weaken them to produce a convenient answer.
- A partial answer must satisfy the question's frozen required aspects and retain a fresh, complete ledger for exactly the delivered text. Failed, malformed, conflicting or unavailable audits cannot inherit earlier approval.
- Preserve the complete eligible canonical audit manifest during production work unless a separately reviewed architecture decision explicitly replaces that contract and proves relevant counterevidence coverage. Context reduction is permitted for causal experiments, not as an undeclared production cap.
- Preserve saved conversations, review history, ingestion fingerprints, source documents and indexed data. No re-OCR, reindex, drift repair or migration is required by this plan.
- Preserve the primary answer and extraction model routes, existing production call limits, deadlines, cancellation and absence of output-token caps. Any proposed verifier-role change or additional production stage needs an explicit evaluated architecture decision; it cannot be hidden inside a successful rerun.
- Deploy only through reviewed immutable GitOps image pins, with a fresh writer check and verified Flux/runtime state. No imperative deployment edits or automatic rollout during experiments.

## Work products and evaluation interface

Keep one evaluation module at the existing query/audit interfaces. It accepts a frozen case manifest and a named implementation/configuration, and returns artifacts and scores. Production and evaluation must exercise the same native adapter, parsing, finalization and delivery code for the stage being measured. Do not create a second verifier that only exists in tests or use mocked supported verdicts to measure semantic accuracy.

The manifest records revision, prompt/schema hashes, model route/settings, evaluation date/date order, source text/identity hashes, source ordering, candidate units, expected assertions, required answer aspects, cache conditions and the allowed call/time budget. Cases and decisions are immutable within an experiment. A changed input, prompt, grader, threshold or configuration creates a new named experiment.

Each run retains every attempt, including protocol corrections, semantic failures, timeouts, refusals, dropped facts and unavailable results. Record raw stage input/output privately for evaluation only, plus request identifiers, actual usage, timings, reference failures, normalized verdicts and delivered output. Public summaries contain sanitized case IDs, counts and fixed diagnostics. Runtime logs must not become a raw-source capture mechanism.

The evaluation has four distinct levels:

| Level | What it proves | What it cannot prove |
| --- | --- | --- |
| Deterministic protocol tests | Parsing, exact handles, optional-field semantics, source/value/date gates and failure propagation. | That a model interprets the source correctly. |
| Real-model assertion evaluation | Whether the configured auditor accepts and rejects independently labelled assertions under representative context. | That retrieval finds all needed sources or the final answer answers the question. |
| End-to-end evaluation | Retrieval through delivery preserves required facts, rejects unsupported additions and returns useful answers. | Production browser behavior or exhaustive archive/world completeness. |
| Live browser and original-source acceptance | The reviewed release handles the reported workflow, source links, saved history and desktop/mobile presentation. | Universal accuracy across all future questions. |

## Dataset and scoring

Before changing behavior, build and independently review the dataset and grader:

1. Retain the existing failed live cases as development cases, including the false semantic approval, reference failures and withheld useful answer. Preserve the actual large context, candidates and batch grouping where captured. Label absent or reconstructed telemetry explicitly.
2. Add at least 32 synthetic assertions across four unrelated document domains, balanced between supportable observations and tempting unsupported assertions. Include selected versus unselected options, state versus change, request versus completion, referenced versus actual instruments, signature versus event dates, quantities/signs/units, and scoped comparisons versus current-world claims. Each negative has a positive counterpart so abstention cannot win.
3. Include compact and representative large-context conditions, repeated similar records, disjoint identifying/table/date passages, source-order changes and conflicting alternatives. Derive the stress condition from measured source/window/token distributions, not repeated generic filler alone.
4. Split synthetic source families into development and held-out sets before candidate work. Keep each semantic pair and all its wording/order variants in the same partition. An independent reviewer/custodian creates and validates the held-out sources and labels; candidate development cannot inspect their contents before G5. Record the access policy and case-manifest hash, and keep held-out content out of implementation diffs. Evaluate the holdout only after the candidate is frozen. After a G5 evaluation, that set is consumed for any subsequent candidate adaptation, whether it passed or failed and whether the later trigger was a live failure. Preserve it as regression evidence and obtain a fresh independently labelled holdout for the next adapted release decision. Predeclared aggregate reporting is allowed; case-level exposure is recorded and cannot be used for tuning while still calling the cases held out.
5. Have a separate reviewer confirm original support, unsupported inference and required answer aspects without seeing a candidate's score. Disputed labels are resolved before scoring; ambiguous cases remain reported and cannot be relabelled to rescue a candidate.

For model measurements, use three predeclared repetitions per condition where independent execution can be established. Verify application cache state and inspect proxy/provider caching where available. Cached or unverifiably repeated outputs are reported as dependent observations, not three independent samples. Do not clear global caches or add meaningless prompt nonces. The manifest must state the supported request-scoped cache control or the sampling method before execution.

Score separately:

- **False acceptance:** an independently unsupported assertion accepted by the auditor or delivered as a fact. Report both, even if another guard happens to withhold the final answer.
- **False rejection:** an answerable assertion rejected despite the necessary original support being supplied. Separate interpretation, reference selection, protocol and transport causes.
- **Task coverage:** which predeclared answer aspects survive delivery, including meaningful earlier and latest subject observations. Count missing required aspects as failures even when every delivered claim is supported.
- **Attribution:** every delivered material assertion has exact original support with matching relationship, value, time and scope. A valid handle alone is insufficient.
- **Reliability and cost:** completion and abstention counts, actual model calls/tokens, cache observations, and stage/end-to-end median and maximum duration. Do not present small-sample percentiles as established service performance.

## Gates and stop conditions

| Gate | Work and deliverable | Exit criterion | Current state |
| --- | --- | --- | --- |
| G0 — plan | Review this spec on Standards and Spec axes; reconcile older execution instructions. | Material plan findings closed; planning request ends without starting implementation. | Complete; both reviews clear |
| G1 — reproduce | Build the evaluation harness, frozen dataset/labels, raw private capture contract and baseline report. This is evaluation-only work. | Deterministic defects reproduce through production interfaces; the real semantic failure reproduces in a controlled native-model evaluation, or a measured nondeterministic failure rate is established. | In progress; [evaluation report](../audits/2026-09-09-query-evaluation.md) |
| G2 — discriminate | Minimize the semantic reproducer while retaining its failure; preregister bounded, one-variable experiments against ranked hypotheses. | At least one candidate materially improves the reproduced failure without failing positive controls; all outcomes retained. | Not started |
| G3 — choose design | Write a short architecture decision with measured alternatives, the chosen interface, call/data flow, failure behavior, cost/latency and implementation tasks. Independently review it. | Both review axes pass; every proposed production change maps to evidence and a regression. | Not started |
| G4 — implement | Implement the complete selected design on one branch, including justified protocol fixes. Run meaningful regressions and required full checks. | Source-head review and CI pass; no production deployment has occurred. | Not started |
| G5 — qualify candidate | Freeze code/config and run held-out real-model and end-to-end evaluations. | All release criteria below pass on that exact candidate. | Not started |
| G6 — release once | Review image pins, drain writers, deploy through GitOps and verify the running revision. Perform live browser/source acceptance. | Live gates and preservation checks pass; restore temporary schedules through reviewed GitOps and close the tracker. | Not started |

G1 must produce an actual command that reaches the relevant production interface and goes red on the defect. Re-grading a stored wrong answer tests the grader only; it does not demonstrate a reproducing model loop. The eight existing easy controls are insufficient. Deterministic reproductions should take seconds. Real-model reproduction may take a bounded audit invocation because model behavior is the subject; document that exception rather than substituting a mocked verdict or another full production deployment.

Before each experiment, freeze the exact case count, batch grouping, three repetitions, maximum provider attempts, model/token cost estimate and elapsed-time ceiling. Initial G2 work evaluates at most two candidate approaches plus baseline. The current audited-unit/batch/correction limits bound each call sequence. Reaching an experiment ceiling means stop and report incomplete results; it does not authorize retries or new production caps.

If G1 cannot reproduce the semantic failure, stop production design work and improve capture or obtain the required evaluation access. If two G2 approaches fail, return to the evidence and hypothesis ranking instead of appending another prompt rule. If G5 fails, retain the failure and return to G1/G2 with a revised experiment. If G6 fails, do not patch and redeploy in the same acceptance cycle: retain the candidate/result, assess whether a reviewed rollback is needed, and reopen qualification. No reroll of a failed run counts as a pass.

## Initial hypotheses to test, not implementation decisions

Rank these after G1 minimization. Their inclusion is not a diagnosis or permission to implement all of them.

| Hypothesis | Controlled comparison | Falsifying result |
| --- | --- | --- |
| Candidate wording anchors the verifier. | Compare current candidate-led judging with a source-first assessment in which source observations are derived before exposure to the proposed assertion. Both retain original evidence and final independent attribution; derived observations gain no source authority. | No improvement on the retained false approvals, or loss of valid positive observations. |
| Evidence presentation burdens association and handle selection. | Compare flat windows with document-grouped context and request-owned handles while preserving the same complete eligible source information. Measure semantic and reference effects separately. | Errors persist at the same rate, or improvement relies on dropping relevant source/counterevidence. |
| Repair introduces unnecessary factual obligations. | Compare the current repair with a question-coverage contract that rebuilds only required source observations, while retaining every required earlier/latest subject. | Lower error counts come from omitting requested facts, or remaining required claims stay wrong. |
| The selected verifier is inadequate for the task/context. | Only after the above controls, propose a separately named verifier-role experiment and compare identical cases/settings under a declared route change. | No stable improvement, unsupported positive claims remain, or resource/latency costs fail the declared criteria. |

Known protocol candidates are narrower: bind native reference choices to the exact request-owned source set, and prevent unused comparison metadata from creating applicability for a coherent non-comparison. Preserve final source validation and genuine comparison/negative-facet rejection. These have local reproductions, but neither is a semantic solution and neither warrants a standalone production rollout under this plan.

## Release criteria

The exact candidate must satisfy all of the following. Thresholds and labels are frozen before G2; changing them invalidates the comparison and requires a new reviewed experiment.

1. **No false semantic approval on critical negative cases**, including every retained failure pattern and the held-out state/action/field-role/date/relationship controls. Report the finite sample size; this is not a claim of zero universal error.
2. **No loss of required positive facts.** Every critical positive counterpart and every required answer aspect in the frozen answerable end-to-end cases survives as a supported observation. Qualifications are permitted; dropping the requested history or latest subjects is not.
3. **Complete correct attribution and delivery.** All delivered material claims pass original-text, semantic, numeric/date and candidate-binding review. HTTP, streaming, cache, saved conversation, timeline and source-detail behavior agree. Fault controls still withhold invalid/unavailable/conflicting results.
4. **Demonstrated improvement over a failing baseline.** A candidate must improve the reproduced semantic failure on matched inputs without new critical failures. An unchanged 8/8 easy-control score, a prettier schema or fewer malformed outputs alone cannot pass.
5. **Measured resource behavior.** No timeouts on required acceptance cases, no hidden retry/stage increase, no output cap and no unreviewed model route change. Compare stage and complete-query times/calls/tokens under matched cache conditions. A material regression requires an explicit design tradeoff and review before release, not a longer timeout to conceal it.
6. **Full engineering validation.** Both exact-head review axes, backend offline/disposable-datastore checks and required frontend graph/lint/type/build/browser CI pass. These remain necessary but cannot substitute for criteria 1–5.

End-to-end qualification includes at least one history and one current/documented-state question in each synthetic domain, positive and unresolved-status behavior, and the retained real cases where evaluation authorization permits. Predetermine the expected aspects and run count in the manifest. Do not publish private case contents or model responses as test fixtures.

Final live acceptance preserves the unchanged reported history question in Timeline mode and the original current-policy question in Strict mode. Run the history question twice in separate conversations, each with its exact application-cache entry absent; a fresh conversation alone is insufficient. Use only scoped, reviewed eviction when needed. Report the provider-cache independence limit honestly. Check every delivered claim against current originals and inspect desktop/mobile rendering, all timeline/source controls, saved restoration and error/partial states with actual computer/browser control. Controlled browser fixtures cover failure states without trying to force them by mutating production data.

## Operating constraints and outstanding dependencies

The last verified v25 state preserves KG schedule controls `0/0/10`, five separately paused Brain jobs, model routes and review history. This is an observed starting state, not permission to leave temporary controls paused indefinitely. Recheck live state before future operational work. If evaluation will be prolonged, prepare a separate reviewed GitOps schedule-restoration decision; do not bundle it into a query fix or restore automatically during this planning request. Before serial acceptance, establish and verify the required stable corpus again.

Standalone replay of private saved payloads previously received an automatic approval rejection. Local read-only reconstruction and synthetic evaluation are distinct from that action. Before private model evaluation, prepare a concrete manifest with the exact cases/source scope, configured destination, retention policy, call/time bounds and command, then obtain the required specific approval. Do not bypass the rejection through another script, provider or tool. If this dependency remains unavailable, report the semantic evaluation gate as unproven; synthetic success cannot silently replace it.

Keep all existing failed releases and test receipts. Update this document's gate table and one evaluation report with revision-specific results; do not grow another sequence of fixes whose latest appendix contradicts the execution plan. Implementation PRs must link the selected G3 decision and G5 evidence. Close issue #33 only after G6, preservation and schedule disposition are complete.

## Planning review and handoff

Independent Standards and Spec reviews both passed at `d1b3734142e50b1b578584e63e6a5aba3e29de06` on September 9, 2026, with no remaining findings. Review identified and closed a holdout-isolation gap: an independent custodian owns hidden cases before qualification, and any evaluated set becomes regression evidence if its results influence subsequent candidate adaptation. This review record and the G0 status were added after those reviews; the reviewed requirements are unchanged.

The planning deliverable is complete. G1 evaluation work is underway and recorded in the gate table and linked report. Complete its dataset, grader, evaluation-access and semantic-reproduction requirements before advancing. No architecture candidate has been selected yet.
