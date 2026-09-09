# Evaluation suites

`fixtures/accuracy-v1.json` is the factual scoring corpus. It contains synthetic document IDs, exact chunk text and six independently written answer/timeline/abstention expectations. The scorer validates reference offsets and content digests against this file, not against an evidence pack supplied by the model. Numeric confidence and trust are reported but never determine passing.

The accepted-answer lists are intentionally conservative. Unreviewed paraphrases fail rather than being assumed equivalent. Extend these lists through review when broadening the fixture suite. Passing does not establish real-model semantic accuracy, retrieval recall or completeness on a private archive.

Run deterministic scorer regressions without a server or model:

```bash
python -m unittest tests.test_eval_harness -v
```

For API execution, use an isolated evaluation instance containing the fixture document IDs and exact indexed chunk text. Do not point this suite at a private archive and interpret unrelated IDs as the fixture corpus. The runner does not seed or mutate document stores:

```bash
python scripts/eval_harness.py --base-url http://localhost:8484 --cases evals/fixtures/accuracy-v1.json --mode strict --json
```

`canonical_questions.json` retains the six earlier private-archive questions as **smoke checks**. Their source facts were never labeled independently, so their results do not measure factual accuracy. They now require terminal output and actual source IDs, and all six former zero-source keyword-only forgeries fail.

Reports separate factual-case count, incorrect supported factual answers, abstentions and execution errors. A high abstention rate can reduce wrong answers while still making the application unhelpful; assess both together. Live API/model evaluations were not run as part of these deterministic checks.

## Query reliability development evaluation

The [reviewed plan](../docs/specs/query-reliability-evaluation.md) separates native
assertion accuracy from end-to-end usefulness. `reliability/development.json`
contains 64 independently reviewed development assertions across four domains.
It is a compact/multi-record control set, not representative large-context or
private-corpus acceptance. Held-out contents remain outside this repository under
independent custody and cannot be used during development.

Prepare an immutable experiment without importing application clients or making
model calls:

```bash
python scripts/eval_source_audit.py evals/reliability/development.json \
  --manifest /tmp/unique-baseline-manifest.json --model gemini-3.8-flash \
  --runtime /tmp/reviewed-runtime.json \
  --repetitions 3 --max-attempts 96 --seconds 1800 --estimated-tokens 250000
```

First capture `runtime_snapshot()` from the intended isolated execution environment
into an owner-only JSON file and review its destination, timeouts, concurrency and
installed package versions. Preparation does not infer these from local defaults;
execution rejects any mismatch. This capture imports configuration but makes no
model calls or datastore connections.

The token number is a resource estimate, not an output cap. Review cost against
the configured provider rates before execution. The manifest freezes dataset and
application source hashes, ordering, batch policy, repetitions and limits. A
changed case, grader or implementation requires a new experiment. Preparation
refuses held-out datasets; G5 needs its own frozen qualification manifest.

After approving the concrete experiment and verifying the configured destination,
run from an isolated process with the same model configuration as the baseline:

```bash
python scripts/eval_source_audit.py evals/reliability/development.json \
  --manifest /tmp/unique-baseline-manifest.json --execute \
  --output /tmp/unique-private-baseline-results
```

This uses the actual `StrandsQueryOrchestrator.audit_answer_units` and
`AnswerFinalizer._audit` path, including normal protocol corrections and original
source validation. It does not invoke retrieval, repair, final delivery or stores.
It observes its own adapter and request-owned SDK results in an isolated process;
no production files or configuration are modified. The process-local SDK binding is
restored when evaluation ends, and nonterminal result text is retained before the
production adapter discards it. Each exact model input/output and normalized audit is written to
owner-only files in a new owner-only directory. Earlier artifacts cannot be
overwritten. A timeout/cancellation retains the sent requests and available
responses. Keep private artifacts out of GitHub; default console summaries contain
counts only. Native request identifiers are not exposed by the current adapter and
are reported unavailable rather than invented. Native usage and stop diagnostics
are retained; unavailable usage is not evidence of zero cost.

A false auditor approval fails even if a later source guard blocks the assertion.
Missing positive facts and unavailable audits fail too. Cached repetitions are not
independent samples; this runner bypasses the application cache but cannot prove
proxy/provider independence and reports that limitation explicitly. Passing this
suite cannot establish G1 semantic reproduction or G5/G6 acceptance by itself.

Offline checks for the grader and two retained protocol defects:

```bash
python -m unittest tests.test_eval_source_audit -v
python scripts/reproduce_audit_protocol.py
```

The second command intentionally exits 1 on the frozen v25 baseline. Its synthetic
transport responses exercise protocol behavior only, not model semantics. Keep its
red result separate from a real-model reproducer.

Captured-window development cases may provide `evidence_pack` and `source_capture`
in addition to original `documents`. Before any invocation, the runner rebuilds
each supplied window using production chunking, verifies evidence identity and
source-context digests, and preserves the supplied ordering and boundaries.
This supports representative reconstructed contexts without replacing them with
whole-document concatenation. It confers no authorization to replay private data;
the concrete private experiment requires its separately scoped approval.

Copied table headers can make a production-reconstructed window noncontiguous in
its original. Default admission rejects that case. The explicit preparation flag
`--allow-noncontiguous-reconstruction` retains it unchanged only as diagnosed
invalid baseline input, writes per-case continuity diagnostics, and forces the
experiment to fail regardless of model scores. Never use this option to qualify a
candidate or silently replace original-window boundaries. Certifying overrides,
changed source titles, and invalid document-context offsets always fail admission.

For a separately frozen cache-controlled experiment, preparation accepts
`--proxy-cache-policy bypass`. The runner adds only request-body
`cache: {"no-cache": true, "no-store": true}` through the native SDK's `extra_body`.
The installed proxy's asynchronous cache handler honors these read/write controls.
No global cache is flushed, no prompt nonce is inserted, and messages, response
schema, model route and output allowance stay unchanged. The chosen policy is
captured in the manifest and each attempt. This requests proxy bypass; it does not
prove upstream sampling independence or establish billed usage.
