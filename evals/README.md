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
  --repetitions 3 --max-attempts 96 --seconds 1800 --estimated-tokens 250000
```

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
It instruments only its own adapter instance; no production files or configuration
are modified. Each exact model input/output and normalized audit is written to
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
