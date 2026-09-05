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
