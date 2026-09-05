# Independent finalization review

The implementation review exercised the public finalizer, timeline validator and QueryEngine delivery interfaces with adversarial provider outputs. The first 13 new tests produced 12 failed assertions and three errors; additional contradiction/current-planner cases also reproduced failures before their fixes. The final hardening suite contains 16 passing tests, with two additional public QueryEngine timeline-projection tests.

## Concrete fixes

- Quotes cannot obtain `321` by cutting digits from `4321`, a minus sign from `-321`, or decimals from `321.50`. Two characters of source context around selected span edges preserve this check when a budgeted span ends inside a number. Saved reference offsets still identify the exact original source quote.
- Numeric values and recognized units are checked as adjacent quantities. Separate `5 mg` and `90 kg` references cannot establish `90 mg`. The registry covers common document, currency and medical units; it is not a universal measurement parser or a substitute for semantic assessment.
- Impossible ISO dates fail even when OCR repeats the same invalid text. Timeline year/month precision must match a complete source date token rather than a prefix of an invalid longer date. Unimplemented date-format conversions remain unresolved.
- Conflicting records sharing one evidence ID fail instead of silently selecting the first. Model-created document links are discarded before auditing and replaced only by validated document citations.
- Every ledger identifies its candidate digest. Each repair attempt starts a fresh ledger, so timeout or provider failure cannot attach old claims to a new candidate revision.
- A malformed temporal scope cannot escape as a server exception. Historical qualification requires historical assessments; unscoped claims are not relabeled historical. Explicit current wording or a current assessment triggers temporal acceptance even if the planner sets `requires_current` to false.
- Timeline references are limited to the exact source manifest supplied to extraction. The independent reviewer sees other relevant supplied spans, including contradictory records, and published references are the ones that reviewer actually cited. Malformed review records fail closed.
- Public query results retain timeline-only document citations and tail quotes. A report opened before the final gate removes the earlier timeline event from the result while leaving independently supported, undisputed facts available.

## Validation and limits

The final targeted run passes 52 tests: finalization, temporal behavior, the new adversarial cases, QueryEngine delivery, public timeline projection, ASGI persistence and factual scorer regressions. `git diff --check` passes. Positive sourced answers, positive source-bound timelines and explicitly qualified historical answers remain accepted.

These checks establish deterministic acceptance and projection behavior. They do not prove semantic entailment, completeness of retrieval, correctness of all OCR or live-model accuracy. Evidence selection remains budgeted; the public manifest reports which spans were reviewed separately from all available spans. Conservative numeric, date and temporal rules can require review for otherwise valid wording or unsupported conversions.
