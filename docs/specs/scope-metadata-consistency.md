# Correct contradictory scope metadata without retrying factual judgments

Status: proposed from the completed original-table native experiment.

The auditor sometimes declares an undated source observation historical while
marking its temporal check not_applicable. The parser downgrades the self-inconsistent
metadata, losing a supported selected-form action in two corrected-local repetitions.
This is distinct from not_established/contradicted facets or unresolved assumptions.

Use the existing single content-free protocol correction for internally inconsistent
scope/check combinations, rather than treating those combinations as a new source
judgment. Do not normalize a required facet to supported or erase declared scope.
The correction sees the same question, candidate, original sources and schema; it
must choose consistent metadata and reassess the sources without any request for a
supported answer. A second inconsistency remains unavailable; no new attempt budget.

Keep schema parsing/normalization and the raw verdict accounting intact. Validate
scope/check consistency as a separate native-adapter step after parsing so the
schema-valid initial raw approval remains captured and scored, even if later
correction rejects it. Emit only an allowlisted content-free reason. The parser's
existing direct-call behavior remains conservative; no caller gains authority from
inconsistent fields. Historical unused comparison-list normalization stays as already
reviewed. Do not retry not_established, contradicted, assumptions, source/value
mismatches, or otherwise consistent unsupported verdicts.

Eligible inconsistency: a not_applicable temporal check with non-none temporal
scope/assertion, or a not_applicable comparison check with declared comparison scope
or compared documents that survived the existing inert-metadata normalization.
Semantic-negative precedence applies to the entire batch: if any row has a raw
unsupported/missing/conflicting verdict, a not_established/contradicted facet, or
unresolved assumptions, decline this new correction trigger and retain the
conservative parsed decisions. A scope inconsistency cannot reroll that row or its
rejected siblings. This deliberately cannot repair mixed-positive/negative
development batches; their existing failed scores remain. It can repair an
otherwise supported candidate whose only defect is inconsistent metadata.
Native tests must cover correction to a consistent supported undated observation,
repeated inconsistency, correction to unsupported, unchanged real semantic rejection,
source/candidate conservation, mixed-signal and mixed-batch negative precedence,
and raw false approval retention across correction.
Run all existing protocol, date/temporal and evaluator controls. Review before a
fresh native measurement; do not change the completed frozen scores or fixtures.
