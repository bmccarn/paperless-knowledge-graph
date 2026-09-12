# Source-audit protocol correction and irrelevant comparison metadata

Status: separate implementation after review; no release claim. Parent:
[document-local evidence](document-local-evidence.md), issue #33.
Baseline: `120938b7ade81635f41f9c5caa45325ed25fdbd0`.

The offline native adapter/finalizer reproducer currently fails both required
behaviors. These are deterministic defects, independent of semantic model accuracy.
Keep the document-local experiment frozen at its reviewed revision; do not run its
matched conditions with these fixes mixed in.

## Required behavior

At the native auditor interface, parse decisions against the exact supplied source
handle set as well as exact unit IDs. An invented/foreign handle is a protocol error
with a content-free reason, eligible for the existing single correction attempt.
Do not silently drop it or certify against a different document. A second invalid
response stays invalid. Preserve the source manifest, original-source validation,
unchanged units and all semantic checks during correction. No extra retry or token
cap; no source text in error messages. Valid references do not prove entailment.

For an individual historical observation with comparison check `not_applicable`,
null comparison scope, and no documented/retrieved comparison assertion, an unused
comparison-document list must not create `semantic_comparison`. Normalize the
irrelevant list to empty so it cannot appear as comparison evidence downstream.
Keep inconsistent comparison scope or temporal comparison assertions rejected.
Keep `not_established`/`contradicted`, unresolved assumptions, predicate, record role,
conditions and temporal failures rejected. Real comparisons retain their required
source sets and finalizer checks. Validate all metadata types before normalization.

## Acceptance

Exercise native auditor -> finalizer with controlled responses: an unknown handle
followed by a valid original handle receives exactly two calls and succeeds; repeated
foreign handles receive only two calls and fail; corrected malformed output cannot
hide semantic contradictions; supported historical observations with unused IDs
survive; actual comparisons cannot evade their checks. Test source text/units remain
unchanged on correction, error messages omit foreign text, and default parser
callers without a supplied manifest preserve compatibility.

Run `scripts/reproduce_audit_protocol.py` before/after, relevant parser/finalizer
regressions and full backend checks. Controlled transports prove protocol behavior,
not native semantic quality. Updated frozen native measurements, representative
coverage, holdout and all-mode/browser qualification remain necessary before release.
