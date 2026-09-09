# Original-table quantity evidence

Status: implemented and reviewed at `9953b89`; native qualification pending. Separate from prior frozen reader experiments.
Parent: [document-local evidence](document-local-evidence.md). Reproductions:
[value evidence findings](../audits/2026-09-09-value-evidence-findings.md).

## Problem and intended behavior

The deterministic value guard rejects valid observations when a numeric source
cell inherits its explicitly printed unit from a Markdown table heading or row
label. It also treats compound concentration units as prefixes (for example mg
inside mg/L). Adding another model cannot repair those lexical false rejections.

Add a small original-text quantity module behind the existing `value_mismatches`
interface. Preserve raw reference quotes and claim offsets. Its derived facts are
validation data, never replacement source text or proof of subject/predicate/date
association. Semantic checks, comparison scope and exact original ownership remain
required; table presence alone cannot certify a claim.

## Rules

- Parse only a complete structural Markdown table within one already validated
  original quote. Certify table ranges against the bound original document (or a
  known document-start chunk) before slicing; unknown preceding context cannot
  grant table inheritance. Carry only wholly contained certified ranges through
  citation trimming and reference validation. Parse raw Markdown before stripping
  presentation, so clipped code fences or HTML literals cannot become tables. Never join excerpts or infer a missing header from another quote.
- A numeric-only data cell can inherit exactly one explicit unit from its own
  column heading. A first-column nonnumeric row label with exactly one explicit
  unit may supply the unit for numeric cells in that row. If both give incompatible
  units, do not derive a quantity. Unit labels must use an unambiguous terminal
  unit (optionally parenthesized); reject numeric/scale annotations, scale words
  and trailing qualifiers rather than calculating a scale. Do not infer units from titles or prose outside
  the table, carry them between tables, or repair malformed/ragged structure.
- Keep signs, Decimal values, magnitude, scale and compound unit identity exact.
  No arithmetic, unit conversion, unconditional absolute values or sign removal.
  A table target and result are both recorded quantities, but are distinct roles;
  existing semantic record/condition checks must still reject interchanging them.
- Explicit currency codes may support their corresponding printed symbols as a
  less-specific rendering (USD/CAD/AUD to $, EUR to €, GBP to £). A symbol alone
  must never establish a currency code. USD must not become CAD, and a claim
  containing both a symbol and a code requires the specified code.
- Preserve compound concentration unit mg/L as a whole; mg, mg/dL and mg/L are
  not interchangeable. No conversion between 12 mg/L and 0.012 g/L is introduced.
- Retain all existing scalar/date/source-boundary gates. The separate signed-credit
  paraphrase failure remains unresolved: it needs its own direction-aware design.

## Validation

First reproduce the table/header false rejections through native audit -> finalizer
using controlled schema-valid responses, without network calls. Add positive
column-unit and row-unit controls, explicit currency/symbol rendering and compound
units. Add negatives for wrong amount/sign/currency/scale/compound unit, missing or
split header, clipped code fences/HTML, scaled headings such as `USD (thousands)`
  or `mg/L ×10³`, unsupported compound suffix `mg/L/min`, conflicting heading/row
  units, malformed/ragged table and units in
unrelated neighboring prose. Preserve the exact text and provenance of references.
Run prior value/date/boundary/adversarial tests and the full backend suite.

Both reviews must pass before a new native experiment includes this change. Keep
currently running reader manifests immutable. New native controls must count raw
false approvals regardless of deterministic rejection, compare the same code and
runtime, and include swapped target/result and charge/credit roles. Engineering
checks alone cannot qualify these quantity facts or the larger query architecture.
No deployment or release before representative and end-to-end gates.

## Post-review native qualification

After both implementation reviews pass, freeze a fresh experiment containing this
module and all reviewed reader/protocol changes. Repeat the unchanged development
set with `source_first`, `document_local` and `document_local_corrected`, three
repetitions per arm. Each arm retains the existing 960-native-invocation and
1,800-second ceilings, estimated 1.5M tokens, one SDK attempt, concurrency four,
90-second call and 120-second audit deadlines. Use the same measured SDK 1.54
runtime and model for each arm; do not transfer results to SDK 1.55. Original
source hashes, labels and claim encodings remain unchanged. The existing set
includes swapped target/result and charge/credit negatives. Preserve interrupted
prior manifests and report all scheduled assertions and raw approvals, even when
a later guard rejects them. No native run begins before the proxy rollout and
health gate; no independent holdout is consumed in this development experiment.

These are development controls, not full-pipeline or release qualification. The
signed-credit direction paraphrase remains a separate known failure; count it
honestly rather than changing expected labels or silently weakening signs.
