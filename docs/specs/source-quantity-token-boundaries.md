# Source quantity token boundaries

Status: plan and implementation independently reviewed and clear on both axes.
Local validation passes. No native rerun or deployment.

## Problem

The unchanged deterministic value guard rejects an explicitly written currency
amount immediately after a slash. A source such as `Limits: $200,000 each person/$600,000 each event.`
fails to support `Coverage is $600,000 per event.` at the quantity-presence layer.
Adding spaces around the slash makes that layer pass. This reproduces source-correct
coverage rejection in the frozen recovery comparison, independently of model output.
The same lexical defect can affect prices, billing and any other domain.

## Scope and invariants

Recognize a complete explicitly prefixed currency amount after a separator without
requiring presentation whitespace. Keep numeric value, sign and currency identity
exact. Do not infer currency from an unlabeled table, convert units or percent
names, infer a century, reconcile arithmetic, or weaken semantic/role verification.
Compound units, malformed numeric suffixes and identifiers must retain the existing
maximal-token protections. Exact original quotes, offsets and source ownership stay
unchanged. A recognized amount is presence evidence only, never support for its role
or the surrounding assertion.

## Plan

1. Add a failing regression at the actual AnswerFinalizer/validated-reference seam
   for an original with compact slash-separated currency values, preserving exact
   source quotes and checking accepted delivery with a supported semantic response.
2. Add a cross-domain presentation matrix (compact versus spaced separators),
   signed and distinct currency cases, and adversarial compound/identifier/wrong-value
   controls. Do not use personal source contents or identifiers in repository fixtures.
3. Narrowly correct source quantity token boundary handling. An exception must
   require a complete currency-prefix amount; it must not turn a compound unit's
   denominator into an independently supported unit.
4. Run structural quantity and finalizer regressions, then the offline backend
   suite. Independently review standards and spec. Replay the affected frozen
   ledger through the new deterministic guard locally as a diagnostic only.
5. Record before/after outcomes separately. Preserve the original failed native
   run and both grades. Fresh admitted whole-query/held-out/UI/GitOps gates remain
   necessary; this local parser repair cannot authorize recovery activation.

## Separate unresolved problems

The recovery pass did not restore the known omitted meanings. It needs a separately
reviewed interpretation design, not a tokenizer change. Short-year calendar forms
currently retain unspecified-century uncertainty, and unlabeled table currency and
percent-word equivalence need explicit source-context policies before expansion.
Incorrect raw semantic approvals require their own source-audit investigation.
These concerns are not silently included in this narrowly reproduced lexical fix.


## Local implementation evidence

Two new finalizer/validated-reference regressions failed six subcases before the
change. The currency-prefix exception now requires a complete amount and rejects
an immediately preceding consumed compound-unit denominator. The 19 structural
quantity tests pass, including currency/sign/value and malformed-token negatives.
The full backend suite passes 984 tests with 58 expected opt-in skips in 39.927s.
Offline replay against the frozen original ledgers removes all eight slash-related
quantity mismatch rejections across both arms/repetitions. This replay tests only
the deterministic guard; it does not re-audit semantics or rewrite the failed run.

Both standards and spec implementation reviews cleared the change independently.
