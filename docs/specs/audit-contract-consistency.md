# Audit contract consistency and bounded correction

Status: diagnosis reproduced and both independent plan reviews clear; implementation
and fresh qualification pending.

## Reproduced problem

The source-relative candidate passed its twelve Strict cases, then failed the
thirteenth all-mode case. All ten raw factual approvals were source-supported, but
the final answer withheld three required meanings. A repaired historical comparison
had affirmative temporal/comparison checks and retrieved-source references while
combining `historical` scope with `retrieved_comparison` framing. That combination
is outside the accepted wire contract. It became a factual rejection instead of
entering the existing bounded protocol-correction path.

The exact captured question pipeline reproduces the fallback offline. A minimized
one-observation/two-original replay has the same rejection. Changing only the
`temporal_scope` field to the already valid `documented` value permits acceptance;
the full controlled replay then preserves every requested meaning. A separate
controlled retention intervention also preserves the already approved observations.
These are causal code-path controls, not fresh model qualifications. The failed
native run remains failed; its other 35 scheduled cases remain unrun.

## Decision and scope

Complete the existing distinction between output-contract inconsistency and a
negative factual judgment. Reuse its single bounded correction opportunity for
cross-field metadata contradictions when the whole batch has affirmative model
verdicts, no unresolved assumptions and no negative semantic facets. A correction
must reassess the unchanged claims against the unchanged source snapshot; it is
never instructed to approve them. It counts as another native call in that audit
invocation, not a new scheduled case or an unlimited retry.

Do not normalize contradictory metadata into acceptance. Do not change the accepted
temporal meanings, reference membership, comparison alternatives, quantity/date
checks, subset safety rules or release thresholds. The controlled observation-
retention alternative remains an architecture candidate, not part of this fix.

This applies to the audit contract in every query mode and domain. It contains no
rule about a specific question, source title, subject, number, date or wording.

## Contract invariants

Use one explicit definition of valid scope/frame combinations for correction
validation and the finalizer's defensive validation:

| Calendar/applicability scope | Assertion frame |
| --- | --- |
| historical | source_observation |
| none | source_observation |
| none | none |
| documented | retrieved_comparison |
| current | present_world |

The strict native-row contract always supplies these fields. Legacy direct-auditor
adapters may omit optional framing; preserve their existing validation behavior
rather than imposing new required fields on them.

An active comparison must use the documented/retrieved-comparison combination,
the retrieved-documents comparison scope and a nonempty typed document list.
Non-comparisons cannot carry active comparison metadata. Preserve the existing
inert-list compatibility rule for historical source observations whose comparison
check is not applicable and whose comparison scope is absent. This rule does not
authorize any claimed comparison or source membership.

An applicable temporal/comparison assertion cannot simultaneously mark its required
facet not applicable. Existing parsing and source checks still reject malformed
objects, unknown handles, foreign comparison documents, missing required referenced
documents, unsupported predicates, conflicting sources and wrong quantities.

Classify metadata inconsistency before the finalizer treats it as a semantic
rejection, but only after checking the entire parsed batch for negative judgments:

- Any unsupported, missing or conflicting model verdict suppresses this correction.
- Any `not_established` or `contradicted` semantic facet suppresses it.
- Any unresolved assumption suppresses it.
- A negative sibling suppresses correction for the batch as well.

When correction is suppressed or fails, preserve conservative rejection and all
diagnostics. When a correction returns a factual rejection, preserve that verdict.
No already observed false approval is erased from evaluation counts.

## Implementation plan

1. Add a reusable metadata-coherence check for the existing contract and reuse its
   accepted scope/frame definition at the finalizer boundary. Keep parsing's
   fail-closed output semantics intact for uncorrected responses.
2. Extend `validate_scope_consistency` to detect these cross-field contradictions
   after its existing batch-wide semantic-negative guard. Return the existing fixed
   protocol error through the existing correction channel; do not send arbitrary
   source/model text as correction instructions.
3. Make the auditor instructions state the valid combinations directly. Clarify that
   retrieved-record comparisons include historical changes as well as latest-record
   comparisons. Source reporting and calendar date roles remain distinct.
4. Bump the finalization policy identity so cached/saved responses cannot acquire
   the new contract implicitly. Bind this spec in the next evaluation manifest.
5. Independently review implementation and exact validation evidence, then freeze
   a fresh candidate for the complete initial/all-mode qualification. No resumption
   or passing-subset reuse of the failed run; live retrieval stays gated.

## Verification

Write red regressions before implementation at the actual native-parser/finalizer
boundary, then at the complete question-pipeline boundary including the preceding
editor. Cover valid corrected historical changes and latest comparisons in all
four modes. Assert identical question, units, source snapshot, requirements and
request identity across correction, with every dispatch retained and counted.

Also verify repeated invalid metadata stops after one correction; a corrected
negative stays rejected; every negative facet/verdict/assumption and a negative
sibling prevent another vote; invalid comparison/source IDs and numeric/date
mismatches cannot be repaired by metadata alone. Valid undated source reporting,
historical observations, explicit comparisons and current-world rejection controls
must retain their established behavior. Test direct finalizer defenses even when
the normal native parser is bypassed by a controlled test auditor.

Mixed correction causes share the same two-dispatch ceiling: unknown handle or
malformed response followed by inconsistent scope, and the reverse order, cannot
start a third call. Exercise this after an editor pass too. The existing audit
deadline and cancellation owner must not restart for a different correction reason.

Re-run the original unchanged capture to confirm it still cannot auto-certify a
contradictory response. Separately replay a controlled correction and final coverage
response through the full pipeline to establish the repaired path. Run the relevant
regressions, full required checks and exact-candidate CI. Native qualification must
independently demonstrate the correction succeeds without lost positive facts or
new false approvals; controlled replays cannot establish that result.
