# Source-relative temporal acceptance

Status: implementation reviewed on both axes; fresh qualification required. Native v6 failed and remains failed.

## Problem and reproduced boundary

A supported report about a source is not necessarily a claim about present reality.
The current temporal contract conflates that assertion frame with whether the answer
mentions a calendar date. Its whole-answer gate also requires every unrelated claim
to carry the same temporal qualification once any unit contains a current-status
word. This can withhold a fully source-supported answer after two successful audits.

The frozen v6 final development case reproduces the public fallback through the
actual source-reader, finalizer, editor, re-audit and delivery path using captured
model responses, with no native calls. A single source-reported field containing
`current` and an audit of `none/none` is sufficient. Replacing only that word removes
the gate; explicitly marking the unit a source observation qualifies it; adding an
unrelated non-temporal unit makes the whole answer unresolved again. Eleven other
cases passed, but the twelve-case run failed. No existing result is rescored.

## Contract

Keep the existing semantic auditor and its source/value/subject/condition checks.
Do not introduce another voting agent, a source-report phrase whitelist, document
name exceptions, insurance-specific rules, or automatic approval from citations.

Separate the existing assertion frame (`temporal_assertion`) from calendar scope
(`temporal_scope`). A source observation is an assertion about what an original
records, whether or not a calendar date appears in that assertion. It may have
historical calendar scope or no calendar scope. Reporting a source field named
current, active or latest does not alone assert present-world validity. Do not assign
a signature, publication or retrieval date to an event or value without source support.

The native auditor must explicitly use source_observation for source-relative
reports, including undated fields. The combination none/source_observation is valid
when the temporal check is not applicable; historical/source_observation requires
supported temporal meaning. Existing none/none remains a genuinely non-temporal
assertion without an explicit source-observation classification. It cannot clear an
ambiguous current-status unit or independently answer a current-world question.
Negative checks, assumptions and negative verdicts remain rejections, never correction
votes. Unknown or contradictory metadata cannot establish temporal eligibility.

Evaluate applicability per audited unit. A current-status word or explicit current/
present_world classification requires source-relative resolution for that unit.
Unrelated non-temporal units must not invalidate another unit's legitimate source
observation or retrieved-document comparison. Conversely, one qualified unit cannot
launder a different unscoped/current-world assertion. A plan cannot disable this gate.
Current-world questions still require a source-relative answer and the application's
qualification that present-world validity and archive completeness are unestablished.
Purely non-temporal claims alone cannot establish current status.

Do not infer that a unit is unrelated to a current-world question merely because
its own text lacks a current-status word. When the question or plan requires current
status, every retained unit needs an explicit source-relative frame; an unscoped
none/none unit remains unresolved even beside qualified units. Undated source reports
can use none/source_observation. Only when the current-status cue originates solely
in another answer unit may unaffected none/none units remain ancillary. This retains
a conservative request-level boundary without another relevance classifier.

Preserve the explicit comparison contract: latest among retrieved records needs the
relevant retrieved-document scope, source identities and checked alternatives. A
source field named latest is not automatically a comparison performed by the answer.
Preserve current-world rejection even when a document lists an active-looking term.
Active comparison scope or identities outside documented/retrieved_comparison are
contradictory metadata and must be rejected, not silently discarded. Preserve only
the existing narrowly defined normalization of inert historical comparison IDs.
Headings and qualifiers remain audited claims, without special exemptions.

Repair should receive concrete unit-level temporal applicability failures rather
than a generic missing-source message alone. An editor may clarify a source report,
never relabel its unedited unsupported meaning as historical. Any revised candidate
requires a fresh full audit and existing coverage/conservation checks.

## Implementation and validation plan

1. Freeze the failed v6 artifact and both failure grades. Keep both no-model replay
   scripts as private diagnostic evidence; do not change their captured responses.
2. Extend the semantic decision validation and prompt for undated source reports,
   using the existing assertion enum. Add positive/negative protocol controls before
   implementation: valid undated source report; contradictory scope/checks; negative
   source basis; fabricated present-world validity; false latest comparison.
3. Refactor temporal acceptance into per-unit applicability with explicit failure
   diagnostics. Keep its public disposition/current-state interface compatible and
   carry structured failures to the editor and public concise explanation. Preserve
   every existing current-status, heading, partial-answer and source-membership guard.
4. Test actual finalization and all four query modes with mixed source-relative and
   non-temporal units, undated fields, dated events, comparisons, true current claims
   and adversarial mixtures. Old captured none/none assessments for current-bearing
   units must remain insufficient; a controlled, explicitly source-relative audit
   demonstrates the corrected contract without pretending to be fresh model evidence.
5. Test saved-answer restoration and cache identity. Advance the finalization policy
   version because the accepted semantic contract changes; invalidate old candidate
   qualification through the existing immutable code/manifest identity.
6. Independent spec and implementation review, relevant regression suite and full
   offline/CI checks precede fresh native evaluation. Run a new complete development
   schedule, then all modes, live retrieval/built-browser qualification and sealed
   holdout. Do not resume v6, reuse its passing subset as qualification, loosen the
   missing-facts gate, or deploy an unqualified candidate.

Success means source-grounded requested meanings survive final delivery while every
unsupported current-world or comparison assertion remains rejected. Lower latency or
fewer model calls alone is not acceptance evidence.
