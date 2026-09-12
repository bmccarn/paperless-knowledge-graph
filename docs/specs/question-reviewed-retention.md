# Retain observations when independent review rejects their omission

Status: implemented and independently reviewed; fresh native qualification pending.
This is a general answer-construction correction across domains and query modes.
It does not change the question, source text, reader/selector/exclusion prompts,
model route, factual-audit rules or release criteria.

## Reproduced failure

Fresh v4 Strict hours-history case01 reads every required meaning, then proposes
omitting the observations carrying conditional overtime, completed training and
absence of recorded attendance. Both exclusion reviews correctly reject those
omissions. Candidate construction nevertheless uses only selector-delivered rows.
The narrowed planner/coverage stage says answered and does not invoke completion.
Final conservation truthfully reports partial coverage, but the answer still loses
one required aspect. The v4 run fails; case00 PASS is not reused as qualification.

The missing operation is deterministic retention between exclusion review and
factual audit. An independent rejection must affect the proposed answer, rather
than merely report lost information after delivery. It does not certify that a
reader observation is true.

## Behavior and authority

Preserve the immutable original reader inventory, original selector dispositions
and every independent exclusion judgment. Derive a retained observation set:

- Every selector-delivered observation is retained as before.
- Every proposed omission with a valid independent `reject` decision is also
  retained, using its exact reader text and original source identity.
- Approved outside-request and explicit covered-by omissions remain omitted.
- Unavailable, malformed or timed-out exclusion reviews provide no retention or
  exclusion authority; their conservation remains unavailable/partial as before.

Keep selected presentation order, then append recovered observations in original
inventory order. Never rewrite, combine or silently deduplicate reader assertions.
There is no additional model stage, audit pass, retry, fallback model, output limit
or completion loop. Retaining more units can increase the existing audit batch
count; compute its existing call bound from the actual retained candidate. Preserve
existing audit capacity/deadline guards and fail closed rather than truncate facts.
The existing original-source factual audit sees the entire retained candidate.
Unsupported recovered assertions cannot bypass that audit, editor, subset checks,
combined completion audit or final delivery validation.

A valid reject means the omission is not justified; it does not necessarily mean
that the observation is unique or true. A conservative rejection can retain an
additional duplicate. Report that consequence honestly; do not turn it into proof
of relevance or factual support. False authoritative accepted exclusions remain
hard failures even if a later stage happens to recover the missing meaning.

Allow an empty selected-ID set for outside-request/reject classifications so the
same repair works when the selector proposes omitting every observation. Covered-by
still requires a nonempty actual selected target. Do not let newly retained rows
retroactively become targets of earlier duplicate decisions or create chains.
If no observation is retained after all reviews, return an explicit unverified
no-factual-answer outcome. Do not send an empty candidate as a successful answer
or claim complete coverage; unavailable reviews remain unavailable.

## Final conservation and compatibility

Original dispositions remain selector proposals; final mappings establish what
actually survived delivery. Derive required retained mappings from the same pure
retention rule used to construct the candidate. A recovered observation counts as
preserved only if its exact whole unit and compatible original source survive the
final audited answer. Removal, rewriting, source substitution or loss during
editing/completion keeps conservation incomplete. Approved duplicate omissions
still depend on their original explicit selected target surviving, without retargeting.

Keep support and coverage independent. Factual-audit failure/timeout retains its
original disposition and unavailable conservation; recovered text is never pasted
into an already finalized answer. Bump the pipeline identity to question-evidence-v5
and conservation receipt to version3. Old receipts/caches cannot certify this new
behavior. Saved-answer restoration recomputes the same retained mappings and
rejects tampered selection, reviews, source bindings, target survival or final text.
The feature remains disabled by default and serving configuration is unchanged.

## Implementation and verification plan

1. Add one pure retained-ID derivation shared by candidate construction and final
   conservation. Preserve existing raw selector/review ledgers for auditability.
2. Update the exclusion parser's empty-selected handling without relaxing exact
   omission identity, schema, target ownership or covered-by constraints.
3. Update restoration/version identity and affected synthetic fixtures. Add a
   production-shaped regression from the failed stage pattern: narrow planning,
   correct source reading, omitted requested facts, valid rejection, and an
   otherwise falsely satisfied coverage assessment. All rejected meanings must
   reach the existing factual audit and survive final delivery to qualify.
4. Add cross-domain controls for all-omitted recovery, accepted irrelevance and
   duplicate decisions, uncertain/unavailable review, source-unsupported recovered
   assertions, two subjects with identical text, removed/rewritten recovered units,
   removed duplicate targets, cancellation and saved-receipt tampering. Verify
   no extra model stage/retry and that raw false approvals cannot be hidden.
5. Review implementation independently and run the appropriate offline suite/CI.
   Then freeze a fresh v5 twelve-case Strict run followed by forty-eight all-mode
   cases only if the initial run passes. Original questions and meaning-based gold
   remain unchanged. No v4 failed run or earlier passing subset resumes.
6. Continue actual-corpus, sealed-holdout and visual browser qualification only
   after all-mode admission. Demonstrated improvement on the failing baseline and
   exact-candidate release/GitOps gates remain mandatory.

V5 starts under this reviewed behavior contract, not the v4 conservative-admission
exception: its application differs, so the v4 exception receipt cannot admit it.
Fresh v5 manifests must retain the strengthened initial/all-mode continuation
checks (individual grade/raw hashes, zero-error aggregate agreement, explicit
coverage/duplicate counts and scheduled identity) even without the v4 exception.
Bind this contract and the shared grade validator in the new manifest.
The unchanged nine-call classifier failures stay recorded as failures; deterministic
retention is evaluated at the delivered-answer boundary. Any missing requested
meaning, unsupported assertion, false factual/exclusion approval, wrong temporal
projection, false completeness or unavailable execution still fails qualification.
