# Development admission with conservative duplicate rejection

Status: independently reviewed development-only admission decision.
This is a prospective, post-hoc relaxation of the internal diagnostic prerequisite;
both failed probes remain failed.
This changes no application behavior, prompt, source, question or gold label.

## Evidence and distinction

Both frozen nine-call model probes made one false rejection of a valid duplicate.
Both rejected the genuinely incomplete targets and made zero false authoritative
exclusion approvals, wrong-target decisions or transport failures. The failed
results stay failed. There is no measured improvement from changing the model on
this small probe, so retain the existing Gemini route for further development.

The current runtime response to a rejected omission is to preserve independently
verified selected facts and mark conservation partial. It grants no source support,
deletes no selected fact, and adds no retry. The rejected duplicate's requested
meaning is already present in its selected target. A conservative partial label is
an accuracy/usability limitation, but not evidence of missing or unsupported answer
content in this control. The existing whole-query rubric already reports conservative
coverage under-reporting separately from required-meaning loss and false completeness.

The proposed decision is to permit whole-query development measurement under that
existing distinction, rather than requiring zero false rejections in every internal
classification before measuring delivered answers. This is not permission to mark
the diagnostic passed, repair gold, promote a passing subset, or deploy the candidate.

## Narrow admission conditions

Require two independent reviews of this decision and a separate immutable admission
receipt bound to the exact policy, primary probe manifest/result/grades and concrete
input/gold hashes. Both model probes and their complete captures remain available.

The exception is eligible only when:

- All scheduled probe calls completed normally with valid immutable capture bindings.
- False authoritative approvals, wrong targets and transport failures are all zero.
- Every observed semantic error is rejection of a full duplicate whose complete
  requested meaning is independently established in one actual selected target.
- The unchanged application fails closed for that rejection: no verified fact is
  removed or invented, and completeness stays false. Original support remains
  independently audited. Offline final-target survival and delivery checks pass.
- The application files and dependency lock exactly match the failed probe's frozen
  candidate. No prompt or code change is smuggled into the exception.

Unique requested meaning that is omitted is ineligible. Unsupported selected claims,
unavailable execution, a false accepted exclusion or a broken final/source binding
remain hard failures. Do not generalize this exception to those defects or to an
unreviewed model route. The admission receipt records the known false-rejection
count explicitly; absence of a count cannot be treated as zero.

## Unchanged whole-query and release gates

Run a fresh twelve-case Strict evaluation on the same v4 application and existing
Gemini route, then the fresh forty-eight-case all-mode run only if all initial cases
pass. Keep the unchanged original-source questions and meaning-based rubric. Zero
raw factual-audit false approvals, delivered unsupported assertions, missing required
meanings, wrong temporal projections and false-complete answers remain mandatory.
Every false authoritative accepted exclusion remains a hard failure even if a later
guard catches it. A rejected omission is not itself a false accepted exclusion;
its effect on delivered meaning and coverage must be graded explicitly.

Every fresh independent whole-query grade must record the conservative duplicate
false-rejection count (including zero) and the exact surviving selected target for
each such rejection. A target that does not survive final delivery cannot justify
this exception.

Conservative coverage under-reporting remains visible in all result summaries and
the UI. It cannot make a missing answer acceptable. Require valid complete or partial
restored coverage with complete planning; unavailable coverage does not qualify.
No automatic retries or continuation past a failed whole-query case.

This decision supersedes only the zero-false-rejection diagnostic prerequisite for
development admission in the v4 authority/comparison specs. It does not establish a
passing diagnostic or model superiority. Live-corpus qualification, the independent
holdout, actual visual UI checks, exact-candidate CI and reviewed GitOps activation
are still required. Any remaining conservative-label limitation must be disclosed
when reviewing a release; this decision alone never grants release approval.

The parent release criterion requiring demonstrated improvement on a failing
baseline remains outstanding. Development admission is not evidence that this
criterion has been met.
