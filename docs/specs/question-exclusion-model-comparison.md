# Matched model comparison for exclusion authority

Status: proposed execution contract; no comparison calls have run.

The v4 Gemini 3.8 Flash classification probe completed all nine calls. It classified
eight controls correctly but rejected the retained balance duplicate instead of
linking to the valid March target. Preserve its failed
result and both independent grades. Do not alter the prompt, original text, gold,
targets or application logic in response to that result.

This is a new nine-call comparison, separate from the untriggered fifteen-call
comparison in `question-fact-model-comparison.md`. That older arm remains unrun.

## Admission and treatment

Admit this arm only after both independent reviewers have finalized the primary
nine-call FAIL and the complete execution/capture hashes are verified. Verify that
`gpt-5.5` is advertised by the same configured model endpoint using a read-only
model-list request. A missing model blocks this arm; do not substitute an alias.

Copy the exact original frozen package bytes, inputs and their independent review
receipts to a new exclusive directory. Use the same production prompt and parser,
source/observation identities, valid-target gold sets, order, locked dependencies,
SDK/client retry settings, cache-bypass policy, endpoint, deadlines and concurrency.
The only runtime treatment is `STRANDS_MODEL=gpt-5.5`. Require equality of the two
manifests after substituting only the runtime model field. Bind the primary result,
grades and manifest into a separate comparison-admission receipt. Do not modify
the copied package to insert this new spec or silently repair model parameters.

Budget: exactly nine scheduled calls, once each, at most 900 active seconds. Capture
every input/output, usage and termination using the existing reviewed diagnostic.
No selector, retrieval, new conversation, fallback, repair call, rerun, additional
model arm, serving configuration change or deployment. Upstream attempts remain
unknown. The model-list preflight is not an inference call.

## Review and consequence

Both reviewers independently grade all nine outputs against the exact predeclared
gold and originals, including alternative valid single targets. Count false
authoritative exclusions, false rejections, wrong targets and transport failures
separately; any nonzero count fails the arm. Raw failures cannot disappear behind
a valid parser result. Preserve the prior model result regardless of outcome.

A passing comparison establishes only performance on these nine development
controls. It permits fresh twelve-case Strict and forty-eight-case all-mode runs
using that same candidate code and explicitly frozen model route. It does not
establish general model superiority or qualify production. The later live-corpus,
sealed-holdout, browser, exact-image CI and GitOps gates remain mandatory. Before
live testing, freeze the model choice consistently in the six request inputs and
their manifest; do not silently compare different requested model routes.

If this arm fails, stop and report the remaining concrete classification failures.
Do not initiate another prompt revision or further model comparison within this
contract. A new design or acceptance proposal requires explicit independent review.
