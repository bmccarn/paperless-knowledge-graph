# Conditional model comparison for fact selection

Status: proposed conditional diagnostic, not a production model change.
Prerequisite: the frozen Gemini 3.8 Flash fact-conservation diagnostic has failed
semantic acceptance. Do not run this arm simply to choose between lucky repeats.

The existing application /models endpoint was read on September 9, 2026 and lists
gpt-5.5. A listed route is not evidence of higher accuracy or working inference.
Use it as the single alternative candidate for the identical reviewed task. This
comparison measures route behavior under these settings; it does not establish a
general model ranking.

Reuse the exact fifteen inputs, gold, prompts, protocol and harness code from the
failed first arm. Change only the isolated process STRANDS_MODEL to gpt-5.5. Do not
change the production route, application settings, proxy configuration, temperature,
reasoning controls, deadlines, retry policy or output-token policy. Provider defaults
remain provider defaults; do not claim they represent maximum reasoning effort.

Use a new exclusive local/remote temporary directory and a new immutable manifest
that records the alternative runtime. Bind the first arm's manifest, raw result and
both failure grades as the admission evidence. Both reviewers must clear this plan
and confirm unchanged input/code identities before execution. Fifteen calls once,
maximum 1,500 active seconds, 90-second stage timeout, zero SDK retries, no Strands
retry, proxy cache bypass, no output-token cap. Keep all outcomes, including route
failure, refusal or timeout. Never replace a failed call with another model.

Independently grade the entire alternative arm using the original frozen labels.
Report selection losses, unnecessary retained material, false exclusion approvals
and false exclusion rejections separately. Any failed call or semantic case rejects
this arm; no passing subset qualifies. Capture native/provider termination, exact
inputs, outputs and usage. Upstream attempts remain unknown.

If this arm passes, the evidence supports further integration design and broader
qualification using that route. It does not authorize assuming factual-audit quality,
activating the pipeline or changing the extraction model. If it fails, preserve the
failure and identify which capability is deficient before further application work.
