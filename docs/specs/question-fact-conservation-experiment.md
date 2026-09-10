# Retained-fact selection and exclusion diagnostic

Status: concrete protocol proposed for independent review before native execution.
Design: [fact conservation](question-fact-conservation.md).

Use seven retained reader inventories matching the previous failed/control cases:
missing-hours, missing-charge, complete-hours, complete-credit, calibration,
capacity-form and constructed latest-only capacity. Original question, date, sources
and exact reader observation text/references enter the selector. The constructed
latest-only selection also includes the retained delivery/receipt document from
the same capacity-history case as a clearly unrequested control; its observation
must be excluded from a question about authorized capacity. The separate latest-only
exclusion challenge keeps the prior two-document setup unchanged. It receives neither
a generated answer, a narrowed plan nor any gold labels. Assign stable content-bound
IDs; preserve source span handles. Reader interpretations remain untrusted.

The selector returns exactly:
`{"dispositions":[{"observation_id":"id","status":"delivered","target_id":null}]}`.
Statuses are delivered, outside_request, duplicate_of. Every inventory ID must occur
once. Only duplicate_of has a target, and its target must be another delivered ID;
chains, cycles, foreign IDs, duplicates and missing dispositions fail. A duplicate
must preserve full requested meaning, not merely a topic or shared value.

Then run eight preconstructed exclusion challenges independently of selector output:
false exclusion of the requested permission; false exclusion of the original charge;
valid exclusion of an older value in a latest-only question; incomplete duplicates
losing condition, subject, or date role; two valid full-meaning duplicates. One
explicitly labelled second-subject original may be constructed for an unambiguous
subject challenge; never mutate the retained original or claim the new record is
native evidence. Freeze every construction and its gold before running.

Each challenge supplies an exhaustive fixed disposition proposal with exactly one
excluded observation. The reviewer returns exactly
`{"decisions":[{"observation_id":"excluded-id","decision":"accept"}]}`,
with decision accept or reject, once per excluded ID and no other IDs. Proposal
reasons and retained observations cannot establish truth or relevance. Compare
original request and original text independently. Semantic uncertainty rejects an
exclusion; unavailable or malformed output is a failed call, not a semantic verdict.

Both protocols require strict JSON without duplicate keys, extra fields, prefix,
suffix or code fences. Reject null, empty, nonterminal, truncated, refused and failed
responses. No protocol correction or fallback. Continue planned controls after an
individual failure within the total budget; preserve all outcomes. Do not rerun or
resume the frozen batch.

Freeze private inputs, per-observation semantic gold, allowed optional context,
challenge verdicts, origins and original/result/reader hashes. Two independent
reviewers approve exact input bytes. Both review harness/offline regressions before
execution. Freeze code and locked runtime in a manifest before the first call.
Production sources, ingestion state and application query code remain unchanged.

The budget is exactly fifteen scheduled model calls, at most 1,500 active seconds,
using existing Gemini 3.8 Flash route with locked Strands 1.55.0, SDK retries zero,
Strands retries disabled, proxy cache bypass and no output-token cap. Upstream
attempts remain unknown. Capture exact prompts, raw model outputs, provider/native
termination, usage and hash bindings. The output directory is exclusive.

Independent graders inspect originals first. Report separate counts for selection
losses, unnecessary retained material, false exclusion approvals and false exclusion
rejections. Required meanings must survive; optional context is not mandatory.
Any raw selection loss fails even if a hypothetical later reviewer could repair it.
Any semantic or transport failure rejects the whole diagnostic. Exhaustive IDs do
not certify facts. Matched hours/credit cases must preserve equivalent requested
meaning despite different reader grouping. Latest-only scope remains distinct.

A pass qualifies only this diagnostic. An implementation spec and fresh full-query
qualification still follow. If it fails, preserve the failed result and evaluate
model suitability on the same reviewed task before introducing additional repair
stages. Never reinterpret a failing gold label to obtain a pass.
