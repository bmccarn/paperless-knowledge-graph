# Large-evidence query execution

Status: both independent reviews cleared slices A and C; local behavior and actual
built-browser regressions pass. Slice B is under design. No release is approved.

## Problem and evidence

The v27 candidate passed all twelve Strict and forty-eight all-mode fixed-original
development cases. Its first real-corpus browser case failed. Thirty-five source
reader calls produced 155 observations, but the finalizer dispatched no audit calls
and delivered its generic fallback with unavailable coverage. The captured
candidate reconstructs exactly from the retained inventory: 38,576 characters and
209 eligible evidence windows. The browser completed desktop/mobile source and
conversation-restoration checks; displaying the fallback is not answer acceptance.

A deterministic finalizer control verifies eighty observations in twenty batches.
Adding one observation results in zero audit calls. Changing only the configured
unit ceiling admits all eighty-one; the same control admits all 155 when the
ceiling is raised to 155. This establishes the hard unit ceiling as the cause of
the pre-audit rejection. Missing source windows or oversized surrounding prose do
not explain this captured structured candidate.

Independent original review also found required source opportunities absent from
the reader inputs. Capacity and retrieval coverage are separate defects. Removing
the ceiling cannot certify recall, relevance, factual accuracy or completeness.
The failed live attempt remains failed, with all other scheduled cases unrun.

## Intended behavior

A valid answer candidate must not become unauditable solely because it contains
more than a fixed total number of observations. Audit the entire candidate in
bounded batches with bounded concurrency and owned cancellation. These execution
controls limit simultaneous work; they do not silently discard facts or certify a
prefix. This applies to initial, repaired and subset candidates, all query modes,
and all domains, including legacy prose callers.

Keep exact source membership, negative verdicts, semantic correction limits,
atomic observation validation, quantity/date checks and final receipt binding.
Neither larger inputs nor a successful transport grants factual authority. Actual
provider context exhaustion, timeout or cancellation remains a visible execution
failure. Do not impose an invented output-token cap or truncate source evidence.

## Implementation slices and gates

### A. Complete audit scheduling

Remove the total `max_units` admission ceiling and its coupling to prose context
length. Retain four-unit audit batches and the existing worker concurrency. Derive
the audit deadline from the actual batch count and worker waves, rather than a
clamped count. The enclosing request/evaluation deadline continues to own the
whole invocation and can cancel it; per-batch correction does not restart it.

Remove the obsolete repair `audit_unit_limit` diagnostic and constructor option.
Do not replace eighty with a larger constant or truncate a repaired candidate.
Remove the related fixed 96,000-character repair-input gate: a completed audit's
normal single repair opportunity must not silently disappear because its valid
candidate is long. The existing repair deadline and provider failure handling own
that call, and its result still needs a complete independent audit.
Preserve whole prose context for legacy attribution; structured observations still
supply their independently meaningful text. Existing per-observation formatting
and size validation is a separate contract and remains unchanged.

Require every scheduled unit to settle before an audit is complete. A failed or
cancelled later batch cannot attach a passing ledger to the full candidate. Keep
complete candidate identity, per-batch diagnostics and all native attempts. Update
the policy identity so prior cached or restored answers cannot inherit new checks.

Before implementation, write failing regressions at the real finalizer and
question-pipeline interfaces. Cover 81 and 155 valid observations, initial and
editor-created candidates, structured and prose input, all four modes, a negative
observation beyond the former ceiling, a late batch failure, outer cancellation,
concurrency bounds and deadline scaling. Controlled auditors prove scheduling and
conservation only; they do not establish model accuracy.

### B. Retrieval coverage diagnosis and design

Trace independently identified missing meanings through discovery, ranking,
reservation, original-window construction and source reading. Distinguish a
document never retrieved from one retrieved then dropped or read without retaining
its required meaning. Use original-based labels, not generated answers, to define
the loss. Preserve measured candidate/reservation truncation and index freshness.

Before changing retrieval, add a concrete design and regression cases here based
on those traces. The design must preserve opportunities across requested subjects
and periods without domain-specific document IDs, insurance keywords, hidden
rubric inputs, or claims of exhaustive archive coverage. It must work for focused,
comparative, contextual and historical requests. Do not solve recall by certifying
model-proposed exclusions or allowing incomplete coverage to count as complete.

Slice A may be implemented and tested independently. Do not start another full
native qualification or deploy it while slice B's known required-source omissions
remain unresolved. This prevents repeating a costly qualification for a candidate
already known to miss the question's scope.

### C. Public metadata and follow-up provenance

The live UI also displayed an incorrect latest-source date and suggested questions
that presupposed completed cancellation and currently active coverage. These came
from indexed date hints and the unaudited retrieval draft even though the final
answer was withheld. Factual safety applies to the whole response, not only the
answer body.

For the question pipeline, do not publish a latest/supporting source date computed
from lexical relevance, a synthetic index header or a mixed-format string maximum.
No such scalar is established by the current claim receipt. Leave those public
authority fields unavailable until a role- and source-bound date contract exists.
Keep private retrieval date diagnostics distinct from answer support; do not
silently reinterpret policy effective/expiration dates as document issue dates.
The same restriction applies to per-source card `date` and derived `date_signals`,
not only summary fields. Omit those unverified date hints from question-pipeline
source cards. Preserve original quotations, source titles and verified Timeline
projections; none is rewritten to fit a guessed issue date.
Always rebuild question-pipeline cards from final validated cited references,
including an empty list when none survive. The present fresh-response fallback to
raw indexed excerpts embeds the same synthetic date header inside the excerpt;
clearing a date badge alone cannot fix it. This matches restored failure handling
and does not change legacy unaudited Quick source behavior.

Do not copy suggested questions from the unaudited retrieval draft. Use neutral,
domain-independent source-review questions that assert no event, active status,
payment, cancellation or other source fact. The actual query and contextual
follow-up execution remain unchanged. This does not add another model call or
change the audit's verdicts. Apply the same rule to failure and successful delivery,
HTTP/SSE, cached answers and restored conversations; bump the relevant identity
where necessary so stale suggestions cannot acquire current acceptance.
Saved conversation follow-ups occupy a separate database column. Sanitize that
read path as well as metadata's failure early-return, including older failed
pipeline messages; a new cache identity alone cannot correct saved presentation.

Before implementation, independently review this scope and add response/browser
regressions with deliberately incorrect index dates and factual presuppositions in
an otherwise plausible draft. The final answer, source panel and follow-up UI must
not imply that those unaudited values were verified.

### D. Requalification and release

Independently review both implementation axes and run required checks after both
defects have regression coverage. Freeze the complete candidate, then run fresh
Strict, all-mode and real-retrieval/browser qualification. Keep v27's successful
development measurements and failed live attempt as historical evidence; never
rescore the failed run or splice passing cases into a new run.

The held-out protocol, matched baseline/resource comparison, final real-history
and current-record browser acceptance, exact-candidate CI and immutable GitOps
release remain required. No production reindex or ingestion rebuild is needed to
change answer audit scheduling. Do not claim deployment or issue closure before
those gates pass.
