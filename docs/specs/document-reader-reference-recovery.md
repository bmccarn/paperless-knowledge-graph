# Bounded document-reader reference recovery experiment

Status: review before implementation; no production activation. Parent:
[document-local evidence](document-local-evidence.md).

The first retained document-local experiment completed one audit: it rejected the
negative and preserved three positives. Two other audits became unavailable because
otherwise completed reader responses referenced handles outside their one-document
input. No original-source guard was bypassed. These are three observations, not a
passing semantic result. The supplied pack contains 35 eligible grouped documents
from 36 captured originals; all 183 admitted windows are preserved.

## Change and invariants

Add constructor-only `document_local_corrected`. It uses exactly the same local
reader requests and verifier as `document_local`. Only after a nonempty reader
response fails the local schema/coverage/ownership parser, make one fresh reader
call for that same document. Supply the unchanged question, date and original
windows plus a content-free `reading_protocol_correction` marker asking for exact
request-owned document IDs and span IDs. Do not send the failed generated notes,
candidate, another document, verifier correction or prior answer. Correct structure
and re-read originals; the correction must not ask for a particular factual verdict.

No retry for absent/provider-failed responses. A second invalid response cancels
and drains sibling work as before. Every call uses the existing shared semaphore,
call timeout and audit deadline. There is at most one protocol correction per
reader execution; the existing native audit-level correction can repeat that
execution and is separately counted. No semantic retry or silent source selection.
Default `flat` and existing experiment strategies remain unchanged.

## Verification and frozen follow-up

Test one invalid-to-valid recovery, exactly two calls on repeated invalid output,
no retry for unavailable response, no leakage of failed notes, unchanged originals,
shared concurrency, cancellation and no changes to uncorrected strategy requests.
Retain reader attempts and later auditor raw approvals in private artifacts.

The follow-up will compare `source_first`, `document_local` and
`document_local_corrected` at one
reviewed code/runtime revision, including the separately reviewed auditor protocol
and raw-approval-accounting fixes in all three arms. Earlier model results cannot be
relabelled as this baseline. Keep the same authorized private four claims/originals,
183 eligible windows, model destination and synthetic development corpus, SDK
version, single-attempt SDK policy and proxy cache bypass. Three repetitions per
arm: retained ceiling 480 native invocations/1,800 seconds/estimated 2M tokens;
development ceiling 960 native invocations/1,800 seconds/estimated 1.5M tokens.
Estimates are not output caps or billing guarantees. Freeze and admit concrete
manifests after review. No source expansion or new private data is authorized by
this experiment. Sampling independence and proxy-internal attempts remain unknown.

All scheduled cases remain in denominators; count raw false auditor approvals
across every correction. The corrected candidate must have zero retained false
approvals, preserve all critical positives and complete all scheduled retained
cases before broader qualification. Reducing unavailable cases alone is not a
semantic win. A completed native verdict with a wrong inference fails the gate.
Retain preceding failures and separate operational receipts from isolated latency
benchmarks. If protocol recovery succeeds, continue representative/adversarial,
holdout, all-mode end-to-end and actual browser acceptance from the parent spec;
it does not itself authorize release. Architecture selection and holdout admission
also require the parent gate: an advantage over the matched broad-reader arm with
no critical false approvals or lost positives. A recovery-only improvement cannot
substitute for that semantic comparison.
