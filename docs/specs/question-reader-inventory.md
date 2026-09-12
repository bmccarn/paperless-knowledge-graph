# Preserve the complete question-led reader inventory

Status: independently reviewed; implementation and qualification in progress.

## Evidence and underlying problem

The original-source reader captured required facts in the failed v4 hours case and
v5 capacity case. Later semantic filters lost them. V5 repaired correctly rejected
omissions, but its first fresh case exposed a false authoritative exclusion: granted
approval was called outside-request, leaving only a signed selection in the answer.
Final coverage then incorrectly claimed completeness. Selection/signature is not
approval. Both whole-query runs and both earlier classifier probes remain failed.

The pipeline already has a question-led, candidate-blind original-source reader.
A second model's judgment about relevance cannot reliably authorize destruction of
that reader's observations. Remove that authority from runtime answer construction.
This applies to every domain, query mode and requested fact role, not a keyword or
status-specific rule.

## Runtime behavior

Build the proposed answer deterministically from every validated reader observation,
in original document/observation order, with exact text and source ownership.
Do not run fact selection or exclusion classification in the question pipeline.
Do not deduplicate repeated observations, collapse subjects, reclassify unrequested
facts, or introduce a replacement relevance/rewriting pass. Historical diagnostic
wire contracts may remain available for artifact interpretation, but they have no
runtime authority and their failed outcomes are not rescored.

Align the existing reader output contract with its use as independent answer units:
each observation names its original-source subject or record and relevant scope,
without relying on sibling observations to supply missing identity or time roles.
Make this broad requirement explicit in the existing reader prompt/schema; do not
add a new reading, rewriting or repair stage. Any source-established material
negative fact, condition or uncertainty belongs in a referenced observation, not
only unreferenced limitations metadata. Limitations remain untrusted processing
notes and are never automatically promoted into factual assertions. Missing records
do not prove absence of an event.

The source auditor continues judging meaning and association, not a syntax blacklist.
A referent uniquely resolved by its original source is not false merely because its
wording is short. Genuinely ambiguous or wrong-subject/action associations cannot
borrow support from an unverified sibling. An existing editor may produce a supported
standalone repair, but rewriting still leaves the original exact inventory mapping
unresolved. Freeze the reader prompt/schema change explicitly with this candidate.

Reader observations remain untrusted interpretations. The entire proposed inventory
passes through the existing original-source factual audit. Unsupported assertions
may be removed by the existing editor/subset path and its fresh audit; the existing
append-only completion and combined audit remain available. No recovered or original
reader assertion is pasted into an already verified answer. Supported incidental
facts and duplicate occurrences can increase verbosity, but every additional claim
must satisfy the same original-source accuracy rules. They are not blanket evidence
of relevance, archive completeness or real-world current status.
This deliberately supersedes the earlier composition presentation restriction
against unrelated supported facts in `question-evidence-answer-pipeline.md`, solely
for the question-led reader inventory. Incidental facts earn no credit for a missing
requested meaning; unsupported-extra and original-question coverage gates remain.

Preserve existing atomic observation, audit-capacity, concurrency and deadline
boundaries. Empty, malformed or oversized inventories produce an explicit unverified
failure; never truncate to a passing prefix, silently skip observations, or invoke
new calls to work around capacity. More facts can increase existing audit batches.
Compute bounds from the full inventory, and report actual calls/latency rather than
claiming a speed improvement from removing two kinds of stage.

## Conservation and saved-answer contract

Use conservation receipt version4 and pipeline identity question-evidence-v6.
The receipt must bind the immutable full reader inventory to the final answer and
original source manifest. Require one distinct, whole, source-compatible surviving
final occurrence for every inventory item. Repeated text across two sources cannot
share a survivor. Every removed, rewritten or source-substituted item remains
unresolved; unavailable factual execution remains unavailable.

No legacy selector disposition, outside-request classification, covered-by target,
review ledger or editor decision can discharge an inventory item. Remove those
fields from the new runtime receipt, or reject them as incompatible with its exact
schema. Saved restoration recomputes full-inventory mappings and rejects old receipt
versions, tampering or any excluded-item completeness shortcut. Preserve verified
partial answers where the existing factual audit permits them, while leaving their
inventory conservation incomplete. Failure mode, trust and Timeline projection must
remain internally consistent and unverified when no factual answer exists.

Preserving all reader observations is not proof that the reader captured every
requested source fact. Original-question coverage and independent original-based
qualification still detect reading/retrieval omissions. Do not redefine complete
coverage as merely exhausting this inventory.

## Implementation and qualification plan

1. Replace the runtime selection interface with immutable full-inventory composition.
   Share one inventory-to-candidate/mapping rule and remove selector/exclusion calls
   from the actual pipeline. Keep diagnostic-only code outside that call path.
2. Simplify receipt generation/restoration to exhaustive inventory mappings and bump
   identities. Update saved-answer/public delivery fixtures and count excluded
   inventory items as zero, retaining compatible UI summary fields where needed.
3. Reproduce both observed patterns through all four modes: reader facts that a
   selector would omit and approval facts that a relevance classifier would wrongly
   exclude. Neither model stage may be called; all facts must reach source audit.
   Demonstrate failure against the frozen prior implementation and success now.
4. Test unsupported reader facts, source substitution, two identical occurrences,
   uniquely source-resolved versus genuinely ambiguous references, no sibling
   inheritance, material negative facts versus limitations-only notes,
   editor/subset removal, completion rewriting, saved receipt tampering, empty and
   over-capacity inventories, timeout/cancellation and actual multi-batch audit
   volume. No approval or incomplete execution may be hidden by formatting or a
   later guard. Retain existing conversation/request/source isolation tests.
5. Keep grading version2 raw/dual-grade/scheduled-identity requirements. Bind this
   contract and updated validators in fresh manifests. V6 has no runtime exclusion
   classification: grades explicitly record zero classification calls/rejections,
   and the harness rejects any captured fact-selector/exclusion attempt. It still
   grades every source-audit approval, delivered assertion and required meaning.
6. Independently review and run the appropriate offline suite/CI, then freeze fresh
   twelve-case Strict and forty-eight-case all-mode runs. Original questions/gold
   remain unchanged. Stop at a failed case and preserve every attempt. No prior
   passing subset or exception admits this changed application.
7. Retain actual-corpus, sealed-holdout, real browser inspection, demonstrated
   improvement on a failing baseline, exact-candidate CI and GitOps release gates.
   Serving configuration remains disabled until qualified activation.

The selector/exclusion diagnostics are retired as runtime prerequisites because the
stages are removed, not because their classification failures became acceptable.
Zero missing required meaning, unsupported assertions, false factual approvals,
wrong temporal projections and false completeness remain mandatory. Unavailable
coverage/coarse planning still cannot qualify. This change makes no general accuracy
or performance claim before the fresh original-based evaluations complete.
