# Source record interpretation

Status: first model-free inventory/binding slice implemented, independently reviewed
and locally validated. The reader adapter/projection and reader-only SDK preflight
and full comparison runner are implemented and independently reviewed. The exact
comparison is admitted but its native execution requires the specific provider
transfer approval described in the linked diagnostic. This
spec is not a passing semantic result or production activation decision.

## Evidence and choice

The completed recovery diagnostic preserved 36/44 required meanings in both arms;
all twelve delivered answers were identical. Eleven of twelve reviews added
nothing, including both first-window replacement qualifications. Nine raw false
approvals remained. A new arithmetic error was introduced by recovery. Exact
source transfer and another summary review did not solve interpretation.

Three independent interface designs were considered: an exhaustive source record
compiler, an extensible relationship algebra, and a passage dossier with prepare
and verify operations. Choose source records with explicit anchored relationships
as the smallest diagnostic candidate. Defer a general expression algebra and a
new production dossier interface until native results justify them. This is a
falsifiable attention/accounting hypothesis, not a claim that more structure makes
a model accurate.

## Intended flow

1. Inventory every original as exact contiguous structural blocks, preserving
   headings, paragraphs, lists, tables and unknown source syntax. Blocks are work
   units; citation windows and Paperless documents retain their own identities.
2. Interpret each scheduled block with original context, without prior summaries
   or a proposed answer. Every block must have a returned record, including blocks
   whose interpretation is unresolved. Models cannot delete source blocks.
3. Propose relationships with separate original anchors for subject, assertion,
   qualifiers, date/action roles and comparison participants. Printed quantities
   stay separate unless a source-supported relation authorizes their combination.
4. Independently audit each complete assertion against original source context,
   without prior reading notes or earlier verdicts as evidence. Retain rejected
   assertions and unresolved dependencies. Reuse final source, scalar and temporal
   guards; do not infer correctness from record accounting.
5. Compose only after interpretation. Preserve question coverage gaps separately
   from supported facts. Whole-query integration and global counterevidence design
   remain gated on a fresh, independently graded comparison.

## First bounded implementation slice

Add an internal, model-free source record inventory module. Accept exact original
text, integer document identity and the caller's expected original digest. Reject
identity/hash drift before producing any work. Use Markdown physical block maps
only as structural hints, never as an allowlist of source content. Retain every
character exactly once in original order, including unparsed syntax and whitespace.
Never split or truncate a block to meet a hidden model/token cap. A very large
block remains measurable and can fail later feasibility admission.

The immutable inventory binds version, original identities, block ordinals,
character ranges and exact text. Its reading-binding operation requires one
record per block in the entire immutable inventory (no caller-selected subset), rejects duplicate/missing/foreign identities, and
accepts only two execution dispositions: interpreted or unresolved. A model may
return observations only for interpreted blocks; observations need nonempty
exact owned block references and text. Every observation must reference its own
record block; additional same-document blocks may supply qualifications. Interpreted
records require at least one observation and no unresolved reason. Unresolved blocks retain a nonempty reason and no
observations. Reject empty or whitespace-only originals before inventory construction.
Attach inter-block whitespace to a neighboring block, preserving every character;
leading whitespace belongs to the first block. All original blocks remain available in either case. Source blocks
referenced by an observation must belong to its document; future cross-document
relationships require a distinct verified interface.

The binding receipt distinguishes record accounting from semantic authority:
all-block accounting can complete while unresolved blocks remain; neither state
certifies support, question completeness or archive completeness. Return deep
copies so an adapter/caller cannot mutate the frozen inventory or a bound record.
Reject extra fields, duplicate JSON keys, non-finite data and malformed types.
Error messages must contain no original text. This module creates no network client,
performs no I/O, runs no models and changes no production query routing.

## Validation and next admission

Before implementation, independently review this spec. Test through inventory
construction and reading binding: exact Unicode/CRLF coverage, nested Markdown
and tables, empty/whitespace originals, source/hash mutation, malformed records,
foreign and duplicate references, mutation isolation and unresolved authority.
Verify the inventory over all six frozen full originals without model calls;
record block counts and largest blocks before deciding batch sizes or budgets.
Keep source text and private metadata out of repository fixtures and reports.

Only after this slice passes may a separately frozen model adapter/protocol add
source-indexed reading and independent verification. Preserve all six development
cases and both repetitions from the completed comparison; do not claim progress
by dropping the long original or the cross-domain controls. Freeze exact source
context per request, call/elapsed budgets, wire measurements, original gold and
all-output grading before native execution. Missing required meanings, unsupported
assertions, false raw approvals and wrong rejections remain failures. No semantic
retry or resuming a failed package. Later held-out, full-query, visual UI and GitOps
release gates are unchanged.


## First-slice measurement

The model-free inventory preserves all characters in the six frozen full originals:
35 and 323 blocks for the two long cases, and one each for the four cross-domain
controls (362 total). The largest block is 2,217 characters/UTF-8 bytes. This is
source block size, not a measured SDK request size. All blocks remain exact; none
was omitted to achieve these sizes. Private inventory measurements are stored at
/private/tmp/kg-source-record-inventory-measurement.json.

Six public inventory/binding tests pass. The full backend suite passes 990 tests
in 39.644 seconds with 58 expected opt-in skips. Model interpretation, relationships,
independent verification and native accuracy remain unimplemented/unmeasured in
this slice; the next admission must measure the full serialized requests and cost.

Both independent implementation reviews cleared the first slice. No new model calls
or production routing changes were made.

Reader adapter progress and exact remaining admission work are tracked in
[source-record native diagnostic](source-record-native-diagnostic.md).
