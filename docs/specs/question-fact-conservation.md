# Preserve source observations through answer construction

Status: design proposal after failed source-coverage diagnostic; not implemented
or qualified. This replaces neither factual audit nor source retrieval.

## Evidence and design question

The v2 source reader correctly extracted all three observations in the omitted
latest-record document. The composer received those observations but dropped them.
An additional document-local source comparison then returned no gaps despite seeing
the full original. That diagnostic failed its frozen acceptance criteria. Adding
another reader or another generic complete/partial vote is not an evidenced fix.

The next question is whether making every source observation accountable prevents
silent loss without forcing irrelevant or unsupported material into the answer.
A closed list of IDs can prevent mechanical disappearance, but cannot establish
that a model's relevance or duplicate judgment is correct. Test both separately.
The scope is all domains and modes, not a special latest-record or insurance path.

## Proposed module

A request-local observation inventory preserves the source reader's exact text,
source references and stable application-assigned IDs. It is an immutable record of
untrusted interpretations, never a source of truth. Original evidence remains the
only support for facts. Free-form limitations are not automatically factual units;
any limitation asserted in the answer needs the same source audit as other facts.

Composition returns ordering/grouping of inventory IDs and explicit dispositions
for every remaining ID. It does not regenerate those observations as new prose.
Proposed dispositions are delivered, duplicate_of, or outside_request. Missing IDs,
foreign IDs, duplicate dispositions, cycles and duplicate links to undelivered
observations are invalid. Duplicate means the full requested meaning survives in
the retained observation, including subject, conditions, quantities and date role.
The disposition structure is immutable and bound to inventory and original request.

An independent exclusion review receives the original question, original passages,
exact inventory texts and proposed exclusions. A planner rewrite and shortened
answer cannot authorize irrelevance. False irrelevance and incomplete duplicate
claims fail acceptance. A validated reference or exhaustive ID list is not semantic
proof that an exclusion is appropriate. Unavailable exclusion review cannot certify
complete coverage. This remains an unproved model judgment; it needs measured tests.

Final factual audit operates on the exact rendered candidate and original sources,
including cross-document alternatives. Unsupported inventory facts cannot enter
merely because the reader emitted them. No synthesized relationship, calculation or
current-world claim follows automatically from two individually supported facts.
Where such an answer needs new wording, register the proposed new observation with
its input IDs and audit the complete new assertion. Existing input meanings must
still have explicit dispositions; a new summary cannot silently erase them.

The final coverage receipt binds the delivered candidate, source snapshot,
observation inventory, all dispositions and their semantic assessments. A required
fact rejected for factual support remains an unresolved requested aspect, not an
irrelevant fact. Candidate changes invalidate affected factual/exclusion/coverage
receipts. Persisted history and caches validate the same bindings. Cancellation and
unavailable stages preserve explicit uncertainty and cannot become empty-success.

## Bounded next experiment, to review before implementation

Use retained candidate-blind reading artifacts from the same failed and control
cases; do not call the reader again or change original questions/gold. Freeze exact
inventory inputs and distinguish missing reader facts from lost composer facts.

Compare proposed ID dispositions with frozen original-source semantic judgments:

- Both known omissions must survive as requested meaning.
- Complete-answer and latest-only controls must not acquire irrelevant demands.
- Requested fact versus unselected alternative and conditional permission versus
  completed action must remain distinct.
- Include paired false-irrelevance and incomplete-duplicate challenges, plus valid
  exclusions and full-meaning duplicates as controls. The same requested source fact
  must remain relevant across matched incomplete and complete answers. Duplicate
  targets that lose subject, condition or date role must be rejected. Score initial
  selection and exclusion review separately; complete ID accounting cannot itself
  count as semantic success. Otherwise this only moves the current bug.
- Offline controls prove that dropping an ID, referencing a foreign ID, changing
  the candidate or losing a terminal response cannot yield complete coverage.

Before native execution, freeze the concrete inventories, expected dispositions,
call schedule, runtime/model identity, token-cap policy, budget and two independent
review receipts. No budget or native run is established by this proposal. Preserve
all attempts; do not select a passing subset or rerun a failed manifest.

If semantic exclusions fail, reject this model/protocol combination. Evaluate a
separately frozen stronger reasoning model on the same task before adding more
repair layers. Model availability and SDK compatibility need live verification;
no alternative model is selected or claimed superior here.

A passing focused experiment would justify a concrete integration spec, including
compatibility with existing completion, partial-answer salvage and restoration.
It would not qualify live retrieval or deployment. A changed application requires
fresh initial and all-mode evaluation, independent holdout, actual UI delivery
and GitOps release checks. Previously failed candidates remain failed.
