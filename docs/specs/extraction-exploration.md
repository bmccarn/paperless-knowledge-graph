# Complete, source-bound extraction

## Alternatives explored

1. Increase the existing 30,000/20,000 character limits. This keeps the interface small but still silently loses document tails and makes verification context limits worse. Rejected.
2. Extract all entities from one entire-document prompt, then use smaller verification prompts. This avoids arbitrary tails only while the document fits the model context; failure and cost are unbounded. Rejected.
3. Process bounded overlapping source windows, validate every result against its window, and reconcile accepted results deterministically. Selected: source coverage and source offsets are explicit, retries are bounded, and a long-document tail uses the same acceptance rules as its first page.

## Contract

`EntityExtractor.extract(title, content, doc_type)` returns the existing metadata, `all_entities`, `people`, `organizations`, and `implied_relationships` fields. It additionally returns `extraction_coverage`, `metadata_evidence`, `metadata_conflicts`, and `extraction_issues`.

Windows contain at most 12,000 Python string characters with 800 characters of overlap. At most 32 windows are processed per extraction, sequentially. Each required pass has at most three completion attempts, with HTTP SDK retries disabled, a 60-second request timeout and 6,000 output tokens per attempt. There are at most five passes per window (empty entity/relationship reviews skip unnecessary calls), so the default budget is at most 160 successful calls or 480 attempts. No live latency/cost is claimed; this favors coverage over cost and may be expensive for large archives. Exceeding the window budget, a malformed required pass, or an unavailable pass produces partial/failed coverage. Coverage means the required passes processed those source regions; it does not measure extraction recall or semantic accuracy. Ingestion must reject partial/failed results before replacing derived state or saving a processing hash.

Each window runs metadata extraction, entity proposals, independent entity review against the actual source text (including a single entity), then relationships constrained to accepted entities. Empty entity lists are legitimate; malformed lists are failures. A verifier adding entities cannot promote them: the accepted list is the intersection with the original candidates and independently validated source names. Failed verification never falls back to unverified entities.

Accepted entities require a named mention in an exact source quote. Source spans are `{start, end, quote}` with zero-based, half-open offsets into the original OCR string. Whitespace differences in a proposed quote may be normalized for locating it, but saved quotes always equal `content[start:end]`. Entity identity is the case-folded name plus type; conflicting types for the same spelling are unresolved and omitted. This does not merge same-name people across documents or establish real-world identity.

Relationships require two accepted endpoints, a valid relationship type, an exact source quote containing both endpoint names, and an explanation. Their `evidence`, `rationale`, and `inferred` fields survive combination. A separate source-aware review must explicitly accept the proposed relationship; mere co-occurrence is insufficient. Claims of explicitness from the proposer are not independently established and remain `inferred: true` unless the reviewer confirms explicit support. Missing or fabricated endpoints are discarded, with no heuristic entity creation. Cross-window relationships with no single supplied support span remain unresolved; overlap reduces this limitation but does not eliminate it.

Metadata carries field-path source quotes. Fields without valid source references are omitted and reported. Conflicting nonempty scalar values at the same field path become null and are recorded with alternatives and provenance. Lists are unioned deterministically; distinct rows are retained rather than guessed to be duplicate facts. Metadata list-row identity reconciliation and semantic validation remain separate evaluation concerns.

`extraction_coverage` contains `status: complete|partial|failed`, `total_characters`, `covered_characters`, and `windows: [{start, end, status, issues}]`. Coverage merges successfully processed intervals to avoid double-counting overlap. `extraction_issues` reports rejected proposals and schema/provenance errors separately from processing coverage. Missing or rejected facts must remain visible even when every source character was processed.

## Acceptance checks

Exercise the complete imported `EntityExtractor.extract` with a controlled OpenAI completion adapter; do not isolate source bodies with AST. A positive document over 31,000 characters must retain a later-page entity, metadata and relationship with exact original offsets. Repeated overlapping evidence must not duplicate entities or relationships. A single-entity list must reach the source-aware review. Made-up entities, verifier-added entities, unknown relationship endpoints, invented quotes and a co-occurrence-only relationship must be rejected. Scalar metadata conflicts must be unresolved rather than overwritten. Malformed or unavailable passes and an exceeded window budget must report incomplete coverage. Record model-call counts without claiming live-model quality or production precision/recall.

## Implementation validation

`/private/tmp/paperless-accuracy-venv/bin/python -m unittest tests.test_extraction -v` passes 18 behavior tests on Python 3.12. Tests configure loopback-only dummy services and use an injected completion adapter; no model endpoint or production document was accessed. The 35,058-character fixture reaches complete coverage, retains its tail premium and employment connection, and preserves exact original offsets. Additional checks reject numeric prefix/sign mistakes and contradictory duplicate relationship reviews, union repeated metadata rows, and omit conflicting entity types.

The suite establishes processing, rejection, reconciliation and failure contracts with controlled model outputs. It does not estimate live extraction precision/recall or prove that an independent model never accepts a false relationship. Conservative literal metadata checks may reject paraphrases, normalized dates and numerically equivalent formatting. No model-independent semantic entailment guarantee is claimed. Documents exceeding 359,200 characters require a separately configured larger budget or manual processing; their default result is explicitly partial.
