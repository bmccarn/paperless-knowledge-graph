# Evidence-aware entity resolution (policy `evidence-identity-v1`)

## Contract and changed paths

Source-window extraction and source-aware verification precede identity resolution. A complete extraction still must pass the existing exact-source/coverage checks. The post-extraction name/title-only validator, general-knowledge merge tiebreaker, process-wide identity/type caches, fuzzy auto-linking and embedding auto-linking are removed. No model or embedding configuration changes are made.

- `entity_policy.py`: narrow typed orthography and explicit co-reference grammar, provenance validation. Diacritics are NFC-normalized, not discarded; initials, generation suffixes, legal suffixes, divisions and distinguishing words survive. Honorifics and equivalent legal-form abbreviations are normalized without erasing their legal class. Legal suffix punctuation is orthographic; deleting `Bank`, `Insurance`, `Holdings`, `LLC` etc. is not.
- `extraction_evidence.py` / `extractor.py`: identity IDs include name/type/source-literal discriminator. Qualified same-name homonyms stay separate. Different supported types at disjoint mentions stay separate; conflicting types at overlapping evidence abstain. Unqualified occurrences cannot join qualified homonyms. Relationships use exact extracted identity IDs (unambiguous name-only responses remain compatible). A changed type needs a separate exact contextual source quote and rationale; otherwise the proposed type and description survive. Models still judge semantic support; a literal quote alone is not a proof of model accuracy.
- `entity_resolver.py`: a unique compatible canonical orthographic match, human-reviewed canonical pair/alias, or actual explicit source co-reference can select an identity. Multiple matches abstain into a source-local ambiguous identity rather than choose the first result. Human vetoes are checked before every match and explicit UUID merge. Short acronyms/partial person names need source scope or reviewed proof. Similarity remains available to steward review suggestions, not mutations.
- `entity_bindings.py` / `pipeline.py`: one document-lifetime registry binds each accepted identity to UUID and final graph label. Legacy metadata can reference but cannot recreate/retype rejected or ambiguous identities. Relationship endpoints and entity vectors consume this registry; no search-based fallback or repeated name resolution. Context-local metadata bindings reset even on error. Errors prevent the document completion marker.
- `graph.py`: candidate reads include all canonical matches and provenance. Source membership is updated on use and removed on document replacement. Raw aliases remain searchable for compatibility but do not authorize identity.
- `POST /resolve-entities` (existing bulk endpoint): now non-mutating identity-review reporting, `total_merged=0`. Co-occurrence or even identical names cannot safely merge existing UUIDs, with one owner-approved exception: the steward's automatic merge of exact-duplicate low-risk and `Address` pairs (below). Explicit human review merge is still supported and guarded by no-merge decisions.

## Automatic merge of exact-duplicate low-risk entities and addresses

Ingestion already binds a new mention to an existing node when their names are equal under `name_key`. The steward may therefore merge two existing nodes that ingestion would treat as one identity. The setting `ENTITY_STEWARD_AUTO_MERGE` is `off`, `shadow` (the default) or `on`.

A pair merges only if every rule holds. `auto_merge_blocker` in `entity_policy.py` checks the node facts (rules 1-2 and 4-10). The resolver checks every rule again on fresh reads under its mutation lock before it merges.

1. Both nodes have exactly one label, the same one, in `AUTO_MERGE_LABELS`: the `LOW_RISK_LABELS` (Organization, Product, System, DocumentRef, Location) and `Address`.
2. Their keys from `auto_merge_key` are equal and non-empty: `name_key` of the name, or for an `Address` the address key (below).
3. Exactly two nodes of the whole label have that key. Groups of three or more stay in the human queue.
4. Neither node is `ambiguous`, `isolated` or `quarantined`.
5. Neither node has `review_anchor = true`.
6. Neither name is context-bound (not checked for `Address`).
7. `identity_hints` are both empty or equal.
8. The source documents do not overlap, counting both `source_doc_ids` and the documents on the node's edges.
9. No direct edge connects the pair.
10. The identifier properties in `IDENTIFIER_PROPERTIES` (other than `identity_hints`) have equal normalized values on both nodes; a value on one side only also blocks.
11. The pair has no terminal decision row and no `legacy_unknown` row.
12. `merge_is_prohibited` and `alias_replay_prohibited` both pass.
13. The steward's agent review returns a merge verdict with numeric confidence of at least 0.82 and low risk. Any other answer, an empty answer, an error or a timeout blocks. The agent can block a merge and can never authorize one.
14. The run is under `ENTITY_STEWARD_AUTO_MERGE_MAX_PER_RUN` (default 10, at most 100). The cap counts agent reviews, so it also bounds model calls; pairs reviewed longest ago go first.

### Addresses

`Address` stays a high-risk label for the similarity review. It enters the automatic merge only through its exact address key. `address_key` in `entity_policy.py` reads `full_address`, or `name` when `full_address` is empty; many `Address` nodes are document-owned and have no name. The key is case-folded, its whitespace collapsed, and `.`, `,`, `;` and `:` replaced by spaces. The only abbreviations are whole-word street types, long form to short: Street, Avenue, Road, Drive, Lane, Court, Boulevard, Place, Parkway and Circle. Directionals, unit words and state names stay as written. Every other character stays, so house numbers, units (`Apt 2` and `Apt 3`) and postal codes are never dropped. An `Address` with neither field has no key and never qualifies. A key also needs a US postal code (5 digits, optionally `-` and 4 more) after its first word; without one, one street address can name places in many towns. That blocker is `postal_code`. A pair blocks on `address_key` when the keys differ. The suggestion note records the matched `key`.

`entity_identity` uses `full_address` as the canonical name of a nameless `Address`, so a split recorded by undo still matches a node that a reindex recreates with a new UUID. A human review merge of a nameless `Address` is still refused.

The survivor is the node with more `source_doc_ids`, then the node on the current `resolution_policy`, then the smaller UUID.

- `shadow` writes `suggest_auto_merge` (`automated_suggestion`, `entity_steward`) with the proposed survivor and merges nothing. The review page shows it through the existing suggestion handling.
- `on` merges with `review_method=entity_steward_auto` and no `review_id`, so the graph writes no `human_review` alias record. The decision row is `auto_merged` with `provenance=automated_merge` and both identity snapshots. The merge journal row allows undo; undo marks the `auto_merged` row `reverted` and records a split.
- Human-review replay and alias lineage read only human `merged` rows, so an `auto_merged` row authorizes no later match. Later ingestion binds either name to the survivor through the orthographic route.
- A pair the agent blocks, or a merge a veto prohibits, is written as `suggest_review` for a person.

Residual risk: two different real organizations or products with the same name, from different documents and with no hints, will merge. No name-only rule can prove identity. Ingestion already takes this risk for new mentions; the merge journal is the remedy. For addresses, ingestion still matches by `name_key` and attaches a new node for each document's property address, so a reindex can create a new duplicate that a later steward run finds again.

## Additive storage/migration interface

Neo4j node properties (lazy, no destructive migration):

| Property | Meaning |
|---|---|
| `aliases` | Existing raw display/search strings; unknown legacy entries remain untrusted |
| `alias_records` | List of JSON strings, each with `alias`, `canonical_name`, `type`, `provenance`, `policy` |
| `resolution_policy` | Policy which created the node |
| `resolution_status` | `new` or `ambiguous`; operator repair can set `quarantined` to exclude an identity from auto-resolution |
| `identity_hints` | Literal source identifiers; disagreement prevents homonym conflation |
| `source_doc_ids` | Source membership, maintained on linking and document replacement |

`source_coreference` alias records also contain `source_doc_id`, `source_hash`, `start`, `end`, `quote_hash`. They authorize only the same source revision under the current policy, and the explicit alias grammar must still validate. A bare co-mention, issuer/brand association, ownership/subsidiary relationship or arbitrary parenthesis is insufficient. Alias quarantine tombstones override other alias records and reviewed-pair replay for that target. Supported forms: explicit `also known as`, `doing business as`, `d/b/a`, `aka`, `hereinafter referred to as`, or mechanically matching initialism parentheses. Local initialism expansions constrain matching even when the alternate full name is not yet in the graph; a prior globally reviewed acronym alias cannot override a different or ambiguous current source expansion. Source analysis is per-call/document-revision, not a process-wide decision cache. The grammar is deliberately incomplete; unsupported equivalences remain separate for review.

`human_review` alias records require `review_id` and `review_method=entity_review_api`, both persisted on the corresponding PostgreSQL decision. Resolver merge and decision methods require this explicit origin (default/unattributed or automated origins are rejected before mutation). The API route supplies the method; it is not inferred from `decision=merged`, an empty note, a snapshot, or merely entering the generic merge function. This records the review interface, not an authenticated individual actor identity; the deployment must continue enforcing its existing API access controls. An explicit human canonical-pair merge records the duplicate canonical name as reviewed, **not every inherited alias**. Unknown records are never retroactively reclassified as human-reviewed. `status=quarantined|revoked|untrusted` disables an individual alias record without deleting historical evidence.

PostgreSQL `entity_review_decisions` adds:

- `provenance TEXT NOT NULL DEFAULT 'legacy_unknown'` — new human review calls use `human_review`; prior rows are not guessed.
- `identity_status TEXT NOT NULL DEFAULT 'unassessed'` — hydration uses `active` or `unresolved_legacy`.
- `review_id TEXT` — correlation ID linking a new human merge to its graph alias records; null for unknown legacy rows.
- `review_method TEXT NOT NULL DEFAULT 'legacy_unknown'` — explicit future method provenance. Legacy `merged` records may have come from automation and **never** establish human review by label alone. Steward suggestions use `provenance=automated_suggestion`, `review_method=entity_steward`; they cannot enter the reviewed alias trust path. Automatic merges use `provenance=automated_merge`, `review_method=entity_steward_auto` and cannot enter it either.
- New snapshots include `canonical_name` and provenance-verified `reviewed_names`; this, not the combined legacy alias set, can authorize a newly human-reviewed pair after UUID replacement. Existing snapshot names remain conservative **veto constraints**, not matching proof.

The existing `hydrate_review_identities()` preserves legacy no-merge rows. A synthetic mixed 19-row fixture (17 one-live-node pairs, 2 neither-live-node pairs) verifies that none becomes a fully reconstructed pair. It hydrates only from extant exact UUIDs and explicitly marks missing-identity rows `unresolved_legacy`; it never fabricates identities. Hydration/quarantine updates do not change original timestamps/notes/UUID order, including historically reversed pairs. Partial vetoes protect their identifiable side until repair. `EmbeddingsStore.get_entity_review_decisions()` returns these fields for an authorized storage audit. The existing `GET /entity-review/candidates` response is a candidate queue, **not** a complete decision/quarantine audit: it only exposes candidates and an ignored-pair count. Parent repair must read and compare the underlying exact decision rows; this PR does not introduce an unrestricted decision-data API. Missing both identities is irrecoverable from names alone: the row survives but cannot identify arbitrary future UUIDs. This limitation must remain an explicit repair/release gate, not a claim of complete veto recovery.

## Parent-owned release and repair gate

This source PR does not merge, deploy, read private documents, modify production graph data or start ingestion.

1. Independently review exact PR SHA and green backend/frontend CI (including real disposable datastore tests). Back up both stores. Keep sync/steward/full migration held.
2. Snapshot aggregate counts plus an access-controlled manifest of exact decision row IDs/UUIDs/statuses before migration. Preserve all **19** missing-identity no-merge rows and their original fields; compare exact sets, not just totals. Source implementation never synthesizes their identities. Parent may recover from authoritative backups or explicitly retain unresolved quarantine with documented limitations.
3. Deploy with additive schema initialization only under parent approval. Compare legacy row preservation/status. Record immutable images and policy fingerprint. Rollback code does not require dropping additive columns, but **old code still trusts old alias strings**; do not resume old ingestion against contaminated aliases as a safe rollback.
4. Audit contaminated identity/alias/relationship scope before repair. Versioning invalidates completion markers but does not undo already-merged shared identities. Parent repair must inventory exact target UUIDs, source IDs, per-source edge support, alias records, and entity vectors. Unknown aliases can stay preserved/untrusted; adjudicated records may be marked quarantined. Canonicals requiring adjudication can be marked `resolution_status=quarantined`. Do not delete a shared entity because one source link is wrong, bless all historic aliases, or treat a reindex as proof of cleanup.
5. Review a bounded dry-run manifest with expected-before values and rollback artifacts, then apply only parent-approved repairs under the no-writer gate. Check source-specific support rather than deleting whole shared edges. Explicit human aliases must use distinguishable provenance; unresolved aliases/decisions must remain auditable.
6. Run controlled real-source canaries and independently assess identity/type semantics, source membership, exact UUID/type endpoint bindings, alias provenance and unchanged extraction coverage/provenance. Do not start bulk sync because structural extraction passed alone.
7. The ingestion fingerprint now includes `RESOLUTION_POLICY` in addition to the existing OCR/source metadata/model fields. Previous markers need normal incremental reconciliation **only after** semantic repair acceptance. OCR hashes and feedback identities remain unchanged. Corpus acceptance and scheduler restoration remain parent-owned.

## Validation and limitations

Run the dependency-free matrix locally: `python3 -m unittest tests.test_entity_policy -v`. Full locked Python 3.12 tests run in CI, first offline and then with disposable loopback-bound Neo4j 5/APOC, pgvector PostgreSQL 16, Redis 7. Image publication depends on the backend job including this real-datastore step. Frontend/browser gates remain required. No production credentials, data or models are used by tests.

Regressions cover positive exact/orthographic names, legal punctuation, NFC diacritics, initials, surname/generation distinctions, agencies, legal entities, issuer/brand/subsidiary separation, explicit aliases, context-specific expansions, ambiguous reviewed aliases, poisoned legacy aliases, homonyms, repeated relations, source-less failure behavior, source/type cache absence, vetoes and the 19-row legacy migration. Existing uncapped extraction, bounded three attempts / SDK retries zero, finite default timeouts, real-truncation splitting, full-source coverage and exact provenance tests remain in the suite.

Limitations: unique multiword exact names without contradictory identifiers may still be unknowable homonyms; no name-only resolver can prove real-world identity in that case. Source-literal discriminators must be actually extracted to distinguish otherwise identical mentions; omission is a model-quality risk. Explicit alias grammar cannot understand every negative, historical or quoted statement. Unsupported variants (accent omission, expanded initials, dropped legal suffixes, OCR errors) need explicit source co-reference or human review rather than a guessed fuzzy merge. Read-only candidate scans remain corpus-linear; no new performance claim or embedding redesign is made.
