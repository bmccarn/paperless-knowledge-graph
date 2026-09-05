# Entity decision enforcement and identity lifecycle

## Problem and alternatives

The current review queue hides split decisions, but bulk and ingestion-time resolution do not enforce them. A generic resolver cache returns deleted UUIDs after reindex. Steward recommendations can bypass the stated confidence/risk thresholds.

1. **Filter bulk candidates only.** Small change, but leaves exact/fuzzy/embedding matching and manual merging able to bypass decisions. Rejected.
2. **Clear the generic cache on every deletion.** Requires every mutation caller to remember invalidation and cannot repair old UUID-only decisions after reindex. Rejected as the primary correctness mechanism.
3. **Enforce decisions at entity resolution and merge entry points, persist conservative identity snapshots, and resolve against live graph records.** Chosen. It concentrates decision semantics in one module, uses the existing storage adapters, and makes review behavior testable through public resolver/steward interfaces.

## Chosen contract

The pre-agreed test seams are public `EntityResolver.resolve_person`, `resolve_organization`, `resolve_generic`, `resolve_all_entities`, `merge_entities`, `record_decision`, `hydrate_review_identities`, and `EntitySteward.run_once`/`choose_recommendation`. Tests import real modules with explicit synthetic configuration and controlled graph, decision-store, and reviewer adapters. No live model or private document data is used.

- `split` and `never_merge` prohibit matching/merging either orientation of a pair. `ignore` hides a queue candidate but does not imply a permanent no-merge instruction. Suggestions never authorize mutations.
- Every automatic matching route checks vetoes before returning an existing entity; every actual merge rechecks decisions. Decision-store failure propagates, rather than permitting a merge with unknown policy.
- `record_decision(left_uuid, right_uuid, decision, note="")` captures both graph identities before persistence. Snapshots include type, normalized names/aliases, and source document IDs. Canonical merges preserve veto associations before deleting the old identity.
- `hydrate_review_identities()` snapshots legacy UUID-only vetoes before destructive reindex while the old nodes still exist. It is safe to call repeatedly. If a needed legacy node has already disappeared, record/report the unresolved migration instead of claiming reindex durability.
- Snapshot matching is a conservative safety constraint, not proof of entity identity. Typed names/aliases survive reindex; source-document context distinguishes reviewed same-name entities when available. Ambiguous matches remain separate. Changed names without preserved aliases/source context cannot be reconstructed reliably; that limitation stays explicit.
- The decision store adds `left_identity`/`right_identity` JSONB to existing decision rows and returns decoded objects. Ordering UUIDs on write must also reorder the corresponding identity snapshots.
- Generic resolution consults current graph records; an old in-process UUID cannot be returned merely because its name was cached.
- Agent review cannot lower a deterministic high-risk classification. Merge recommendations require confidence at least 0.82 and low risk from both assessments; unsupported/invalid confidence and low-confidence recommendations remain review-required.

## Shared integrations owned by root

- Extend `EmbeddingsStore.add_entity_review_decision` with keyword-only `left_identity=None, right_identity=None`; return them from `get_entity_review_decisions`.
- Route human split/ignore through `entity_resolver.record_decision` and manual merge through `entity_resolver.merge_entities`.
- Call `entity_resolver.hydrate_review_identities()` before destructive graph clearing/removal while reviewed UUIDs still resolve.
- Preserve truthful mutation errors: prohibited pairs should become a conflict response; decisions cannot be silently bypassed when PostgreSQL is unavailable.

## Acceptance and validation

Implement in vertical slices, observing each regression fail before its fix:

1. A stored split/never-merge prevents a normally eligible bulk merge, in either pair orientation; an allowed pair still merges.
2. An ingestion-time exact, fuzzy, LLM-assisted or embedding candidate cannot bypass a veto; decision-store failure does not merge.
3. Generic resolution after deleting/reindexing its graph node returns an existing newly created node, never the removed UUID.
4. Human decisions acquire snapshots; hydration preserves existing notes and snapshots. Recreated nodes with changed UUIDs are still protected where identity evidence is available.
5. A related allowed canonical merge carries the old entity's veto forward; a forbidden direct manual merge performs no graph mutation.
6. High-risk or low-confidence agent recommendations cannot become merge suggestions, while a supported low-risk/high-confidence case can.

Cross-database atomicity and multi-process locking are not established by controlled-adapter tests. Persisting conservative veto associations before graph mutation favors preventing an unsafe merge if a later graph write fails. Real Neo4j/PostgreSQL migration and fault-injection checks remain necessary for deployment confidence.

## Graph merge implementation extension

The parent workstream also assigns `GraphStore.merge_entities` and the three manual review routes here. Merge will use a single Neo4j write transaction with exact UUID identity, retain primary identity and removed names/aliases, and union source-document IDs. Edges landing on the same endpoints/type combine per-document support through the shared `merge_support_properties` implementation rather than discarding duplicate provenance. Any failure rolls back graph mutation. An opt-in regression imports the actual `GraphStore` and uses the explicitly authorized disposable local Neo4j; fixture cleanup is limited to a unique test UUID prefix. Shared support creation/deletion and PostgreSQL migrations remain owned by the parent.

## Implemented validation, 2026-09-04

Twenty-two public behavior regressions pass: eighteen use controlled graph/decision/reviewer adapters, and four exercise the actual `GraphStore` against disposable Neo4j. The latter verify exact UUID matching, inbound/outbound support, same-document quote union, alias preservation, and rollback on malformed support. The original APOC implementation failed by dropping the duplicate's aliases. Two additional red-before-green cases reproduced a broken LLM request (`{{dict}}` expressions outside the prompt) and repeated same-name entity creation after a split; request shape/strict boolean acceptance and stored source IDs repair them. Model approval cannot override deterministic person-name safeguards. Failed bulk/manual merge routes clear earlier cached answers even when a later metadata write fails. Steward suggestions reject boolean confidence and missing agent risk.

Commands (using the task's isolated Python 3.12 environment):

```sh
python -m unittest tests.test_entity_decisions -v
NEO4J_TEST_BOLT=bolt://127.0.0.1:17687 python -m unittest tests.test_entity_decisions -v
```

The default command skips four Neo4j tests. The opt-in command requires an explicitly authorized disposable localhost database; it creates and deletes only unique `entity-merge-test-` fixtures. These checks do not establish cross-store atomicity or safety under simultaneous workers. The model responses are controlled fixtures, so tests validate acceptance rules rather than model judgment accuracy.
