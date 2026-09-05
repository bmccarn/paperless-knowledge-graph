# Functional and accuracy audit — September 4, 2026

Historical baseline. The [implementation report](2026-09-04-implementation-report.md) records the subsequent fixes and current validation; the defect-asserting probes below are not current CI gates.

The backend has substantial functionality, but its current checks do not establish answer accuracy. Strict mode can return an unsupported answer, model-supplied citations and dates are not fully validated, and human entity-review decisions can be bypassed by automatic resolution. These are higher priorities than adding more retrieval or model passes.

This extends the [initial repository audit](2026-09-04-repo-audit.md). Backend accuracy findings below were reproduced against revision `caaaa5a86d2eb17c5d4260a42077f488646242e6`; those query, extraction, and review implementations remain unchanged in this working-tree update. Graph browsing defects were fixed separately, as described below.

## What was actually checked

- Source survey across ingestion, storage, retrieval, ordinary/streaming queries, evidence checking, timelines, document inspection, feedback, entity review, conversations, domain hubs, and operations.
- Fourteen offline probe outcomes using the actual selected function bodies with controlled dependencies. These expose acceptance of bad inputs and failure handling; they do **not** measure how often a real model makes these errors.
- Graph browsing queries executed in a disposable Neo4j 5 Community database, including APOC and an actual missing-procedure response. The test adapter uses Neo4j's HTTP transaction interface; it does not validate Python Bolt-driver pooling or full FastAPI startup.
- Interactive Chrome checks of the rebuilt frontend using synthetic graph data, including desktop and 390 × 844 layouts. No private documents, production databases, or model endpoints were used.

Run the accuracy probes from the repository root:

```bash
python3 docs/audits/reproduce-accuracy-2026-09-04.py
```

[Captured results](2026-09-04-accuracy-probes.json) include the fabricated-reference, strict-mode, trust-score, extraction, review, and evaluation cases. The probes assert existing defects, so a successful run means those defects remain reproducible. Convert each into a regression asserting the desired behavior when fixing it. The script needs only the Python standard library and avoids application startup side effects.

## Feature coverage

“Implemented” means the code path exists. It does not mean the feature passed production-quality acceptance tests.

| Feature | Implemented behavior | Accuracy/reliability assessment and remaining acceptance work |
| --- | --- | --- |
| Classification and extraction | Type-specific metadata, entities, inferred relationships, entity verification, document summaries | Tail content is omitted and relationship endpoints can escape entity verification (B07). Need a synthetic document corpus with expected entities, dates, values, units, and source spans; measure precision and recall per document type. |
| Sync, full reindex, targeted reindex | Incremental scans, content hashes, concurrency, deletion cleanup, task progress | Initial audit A01/A03/A04/A07 reproduces or traces checkpoint loss, stale UUIDs, cancellation overlap, and destructive replacement ordering. Need storage fault injection and restart/retry convergence tests. |
| Graph and vector persistence | Neo4j entities/relationships, PostgreSQL chunks/embeddings/hashes | A05 loses shared relationship support when one document is removed. Exact-ID agreement checks presence, not extracted-content correctness. Cross-store partial writes and restores remain untested. |
| Hybrid retrieval | Vector, keyword, graph expansion, planning and gap review | Request/model/cache isolation is broken (A02); corpus search is capped (A10). Retrieval recall, ranking, and index use were not measured on a labeled corpus. |
| Quick/deep/strict answers | Different retrieval and verification budgets, answer repair, evidence grading | Strict mode is not an enforced abstention contract (B01). More passes do not compensate for missing deterministic acceptance checks. |
| Evidence, citations, trust | Evidence packs, claim ledgers, verifier results and numeric trust dimensions | B02–B04 allow bad references, contradictory checks, and incomplete coverage to appear more authoritative than warranted. |
| Timeline and current-state answers | Model timeline events, date sorting, current-state summary | Invalid dates and unknown documents survive; any dated source can mark current state resolved (B02/B05). Need effective/expiry semantics and explicit evaluation time. |
| Graph explorer | Initial sample, search, expansion, node details, 2D/3D rendering | Reworked and tested in this update. Graph projection and duplicate-edge defects fixed. Large-corpus completeness, dense-graph frame rate, and expansion limits remain unmeasured. |
| Documents and extraction feedback | Search/list, raw OCR, chunks, extracted facts, reindex action, review flags | Browser searches only a capped subset (A10). Feedback is recorded but does not correct or quarantine extraction (B09). Need real pagination and a resolution workflow. |
| Entity review/steward | Candidate comparisons, suggested actions, merge, split/ignore decision records | Human split decisions do not veto bulk auto-merge; low-confidence high-risk suggestions can still say merge (B06). Need durable decision enforcement and provenance-aware undo. |
| Conversations and streaming | Persisted user/assistant messages and evidence metadata; background completion after disconnect | Source survey only for persistence. Cache context collision reproduced in A02; strict verification timeout can retain draft text (B01). Need ordinary/streaming/saved-message parity, concurrent conversation requests, disconnect/reconnect, and process-restart checks. |
| Domain hubs | Five preset searches, up to 12 document results per hub, suggested questions | These are search shortcuts, not complete domain inventories. They inherit graph search limits and query accuracy gaps. Need explicit error/retry state and completeness/pagination checks. |
| Dashboard, debug, freshness and repair | Health/status, exact ID drift, tasks, log streaming and targeted repair | Source survey plus initial cancellation/checkpoint probes. Health and matching counts/IDs do not prove semantic correctness. Backup/restore drills, task recovery, and datastore fault tolerance remain untested. |
| Evaluations and CI | Six canonical questions, API smoke scripts, image publishing | The canonical scorer passes fabricated zero-source responses for all six cases (B08). CI lacks a behavior gate (A11). |

## Accuracy findings

All B01–B08 are P1 correctness issues. B09 is a P2 workflow gap. Findings are ordered by how directly they can allow a wrong answer or corrupt its evidence.

### B01 — Strict mode does not enforce a final supported-answer gate

**Evidence:** [query.py](../../app/query.py#L372), `_verify_repair_and_grade`, especially lines 470–547; [evidence.py](../../app/evidence.py#L444), `answer_needs_repair`; streaming verification timeout/error handling at [query.py](../../app/query.py#L959).

The actual strict-mode finalizer returned `Your current insurance premium is $999.` unchanged with an empty evidence pack and an unavailable verifier. A missing-evidence-only verifier result did not invoke answer repair. A later ledger marked the claim unsupported, but the ledger arrives after the last repair decision and did not trigger another repair or abstention. Streaming can also retain the original answer on verification failure.

**Required behavior:** define one finalization contract shared by ordinary responses, streaming completion, and conversation persistence. A strict factual answer must have validated evidence for each material claim. On unavailable/incomplete checks, return a clear inability to establish the answer, or explicitly separate supported content from unresolved questions. Streamed drafts must remain provisional until finalization; failures must not silently promote draft text to a final supported answer.

**Acceptance:** controlled missing evidence, verifier outage, repair returning nothing, repair introducing a new claim, late ledger conflicts, and timeouts must all produce the same safe final result across delivery and persistence paths. Retain a passing fully supported case so stricter checks do not become unconditional abstention.

### B02 — Claim references and timeline events are accepted without sufficient validation

**Evidence:** [evidence.py](../../app/evidence.py#L376), `normalize_claim_ledger` and `claim_ledger_from_verification`; [query.py](../../app/query.py#L518), ledger finalization; [strands_orchestrator.py](../../app/strands_orchestrator.py#L143), timeline output; [query.py](../../app/query.py#L353), timeline handling.

The normalizer retained document `999999`, evidence ID `NOT-IN-PACK`, and `FABRICATED QUOTE`, counting the claim as supported. The fallback ledger can also mark verifier strings supported without attaching evidence. The timeline path emitted an event for nonexistent document `999999`, dated `2099-02-31`, with trace status `ok`.

**Required behavior:** check that each referenced evidence item exists in the evidence pack, belongs to the cited document, and contains the quoted span under a documented normalization rule. A matching quote establishes provenance, not entailment; claim support still needs an independent assessment. Validate dates as actual calendar values, preserve unknown precision, and distinguish a document's date from an event or effective date. Reject or downgrade invalid references; do not manufacture replacements.

**Acceptance:** unknown IDs, quotes copied from the wrong document, altered numbers/units, invalid dates, and unsupported timeline events must not count as supported. All displayed evidence must be traceable to the original document/chunk/span.

### B03 — A general verifier result can override an unsupported claim ledger

**Evidence:** [query_quality.py](../../app/query_quality.py#L272), claim scoring and verifier override through line 320; overall grade at lines 350–362.

With five retrieved sources, strong coverage metadata, an entirely unsupported one-claim ledger, and a separate verifier result of `verified`, the actual scorer produced **0.945 / high**, with claim support **0.9**. This is a deterministic contradiction in the grading rules, not an estimate of observed model accuracy.

**Required behavior:** unresolved unsupported/conflicting claims must not be canceled out by retrieval quantity or a coarse verifier label. Derive the verdict from validated claim evidence, preserve disagreements as review-required, and report audit coverage separately from support. Treat numeric scores as heuristics until calibrated against independently labeled outcomes.

**Acceptance:** the captured contradictory case must never be high trust. Test unsupported, partial, conflicting, unknown, absent, and fully supported ledgers against each verifier status. Adding irrelevant sources must not turn an unsupported answer into a supported one.

### B04 — Verification omits content while grading the whole answer

**Evidence:** [strands_orchestrator.py](../../app/strands_orchestrator.py#L172), first 6,000 answer characters and 8,000 context characters; [evidence.py](../../app/evidence.py#L127), 1,200-character excerpts; serialization at lines 230–235. Compare synthesis chunk handling at [query.py](../../app/query.py#L1792).

Markers beyond the verifier limits were absent from the actual constructed verification prompt. A relevant fact after character 1,300 remained in an evidence item's full content but was omitted when the evidence pack was serialized for the model. The returned check is still applied to the entire answer.

**Required behavior:** budget and batch verification by material claim and its actual supporting spans. Track which claims and source regions were checked. If coverage is incomplete, report that fact and prevent a whole-answer verified verdict. Avoid merely increasing hardcoded character limits; long tables and later-page evidence still need deliberate selection.

**Acceptance:** place the only relevant fact near the end of a long selected chunk and the unsupported claim at the end of a long answer. Both must be checked or explicitly marked unverified.

### B05 — Dated evidence is mistaken for proof of current state

**Evidence:** [query_quality.py](../../app/query_quality.py#L244), freshness scoring; `current_state_summary` at lines 377–387; synthesis instructions at [query.py](../../app/query.py#L1441).

A source dated `2001-01-01` with an excerpt saying the policy expired in 2002 produced current-state status `resolved`, even while the same result counted one superseded source. The prompt also suggests using the newest document dates as a proxy for “now.”

**Required behavior:** separate newest retrieved evidence, explicit effective/expiry intervals, and verified current state. Use an explicit evaluation date, resolve amendments/cancellations and contradictory documents, and abstain from current-state assertions when the archive cannot establish them. Retrieval recency alone is insufficient.

**Acceptance:** expired-only, future-effective, renewed, canceled, and contradictory-policy fixtures should resolve correctly at pinned dates. An old document can be relevant historical evidence without proving a present condition.

### B06 — Human entity decisions and steward thresholds are not consistently enforced

**Evidence:** [main.py](../../app/main.py#L1409), stored split decisions; candidate filtering at lines 1347–1354; [entity_steward.py](../../app/entity_steward.py#L55); [entity_resolver.py](../../app/entity_resolver.py#L732), bulk merger through line 834; bulk invocation after reindex at [pipeline.py](../../app/pipeline.py#L1472). Steward choice logic: [entity_steward.py](../../app/entity_steward.py#L183).

With a stored split decision and a deterministically eligible pair, the actual bulk-merger orchestration merged the pair without reading decisions. The UI/steward candidate filters hide terminal decisions, but that is not a mutation veto. Separately, a high-risk agent recommendation with confidence `0.01` returned `merge` through the chooser's final branch. That second result is a suggestion, not evidence of an automatic destructive merge by the steward.

**Required behavior:** enforce human no-merge decisions at every merge entry point, including reindex resolution. Preserve decision identity across canonical merges. Keep split/never-merge intent distinct from actually undoing a previous merge. Require explicit thresholds in every recommendation branch; unresolved high risk should remain review-required.

**Acceptance:** never merge a prohibited pair in manual, automatic, reindex, or steward workflows; test low-confidence/high-risk recommendations and decision survival after related entities merge. Use Neo4j/PostgreSQL integration tests for actual merge, source preservation and undo behavior.

### B07 — Extraction can miss document tails and create unverified endpoints

**Evidence:** [extractor.py](../../app/extractor.py#L720), 30,000-character metadata/entity inputs and 20,000-character relationship input; verification at lines 776–812; combination/type fallback at lines 856–899; [pipeline.py](../../app/pipeline.py#L733), persistence of implied relationships.

A marker after character 31,000 was absent from all three actual extraction prompts. Entity verification uses title/entity-list context rather than the original source text and skips very small lists. Combining verified entities containing only `Alice Example` with a proposed relationship to `Fabricated Person` retained the unverified endpoint. The pipeline then attempts to resolve/create endpoints. Relationship rationale is also discarded before storing the inferred edge's limited metadata.

**Required behavior:** extract over complete documents with explicit coverage records, reconcile repeated entities, and bind every extracted claim/relationship to source spans. Constrain relationship endpoints to validated entities or validate new endpoints against the document before admitting them. Preserve the distinction between explicit and inferred relationships plus their evidence and explanation.

**Acceptance:** later-page entities/values, repeated tables, same-name people, unsupported relationship endpoints, and OCR-corrupted identifiers. Report extraction precision and recall, including coverage of long documents; a successful ingestion count is not an accuracy metric.

### B08 — The canonical evaluation suite does not detect unsupported answers

**Evidence:** [scripts/eval_harness.py](../../scripts/eval_harness.py#L39), `score_case`; [evals/canonical_questions.json](../../evals/canonical_questions.json).

All six cases passed a response made from required keywords, **zero sources**, verification `not_run`, arbitrary nonempty trace data, and an impossible timeline date. None of the six specifies required source IDs. `sensitive_domain` is unused, verification status is only reported, and the CLI's mode override omits `strict`.

**Required behavior:** establish a versioned synthetic corpus with known answers, source spans and expected abstentions. Score answer values/units, source validity, support, contradictions, temporal correctness, and completeness independently of model-supplied confidence/trust. Include negative cases and ordinary/streaming parity. Keep smoke checks, retrieval evaluations, and factual-quality evaluations separate.

**Acceptance:** the six fabricated responses must fail. Also include fully correct low-confidence answers, plausible wrong high-confidence answers, insufficient evidence, adversarial document instructions, same-name entities, long documents, and expected abstentions. Report incorrect-answer and abstention rates rather than only aggregate pass counts.

### B09 — Extraction feedback is a record, not a completed correction workflow

**Evidence:** [main.py](../../app/main.py#L979), feedback endpoint; [embeddings.py](../../app/embeddings.py#L666), feedback count and insert; [document detail page](../../frontend/src/app/documents/[docId]/page.tsx#L65). Repository-wide references to document feedback show insertion and counting, with no retrieval/extraction consumer.

“Review flag recorded” accurately describes the current behavior. The report does not amend the graph, quarantine questionable facts, lower their retrieval eligibility, or track resolution. Users can report an extraction error and continue receiving answers derived from it.

**Required behavior:** define feedback states, a review queue, the scope of any quarantine, and a correction/re-extraction path that invalidates affected answers. Do not silently discard an entire document just because one extraction is challenged; retain access to the original OCR and distinguish disputed derived facts.

**Acceptance:** flag a wrong extracted value, resolve it, and verify graph facts, chunks, cached answers and review status converge. The UI should identify whether a report is recorded, under review, or resolved.

## Graph rework delivered in this update

The explorer now starts in 2D with readable labels, a searchable node list, explicit expansion, type filtering, neighborhood focus and a single responsive evidence inspector. 3D remains optional. Clicking a node inspects it without moving the camera; dragging pins its position. Sample counts describe the loaded view, with visible notices for unusable returned identities.

Graph metadata is kept separate from the force renderer's mutable coordinates. Expansion merges by stable relationship identity, retains source properties, and no longer increases counts for repeated responses. Node details follow backend `in`/`out` direction, list recorded edge sources, distinguish inferred connections, and render descriptions as text rather than injecting HTML. Source links lead to the application's existing document-inspection route.

Backend graph changes are confined to browsing projections:

- Document relationship endpoints now use `doc-<Paperless ID>` when UUIDs are absent, matching node identity.
- Initial and neighborhood responses include Neo4j relationship IDs. Initial loading deduplicates the undirected-match rows by relationship identity, without collapsing distinct database edges.
- Missing APOC triggers the fallback only for a missing-procedure error. Other database errors remain visible.
- Fallback traversal respects bounded requested depth, returns the same endpoint shape and relationship IDs, includes edges among discovered nodes, and retains isolated nodes.

The original UI fixture went from 4 nodes / 3 edges to 4 nodes / 6 edges after one repeated expansion. The revised viewer stays at 4 / 3 after repeated expansion. The real-Neo4j regression initially had one failure and three errors; after the projection/fallback fixes all six checks passed. Frontend graph-data tests cover eight behaviors. See [graph validation](graph-validation.md) for reproduction and full verification limits.

These changes do not repair lost historical edge provenance or incorrect extracted facts. A graph can display stored data faithfully while that data is wrong. Remaining graph work includes datastore-side search/pagination, explicit expansion budgets/truncation metadata, profiling dense neighborhoods, preserving per-document support across deletes, and evaluating an evidence-first non-force layout for large graphs.

## Recommended implementation sequence

1. **Final answer acceptance and claim provenance:** B01–B04 together, with one finalization contract used by both response adapters and conversation persistence. Establish the negative and positive fixture evaluations in B08 before changing grading rules.
2. **Current-state and extraction correctness:** B05/B07, with explicit dates, source spans, document coverage and validation of relationship endpoints. Measure quality on the synthetic corpus before adjusting models, prompts or retrieval budgets.
3. **Human decisions and recoverable ingestion:** B06/B09 plus A01/A03/A04/A05/A07. Enforce mutation vetoes, preserve per-document evidence support, and prove retries converge under real datastore failures.
4. **Feature completeness:** actual pagination for documents/hubs/graph search; ordinary/streaming/saved-answer parity; visible review resolution and failure/retry states. Wire the behavioral checks into CI.
5. **Measured optimization:** fix per-request model/cache identity (A02), profile blocking Redis and repeated retrieval (A08), and evaluate vector index options (A09) against labeled recall. Record query latency, cost, recall, unsupported-answer rate and graph frame time. Reject optimizations that materially reduce accuracy; no speedup or quality gain is claimed by this audit.

Production accuracy, model quality, full ingestion recovery, and dense-graph performance remain unmeasured. The backend findings above are open; this update fixes graph browsing and supplies reproducible evidence and a prioritized correctness plan for the broader application.
