# Paperless Knowledge Graph

This project derives a searchable knowledge graph and vector index from Paperless-ngx documents. Paperless remains the source of document content; the graph, embeddings, and processing records are derived state.

## Language

**Paperless document**: A source document identified by its integer Paperless ID, with OCR content, title, timestamps, and tags. Graph UUIDs and conversation IDs are different identifiers.

**Eligible document**: A Paperless document whose tags do not match the configured skip tags, which default to `needs-review`.

**Sync**: Incremental ingestion of eligible documents, reconciling the full eligible snapshot with completion records and a versioned ingestion fingerprint. The fingerprint covers OCR, source metadata and the primary extraction model; the separate OCR content hash remains the feedback-review identity.

**Reindex**: Rebuilding derived state for one document or the entire corpus. Full reindex prepares and replaces each document individually. Missing completion hashes mark partial writes for retry; cross-store replacement is recoverable, not atomic.

**Freshness**: Comparison of exact document ID sets in Paperless, the graph, embeddings, and processing hashes, plus modification timestamps and ingestion fingerprints. Equal counts alone do not establish freshness.

**Drift**: Missing, stale, or orphaned derived document state reported by freshness checks. Targeted repair reindexes or purges the affected document IDs.

**Entity**: An extracted person, organization, or other typed concept with a graph UUID; the same entity may be mentioned by several documents.

**Entity resolution**: Binding an accepted source identity to a unique compatible canonical identity, provenance-verified alias or explicit source co-reference. Similarity suggests candidates but cannot authorize matching, trusted aliases or merges. Human-reviewed UUID merges are separate from ingestion. The one exception is an automatic merge of an exact-duplicate pair (same `name_key`) of a low-risk type, which ingestion would already treat as one identity. See `docs/specs/evidence-aware-entity-resolution.md`.

**Entity steward**: A conservative reviewer that records merge, split, or ignore suggestions. Suggestions are distinct from the mutations performed by entity resolution or an explicit merge. Its automatic merge pass is in shadow mode by default: it records `suggest_auto_merge` for each exact-duplicate pair that passes the gate and merges nothing. Only the setting `on` lets it merge such a pair, as `auto_merged`, and each merge can be undone from the merge journal. Its agent review can block an automatic merge but never authorize one.

**Chunk**: A text segment stored with its embedding for retrieval. Raw OCR is stored separately from embedding metadata; generated summary chunks can guide retrieval but cannot certify facts. Chunk count and document count are different measures.

**Hybrid retrieval**: Combining vector, keyword, and graph evidence for a question, with additional planning and expansion depending on query mode.

**Query mode**: `quick`, `deep`, `timeline`, or `strict`; selects retrieval and answer-processing behavior independently of the selected model.

**Answer model**: The request-selected route for ordinary final synthesis. Existing failure handling can use a fallback route or an audited helper draft; the optional question pipeline uses Strands composition. JSON retrieval helpers use the configured primary route, then the fallback after parse errors or rate limits. Query cache identity includes the helper route. Strands planning and auditing use their separately configured route.

**Evidence pack**: Source excerpts and provenance assembled for answer synthesis and verification.

**Claim ledger**: Structured claims with supporting evidence and verification status bound to one exact answer candidate. A complete ledger means every unit was audited, not that the answer or archive is complete.

**Audit source prefix**: Shared source context serialized before candidate and batch fields to permit prompt-prefix caching. Reuse changes no audit rule or delivered answer state; every delivered claim still requires support from the last complete audit.

**Verified partial answer**: Whole supported units from the final eligible audit, independently re-audited as a new candidate. A complete subset audit that rejects more units allows one more subset round; at most two rounds run, and the last audit must support every delivered unit. Units whose original audit batch kept an invalid reply after its correction are omitted as unchecked, never delivered. Its delivered ledger is complete, while answer completeness remains false and omitted counts are preserved separately.

**Documented state**: An audited observation or comparison scoped to retrieved dated records. It does not establish active real-world status or archive completeness.

**Historical source reservation**: A bounded retrieval opportunity for older and newer relevant indexed documents that survives ranking into synthesis and audit context. It confers no factual authority.

**Timeline date projection**: Chronologically sorted calendar mentions from the final accepted candidate. Independent observations retain their full text and sources; legacy prose date entries point back to the complete answer context. A mentioned date does not independently establish an event or active status.

**Trust dimensions**: Reported quality signals about evidence, verification, coverage, and answer risk; these are computed signals, not guarantees of correctness.
