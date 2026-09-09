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

**Entity resolution**: Binding an accepted source identity to a unique compatible canonical identity, provenance-verified alias or explicit source co-reference. Similarity suggests candidates but cannot authorize matching, trusted aliases or merges. Human-reviewed UUID merges are separate from ingestion. See `docs/specs/evidence-aware-entity-resolution.md`.

**Entity steward**: A conservative reviewer that records merge, split, or ignore suggestions. Suggestions are distinct from the mutations performed by entity resolution or an explicit merge.

**Chunk**: A text segment stored with its embedding for retrieval. Raw OCR is stored separately from embedding metadata; generated summary chunks can guide retrieval but cannot certify facts. Chunk count and document count are different measures.

**Hybrid retrieval**: Combining vector, keyword, and graph evidence for a question, with additional planning and expansion depending on query mode.

**Query mode**: `quick`, `deep`, `timeline`, or `strict`; selects retrieval and answer-processing behavior independently of the selected model.

**Evidence pack**: Source excerpts and provenance assembled for answer synthesis and verification.

**Claim ledger**: Structured claims with supporting evidence and verification status bound to one exact answer candidate. A complete ledger means every unit was audited, not that the answer or archive is complete.

**Verified partial answer**: Whole supported units from the final eligible audit, independently re-audited as a new candidate. Its delivered ledger is complete, while answer completeness remains false and omitted counts are preserved separately.

**Documented state**: An audited observation or comparison scoped to retrieved dated records. It does not establish active real-world status or archive completeness.

**Historical source reservation**: A bounded retrieval opportunity for older and newer relevant indexed documents that survives ranking into synthesis and audit context. It confers no factual authority.

**Timeline date projection**: Chronologically sorted calendar mentions from the final accepted candidate. Independent observations retain their full text and sources; legacy prose date entries point back to the complete answer context. A mentioned date does not independently establish an event or active status.

**Trust dimensions**: Reported quality signals about evidence, verification, coverage, and answer risk; these are computed signals, not guarantees of correctness.
