"""Read current corpus identities without initializing or repairing derived state."""
import asyncio
import hashlib
import json
import re


def encoded_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def positive_ids(values):
    result = set(values)
    if any(type(value) is not int or value <= 0 for value in values):
        raise ValueError('Corpus document identities must be positive integers')
    return result


async def current_corpus(paperless, graph, vectors, generation):
    """Use already attached read-only stores and a dedicated inventory client.

    This captures observed source/index identity, not atomic archive completeness.
    Caller must re-read and compare before execution and after delivery. Rubric
    originals never enter retrieval through this inventory operation.
    """
    before = await generation()
    if not isinstance(before, str) or not re.fullmatch(r'redis:[0-9]+', before):
        raise ValueError('Shared corpus generation unavailable; local fallback cannot qualify')
    summary_before = await paperless.get_document_summary()
    tags = await paperless.get_all_tags()
    names = paperless._configured_skip_tag_names()
    skip = {tag['id'] for tag in tags
            if str(tag.get('name') or '').strip().lower() in names
            or str(tag.get('slug') or '').strip().lower() in names}
    positive_ids(skip)
    documents = await paperless.get_all_documents()
    ids = positive_ids([doc['id'] for doc in documents])
    count = summary_before.get('count')
    if type(count) is not int or count != len(documents) or len(ids) != count:
        raise ValueError('Source inventory count or identity changed')
    async with asyncio.TaskGroup() as group:
        reads = [group.create_task(operation()) for operation in (
            graph.get_all_document_ids, vectors.get_document_embedding_ids,
            vectors.get_document_hash_ids, vectors.get_ingestion_fingerprints,
            lambda: vectors.get_open_feedback_document_ids(sorted(ids)),
        )]
    graph_ids, vector_ids, hash_ids, fingerprints, feedback = [task.result() for task in reads]
    graph_ids, vector_ids, hash_ids, feedback = [positive_ids(v) for v in
                                               (graph_ids, vector_ids, hash_ids, feedback)]
    if positive_ids(fingerprints) != hash_ids:
        raise ValueError('Processing markers changed while capturing corpus')
    summary_after = await paperless.get_document_summary()
    tags_after = await paperless.get_all_tags()
    after = await generation()
    summary_keys = ('count', 'latest_id', 'latest_modified')
    if (before != after or tags != tags_after or any(summary_before.get(k) != summary_after.get(k)
                                                  for k in summary_keys)):
        raise ValueError('Corpus changed while capturing inventory')
    inventory, eligible, stale = [], set(), set()
    for doc in sorted(documents, key=lambda d: d['id']):
        doc_id = doc['id']
        if not paperless.has_any_tag(doc, skip):
            eligible.add(doc_id)
        fingerprint = paperless.ingestion_fingerprint(doc)
        if doc_id in hash_ids and fingerprints[doc_id] != fingerprint:
            stale.add(doc_id)
        inventory.append({'id': doc_id, 'modified': doc.get('modified'),
                          'content_sha256': paperless.content_hash(doc.get('content') or ''),
                          'ingestion_fingerprint': fingerprint})
    return {'generation': before, 'source_inventory_sha256': encoded_hash(inventory),
            'tags_sha256': encoded_hash(tags), 'source_ids': sorted(ids),
            'eligible_ids': sorted(eligible), 'graph_ids': sorted(graph_ids),
            'vector_ids': sorted(vector_ids), 'completion_ids': sorted(hash_ids),
            'processing_sha256': encoded_hash(sorted(fingerprints.items())),
            'stale_ids': sorted(stale), 'open_feedback_ids': sorted(feedback),
            'missing_ids': sorted(eligible - (graph_ids & vector_ids & hash_ids)),
            'extra_ids': sorted((graph_ids | vector_ids | hash_ids) - eligible)}
