"""Failure-preserving sampled search and explicit graph provenance leads."""


def graph_document_ids(value):
    """Only typed provenance fields grant IDs; generated prose never does."""
    ids = set()
    if isinstance(value, list):
        for child in value: ids.update(graph_document_ids(child))
    elif isinstance(value, dict):
        for key, child in value.items():
            if key in {'source_doc', 'paperless_id'} and type(child) is int and child > 0:
                ids.add(child)
            elif key == 'source_doc_ids' and isinstance(child, list):
                ids.update(i for i in child if type(i) is int and i > 0)
            elif isinstance(child, (dict, list)):
                ids.update(graph_document_ids(child))
    return sorted(ids)


class SourceDiscovery:
    def __init__(self, index, graph):
        self.index, self.graph = index, graph

    async def search(self, query, operation_id):
        context = {'vector_results': [], 'keyword_results': [], 'entity_results': [],
                   'entity_kw_results': [], 'graph_nodes': [], 'entity_names': [], 'subgraph': {},
                   '_discovery': []}
        operations = context['_discovery']
        # The query itself is also a declared lexical enumeration opportunity.
        operations.append({'id': operation_id, 'query': query, 'status': 'complete',
                           'sampling': 'sampled', 'document_ids': []})
        methods = [('vector_results', self.index.vector_search, 20),
                   ('keyword_results', self.index.keyword_search, 15),
                   ('entity_results', self.index.entity_vector_search, 8),
                   ('entity_kw_results', self.index.entity_keyword_search, 8)]
        for key, method, limit in methods:
            op = {'id': operation_id + ':' + key, 'status': 'pending',
                  'sampling': 'sampled', 'document_ids': [], 'limit': limit}
            operations.append(op)
            try:
                result = await method(query, limit=limit, strict=True)
                if not isinstance(result, list) or any(not isinstance(r, dict) for r in result):
                    raise ValueError('invalid_search_response')
                context[key] = result
                op['document_ids'] = sorted({r['document_id'] for r in result
                    if type(r.get('document_id')) is int and r['document_id'] > 0})
                op['status'] = 'complete'
            except Exception as exc:
                partial = getattr(exc, 'partial_results', [])
                context[key] = partial
                op['document_ids'] = sorted({r['document_id'] for r in partial
                    if type(r.get('document_id')) is int and r['document_id'] > 0})
                op.update(status='failed', error=type(exc).__name__)
        # Preserve the existing sampled document-to-entity discovery before
        # presentation limits; all provenance returned by a sample becomes leads.
        doc_ids = list(dict.fromkeys(r['document_id'] for r in context['vector_results']
            if type(r.get('document_id')) is int and r['document_id'] > 0))[:8]
        doc_entities = []
        for i in doc_ids:
            op = {'id': f'{operation_id}:document_entities:{i}', 'status': 'pending',
                  'sampling': 'sampled', 'document_ids': [i], 'document_sample_limit': 8}
            operations.append(op)
            try:
                nodes = await self.graph.get_document_entities(i)
                if not isinstance(nodes, list): raise ValueError('invalid_document_entities')
                op['document_ids'] = sorted({i} | set(graph_document_ids(nodes)))
                doc_entities.extend(nodes)
                op['status'] = 'complete'
            except Exception as exc:
                op.update(status='failed', error=type(exc).__name__)
        entity_uuids = list(dict.fromkeys(r['entity_uuid'] for key in ('entity_results', 'entity_kw_results')
            for r in context[key] if isinstance(r.get('entity_uuid'), str) and r['entity_uuid']))
        entity_uuids = list(dict.fromkeys(entity_uuids + [r['uuid'] for r in doc_entities
            if isinstance(r.get('uuid'), str) and r['uuid']]))
        for uuid in entity_uuids:
            op = {'id': operation_id + ':entity:' + uuid, 'status': 'pending',
                  'sampling': 'sampled', 'document_ids': []}
            operations.append(op)
            try:
                node = await self.graph.get_node(uuid)
                if node is None: raise ValueError('missing_discovered_entity')
                op['document_ids'] = graph_document_ids(node)
                op['status'] = 'complete'
                context['graph_nodes'].append(node)
                name = node.get('properties', {}).get('name')
                if isinstance(name, str): context['entity_names'].append(name)
            except Exception as exc:
                op.update(status='failed', error=type(exc).__name__)
        if entity_uuids:
            op = {'id': operation_id + ':subgraph', 'status': 'pending', 'sampling': 'sampled',
                  'document_ids': [], 'entity_sample_limit': 15, 'depth': 3}
            operations.append(op)
            try:
                subgraph = await self.graph.get_subgraph(entity_uuids[:15], depth=3)
                if not isinstance(subgraph, dict): raise ValueError('invalid_subgraph')
                op['document_ids'] = graph_document_ids(subgraph)
                op['status'] = 'complete'; context['subgraph'] = subgraph
            except Exception as exc:
                op.update(status='failed', error=type(exc).__name__)
        return context

    async def entities(self, names, operation_id):
        context = {'graph_nodes': [], 'entity_names': [], '_discovery': []}
        for number, name in enumerate(dict.fromkeys(names)):
            op = {'id': f'{operation_id}:{number}', 'status': 'pending', 'sampling': 'sampled',
                  'document_ids': [], 'limit': 8}
            context['_discovery'].append(op)
            try:
                nodes = await self.graph.search_nodes(name, limit=8)
                if not isinstance(nodes, list): raise ValueError('invalid_graph_search')
                # Preserve direct provenance before fetching additional context.
                op['document_ids'] = graph_document_ids(nodes)
                context['graph_nodes'].extend(nodes)
                for node in nodes:
                    uuid = node.get('properties', {}).get('uuid')
                    if not isinstance(uuid, str) or not uuid: continue
                    full = await self.graph.get_node(uuid)
                    if full is None: raise ValueError('missing_discovered_entity')
                    op['document_ids'] = sorted(set(op['document_ids']) | set(graph_document_ids(full)))
                    context['graph_nodes'].append(full)
                op['status'] = 'complete'
                context['entity_names'].append(name)
            except Exception as exc:
                op.update(status='failed', error=type(exc).__name__)
        return context

    async def broad(self, entity_types):
        """Existing broad graph samples are leads, never transfer quotas."""
        operations = []
        org = {'id': 'broad:organizations', 'sampling': 'sampled', 'status': 'pending',
               'document_ids': [], 'limit_per_organization': 3, 'entity_types': entity_types}
        operations.append(org)
        try:
            rows = (await self.graph.get_recent_docs_per_organization_filtered(entity_types=entity_types, limit_per_org=3)
                    if entity_types else await self.graph.get_recent_docs_per_organization(limit_per_org=3))
            org['document_ids'] = sorted({r['doc_id'] for r in rows if type(r.get('doc_id')) is int and r['doc_id'] > 0})
            org['status'] = 'complete'
        except Exception as exc:
            org.update(status='failed', error=type(exc).__name__)
        if entity_types:
            types = {'id': 'broad:entity_types', 'sampling': 'sampled', 'status': 'pending',
                     'document_ids': [], 'limit': 40, 'entity_types': entity_types}
            operations.append(types)
            try:
                ids = await self.graph.get_documents_by_entity_types(entity_types, limit=40)
                if not isinstance(ids, list) or any(type(i) is not int or i < 1 for i in ids):
                    raise ValueError('invalid_graph_document_ids')
                types.update(document_ids=list(dict.fromkeys(ids)), status='complete')
            except Exception as exc:
                types.update(status='failed', error=type(exc).__name__)
        return {'_discovery': operations}
