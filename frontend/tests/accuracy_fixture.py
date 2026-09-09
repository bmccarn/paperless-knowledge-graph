"""Synthetic UI contract fixture; listen only on localhost and never contact services.

Run: python3 frontend/tests/accuracy_fixture.py --port 8485
303 indexed docs, 120 pageable search entities, mutable feedback, safe/failed SSE.
This fixture validates frontend behavior, not backend factual correctness.
"""
import argparse
import hashlib
from pathlib import Path
import sys
import copy
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import threading
import time
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from app.answer_delivery import render_verified_answer
from app.timeline import project_timeline

from graph_fixture import NODES, RELS, identity

RELS = copy.deepcopy(RELS)
RELATIONSHIP_QUOTE = 'Alex is a customer. <img src=x onerror="window.__relationship_xss=1">'
RELS[0]['props'].update(source_doc_ids=[101], inferred=True, support_records=[json.dumps({
    'source_doc': 101, 'inferred': True,
    'evidence_json': json.dumps([{'start': 0, 'end': len(RELATIONSHIP_QUOTE), 'quote': RELATIONSHIP_QUOTE}]),
    'rationale': ['The source names Alex as the customer of Example Utility.'],
})])

LOCK = threading.RLock()
NOW = '2026-09-04T12:00:00+00:00'
DOCS = [dict(node['props']) for node in NODES if 'Document' in node['labels']]
DOCS[0].update(title='January premium statement', doc_type='insurance')
DOCS.extend({'paperless_id': 9000 + index, 'title': f'Synthetic archive {index:03d}',
             'doc_type': 'invoice' if index % 2 == 0 else 'medical', 'date': '2026-09-01'}
            for index in range(301))
SEARCH_NODES = copy.deepcopy(NODES) + [
    {'labels': ['Organization'], 'props': {'uuid': f'synthetic-search-{index:03d}',
        'name': f'Search Fixture Entity {index:03d}', 'description': 'Synthetic pageable graph search fixture.'}}
    for index in range(120)]
FEEDBACK = []
CONVERSATIONS = {}
TASKS = {}
PROCESSED = {doc['paperless_id']: NOW for doc in DOCS}
REVIEWED = set()
REVIEW_CANDIDATES = [
    {'score': 90, 'label': 'Organization',
     'left': {'uuid': f'synthetic-review-{i}-left', 'name': f'Cedar Lab {i}', 'properties': {'description': 'Synthetic review candidate.'}},
     'right': {'uuid': f'synthetic-review-{i}-right', 'name': f'Cedar Laboratory {i}', 'properties': {'description': 'Synthetic alternate name.'}},
     'steward': {'decision': 'suggest_review', 'deterministic': {'risk': 'medium', 'score': .9,
                  'reasons': ['Compare the synthetic source evidence before deciding.']}}}
    for i in range(1, 4)]
LOGS = [{'timestamp': NOW, 'level': level, 'logger': 'synthetic.fixture',
         'message': f'Synthetic {level.lower()} log for visual acceptance.'}
        for level in ('DEBUG', 'INFO', 'WARNING', 'ERROR')]
SOURCE_TEXT = 'January statement: the premium is $25. This is synthetic document evidence.'


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def document(doc_id):
    return next((doc for doc in DOCS if doc['paperless_id'] == doc_id), None)


def detail(doc_id):
    doc = document(doc_id)
    reports = [copy.deepcopy(row) for row in FEEDBACK if row['document_id'] == doc_id]
    content = SOURCE_TEXT if doc_id == 101 else f"Synthetic source text for {doc['title']}."
    return {'paperless': {'id': doc_id, **doc, 'content': content},
            'graph': {'document': doc, 'entities': [], 'relationships': []},
            'chunks': [{'chunk_index': 0, 'title': doc['title'], 'doc_type': doc['doc_type'], 'content': content}],
            'processing': {'processed': True, 'processed_at': PROCESSED[doc_id], 'content_hash': 'synthetic-hash',
                           'chunk_count': 1, 'feedback_count': len(reports),
                           'open_feedback_count': sum(row['status'] == 'open' for row in reports)},
            'feedback': reports}


def final_answer(question):
    claim = 'The January statement lists a premium of $25.'
    quote = 'January statement: the premium is $25.'
    reference = {'span_id': 'span-101-0', 'evidence_id': 'doc-101-chunk-0', 'document_id': 101,
                 'chunk_index': 0, 'start': 0, 'end': len(quote), 'quote': quote,
                 'source_title': 'January premium statement'}
    return {'question': question, 'answer': claim + ' [Document 101](/documents/101)', 'mode': 'strict',
            'confidence': 0.8, 'cached': False, 'entities_found': [], 'follow_up_suggestions': [],
            'sources': [{'document_id': 101, 'title': 'January premium statement', 'doc_type': 'insurance',
                         'chunk_index': 0, 'excerpt': SOURCE_TEXT, 'date': '2026-01-31', 'similarity': 0.9}],
            'query_plan': {'mode': 'strict', 'intent': 'lookup', 'domain': 'insurance', 'requires_current': False},
            'verification': {'status': 'verified', 'supported_claims': [claim], 'unsupported_claims': [],
                             'stale_or_conflicting_claims': [], 'missing_evidence': []},
            'claim_ledger': {'complete': True, 'claims': [{'id': 'unit-0', 'claim': claim, 'start': 0,
                              'end': len(claim), 'status': 'supported', 'references': [reference],
                              'document_id': 101, 'source_title': 'January premium statement',
                              'evidence_ids': ['doc-101-chunk-0'], 'evidence_quote': quote,
                              'temporal_scope': 'historical'}],
                             'summary': {'supported': 1, 'unsupported': 0, 'conflicting': 0, 'missing': 0,
                                         'unchecked': 0, 'support_ratio': 1, 'audit_coverage': 1}},
            'source_summary': {'source_count': 1, 'trust_score': 0.8, 'trust_level': 'high',
                               'verification_status': 'verified', 'audit_status': 'claim_audited',
                               'claim_summary': {'supported': 1},
                               'trust_dimensions': {'claim_support': 1, 'audit_coverage': 1}},
            'evidence_pack': {'items': [{'id': 'doc-101-chunk-0', 'document_id': 101, 'chunk_index': 0,
                                       'title': 'January premium statement', 'excerpt': SOURCE_TEXT}],
                              'coverage': {'evidence_item_count': 1, 'source_document_count': 1}},
            'evidence': {'score': 0.8, 'level': 'high'},
            'finalization': {'status': 'supported', 'complete': True},
            'trace': [{'step': 'source_audit', 'status': 'ok', 'detail': 'Synthetic complete source support assessment'}],
            'timeline_events': []}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def payload(self):
        return json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))) or '{}')

    def reply(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PATCH,DELETE,OPTIONS')
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        route, params = parsed.path, parse_qs(parsed.query)
        get = lambda key, default='': params.get(key, [default])[0]
        if route == '/logs/stream':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            try:
                for _ in range(30):
                    self.wfile.write(b': heartbeat\n\n')
                    self.wfile.flush()
                    time.sleep(1)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        with LOCK:
            if route == '/_fixture':
                return self.reply({'fixture': 'paperless-accuracy-ui-v1'})
            if route == '/status':
                return self.reply({'status': 'healthy', 'graph': {'documents': len(DOCS), 'entities': 122, 'relationships': 3},
                                   'embeddings': {'document_chunks': len(DOCS), 'entity_embeddings': 122, 'docs_with_embeddings': len(DOCS)},
                                   'active_tasks': {}, 'cache': {}, 'last_sync': NOW})
            if route == '/config':
                return self.reply({'paperless_url': 'http://127.0.0.1:8000', 'owner_name': 'Synthetic fixture'})
            if route == '/entity-review/candidates':
                return self.reply({'candidates': [row for row in REVIEW_CANDIDATES if row['left']['uuid'] not in REVIEWED]})
            if route == '/logs':
                return self.reply({'lines': LOGS})
            if route == '/models':
                return self.reply({'models': [{'id': 'synthetic-model', 'name': 'Synthetic model'}], 'default': 'synthetic-model'})
            if route == '/conversations':
                return self.reply([{key: value for key, value in conv.items() if key != 'messages'} for conv in CONVERSATIONS.values()])
            if route.startswith('/conversations/'):
                return self.reply(CONVERSATIONS.get(route.rsplit('/', 1)[1], {'messages': []}))
            if route == '/documents':
                query, selected_type = get('q').lower().strip(), get('doc_type')
                if query == 'fixture-error':
                    return self.reply({'detail': 'Synthetic document search failure'}, 503)
                matches = [doc for doc in DOCS if query in (' '.join(str(v) for v in doc.values())).lower()]
                facets = {kind: sum(doc['doc_type'] == kind for doc in matches) for kind in {doc['doc_type'] for doc in matches}}
                matches = [doc for doc in matches if not selected_type or doc['doc_type'] == selected_type]
                sort = get('sort', 'title')
                matches.sort(key=lambda doc: (str(doc.get(sort, '')), doc['paperless_id']), reverse=get('direction') == 'desc')
                offset, limit = int(get('offset', '0')), int(get('limit', '25'))
                return self.reply({'scope': 'indexed_documents', 'total': len(matches), 'offset': offset, 'limit': limit,
                                   'has_more': offset + limit < len(matches), 'doc_types': facets,
                                   'results': [{'labels': ['Document'], 'properties': doc} for doc in matches[offset:offset + limit]]})
            if route == '/graph/initial' or route.startswith('/graph/neighbors/'):
                return self.reply({'nodes': NODES, 'relationships': RELS})
            if route == '/graph/search':
                query = get('q').lower().strip()
                if query == 'fixture-error':
                    return self.reply({'detail': 'Synthetic graph search failure'}, 503)
                rows = [{'labels': node['labels'], 'properties': node['props']} for node in SEARCH_NODES
                        if query in json.dumps(node).lower()]
                if get('type') == 'Document' and get('limit') == '12':
                    # Domain searches share a complete synthetic matching set;
                    # browser checks cover paging rather than domain relevance.
                    rows = [{'labels': ['Document'], 'properties': doc} for doc in DOCS]
                offset, limit = int(get('offset', '0')), int(get('limit', '50'))
                return self.reply({'total': len(rows), 'offset': offset, 'limit': limit,
                                   'has_more': offset + limit < len(rows), 'results': rows[offset:offset + limit]})
            if route.startswith('/graph/node/'):
                uid = route.rsplit('/', 1)[1]
                node = next((node for node in SEARCH_NODES if identity(node) == uid), None)
                if node is None and uid.startswith('doc-'):
                    doc = document(int(uid[4:]))
                    node = {'labels': ['Document'], 'props': doc} if doc else None
                if node is None:
                    return self.reply({'detail': 'Unknown synthetic node'}, 404)
                relationships = []
                for rel in RELS:
                    if uid not in (rel['start'], rel['end']):
                        continue
                    other = rel['end'] if rel['start'] == uid else rel['start']
                    neighbor = next(node for node in NODES if identity(node) == other)
                    relationships.append({'rel_type': rel['type'], 'rel_props': rel['props'],
                                          'direction': 'out' if rel['start'] == uid else 'in',
                                          'neighbor_labels': neighbor['labels'], 'neighbor_props': neighbor['props']})
                return self.reply({'labels': node['labels'], 'properties': node['props'], 'relationships': relationships})
            match = re.fullmatch(r'/document/(\d+)/(detail|feedback)', route)
            if match:
                doc_id = int(match[1])
                if not document(doc_id):
                    return self.reply({'detail': 'Unknown fixture document'}, 404)
                data = detail(doc_id)
                return self.reply(data if match[2] == 'detail' else {'feedback': data['feedback'], 'open_count': data['processing']['open_feedback_count']})
            if route.startswith('/task/'):
                task = TASKS.get(route.rsplit('/', 1)[1])
                if not task:
                    return self.reply({'detail': 'Unknown task'}, 404)
                if 'doc_id' in task:
                    PROCESSED[task['doc_id']] = timestamp()
                return self.reply({'status': 'completed', 'started': NOW, 'total_docs': 1, 'processed': 1, 'skipped': 0, 'errors': 0, 'elapsed_seconds': 1, 'current_doc': '', 'recent_results': [], 'result': task.get('result', {'processed': 1, 'skipped': 0, 'errors': 0})})
            return self.reply({'detail': 'Synthetic endpoint not implemented'}, 404)

    def do_POST(self):
        route = urlparse(self.path).path
        body = self.payload()
        if route == '/query/stream':
            return self.stream(body)
        with LOCK:
            if route == '/_fixture/reset':
                FEEDBACK.clear()
                CONVERSATIONS.clear()
                TASKS.clear()
                REVIEWED.clear()
                PROCESSED.update({doc['paperless_id']: NOW for doc in DOCS})
                return self.reply({'fixture': 'paperless-accuracy-ui-v1', 'status': 'reset'})
            if route in ('/entity-review/merge', '/entity-review/split', '/entity-review/ignore'):
                REVIEWED.add(body.get('left_uuid') or body.get('primary_uuid'))
                return self.reply({'status': 'completed'})
            if route in ('/sync', '/entity-review/steward/task'):
                task_id = f'synthetic-task-{len(TASKS) + 1}'
                TASKS[task_id] = {'result': {'processed': 1, 'errors': 0, 'reviewed_count': 3, 'suggest_review': 3}}
                return self.reply({'task_id': task_id, 'status': 'started', 'message': 'Synthetic task started'})
            if route == '/conversations':
                key = f'synthetic-conversation-{len(CONVERSATIONS) + 1}'
                CONVERSATIONS[key] = {'id': key, 'title': body.get('title', 'New conversation'), 'messages': [],
                                      'message_count': 0, 'last_message_at': NOW}
                return self.reply(CONVERSATIONS[key])
            if route == '/generate-title':
                return self.reply({'title': 'Synthetic accuracy check'})
            if route.startswith('/reindex/'):
                doc_id = int(route.rsplit('/', 1)[1])
                task_id = f'synthetic-task-{len(TASKS) + 1}'
                TASKS[task_id] = {'doc_id': doc_id}
                return self.reply({'task_id': task_id, 'status': 'started', 'message': 'Synthetic reindex started'})
            match = re.fullmatch(r'/document/(\d+)/feedback', route)
            if match:
                row = {'id': len(FEEDBACK) + 1, 'document_id': int(match[1]), 'reason': body['reason'],
                       'note': body.get('note', ''), 'status': 'open', 'created_at': timestamp()}
                FEEDBACK.append(row)
                return self.reply({'status': 'recorded', 'feedback': row})
            match = re.fullmatch(r'/document/(\d+)/feedback/(\d+)/resolve', route)
            if match:
                row = next((row for row in FEEDBACK if row['id'] == int(match[2]) and row['document_id'] == int(match[1])), None)
                if not row:
                    return self.reply({'detail': 'Review report not found'}, 404)
                if row['status'] != 'open':
                    return self.reply({'detail': 'Review already resolved'}, 409)
                if not body.get('note', '').strip():
                    return self.reply({'detail': 'Review note required'}, 422)
                if body['resolution'] == 'reindexed_and_reviewed' and PROCESSED[row['document_id']] <= row['created_at']:
                    return self.reply({'detail': 'Complete a successful reindex after this report, inspect the result, then resolve it'}, 409)
                row.update(status='resolved', resolution=body['resolution'], resolution_note=body['note'], resolved_at=timestamp())
                return self.reply({'status': 'resolved', 'feedback': row})
            if route == '/query':
                return self.reply(final_answer(body.get('question', '')))
            return self.reply({'detail': 'Synthetic endpoint not implemented'}, 404)

    def do_PATCH(self):
        key = urlparse(self.path).path.rsplit('/', 1)[1]
        body = self.payload()
        with LOCK:
            CONVERSATIONS[key]['title'] = body.get('title', 'Synthetic accuracy check')
            self.reply(CONVERSATIONS[key])

    def do_DELETE(self):
        key = urlparse(self.path).path.rsplit('/', 1)[1]
        with LOCK:
            CONVERSATIONS.pop(key, None)
            self.reply({'status': 'deleted'})

    def stream(self, body):
        question, conv_id = body.get('question', ''), body.get('conversation_id')
        with LOCK:
            conv = CONVERSATIONS.get(conv_id)
            if conv:
                conv['messages'].append({'role': 'user', 'content': question, 'timestamp': int(time.time() * 1000)})
                conv['message_count'] = len(conv['messages'])
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        def emit(event):
            self.wfile.write(('data: ' + json.dumps(event) + '\n\n').encode())
            self.wfile.flush()
        try:
            emit({'type': 'status', 'message': 'Synthetic source audit in progress'})
            time.sleep(0.15)
            if 'interrupt' in question.lower() or 'stream-error' in question.lower():
                # Deliberately emulate an old/provisional producer so clients
                # cannot promote draft tokens when completion never arrives.
                emit({'type': 'answer_chunk', 'content': 'DO_NOT_KEEP_UNSUPPORTED_$999'})
                emit({'type': 'answer_done', 'answer': 'DO_NOT_KEEP_UNSUPPORTED_$999'})
                time.sleep(0.15)
                if 'stream-error' in question.lower():
                    emit({'type': 'error', 'message': 'Synthetic verifier outage'})
                return
            result = final_answer(question)
            result['mode'] = body.get('mode', 'strict')
            if 'partial' in question.lower():
                claim = '- On January 31, 2026, the January statement lists a premium of $25. The amount is labeled as a premium.'
                result['answer'] = claim + ' [Document 101](/documents/101)'
                result['claim_ledger']['unitization'] = 'observations_v1'
                result['claim_ledger']['claims'][0].update(id='u1', claim=claim, start=0, end=len(claim))
                result['verification']['supported_claims'] = [claim]
                result['answer'] += '\n\nPartial answer: some claims could not be verified and were omitted. This does not answer every part of your question.'
                result['verification'].update(status='partial', partial={'original_total': 2, 'original_supported': 1, 'omitted_count': 1},
                                               missing_evidence=['Some claims were omitted because they could not be verified.'])
                result['finalization'].update(disposition='partial', complete=False, answer_verified=True)
                result['confidence'] = 0.65
                result['source_summary'].update(trust_score=0.65, trust_level='medium', verification_status='partial', audit_status='partial')
                result['evidence'].update(score=0.65, level='medium', audit_status='partial', coverage={'answer_complete': False})
            if result['mode'] == 'timeline':
                if 'many dates' in question.lower():
                    observations = [f'- Cedar invoice {i} records a service request on January {i}, 2026. The request does not confirm completed service.' for i in range(1, 9)]
                    result['claim_ledger']['unitization'] = 'observations_v1'
                else:
                    observations = [result['claim_ledger']['claims'][0]['claim']]
                candidate = '\n\n'.join(observations)
                claims = []
                offset = 0
                for index, claim in enumerate(observations):
                    quote = claim.removeprefix('- ')
                    ref = {'document_id':101, 'evidence_id':'ui-source', 'span_id':f'ui-span-{index}',
                           'source_title':'January premium statement', 'quote':quote, 'start':0, 'end':len(quote),
                           'content_digest':hashlib.sha256(quote.encode()).hexdigest()}
                    claims.append({'id':f'u{index+1}', 'start':offset, 'end':offset+len(claim),
                                   'claim':claim, 'status':'supported', 'references':[ref]})
                    offset += len(claim)+2
                ledger = result['claim_ledger']
                ledger.update(candidate_text=candidate, candidate_digest=hashlib.sha256(candidate.encode()).hexdigest(),
                              complete=True, claims=claims)
                ledger.setdefault('unitization','prose_v1')
                ledger['summary'].update(total=len(claims), audited=len(claims), supported=len(claims))
                partial = result['verification']['status'] == 'partial'
                result['answer'] = render_verified_answer(candidate, claims, partial=partial)
                result['finalization'].update(answer_verified=True, disposition='partial' if partial else 'supported',
                    answer_digest=hashlib.sha256(result['answer'].encode()).hexdigest(),
                    candidate_digest=ledger['candidate_digest'], documented_qualification=False)
                events, receipt = project_timeline(result['answer'], ledger, result['finalization'])
                result['timeline_events'] = events
                result['finalization']['timeline'] = receipt
            if 'markup' in question.lower():
                result['answer'] += '\n\nLiteral source markup: <img src=x onerror="window.__fixture_xss=1"> <script>window.__fixture_xss=2</script>'
            if 'coverage' in question.lower():
                unavailable = 'unavailable' in question.lower()
                partial_coverage = 'partial' in question.lower()
                result['finalization'].update(pipeline_version='question-evidence-v1',
                    question_coverage={'status': 'unavailable' if unavailable else 'partial' if partial_coverage else 'complete',
                        'complete': not unavailable and not partial_coverage, 'planning_status': 'complete',
                        'omitted_requested_aspects': None if unavailable else False,
                        'requirements': [
                            {'requirement_id': 'r1', 'aspect': 'The documented earlier amount', 'status': 'unavailable' if unavailable else 'answered'},
                            {'requirement_id': 'r2', 'aspect': 'The latest documented amount',
                             'status': 'unavailable' if unavailable else 'unresolved' if partial_coverage else 'answered'}]})
            with LOCK:
                if conv:
                    conv['messages'].append({'role': 'assistant', 'content': result['answer'],
                                             'timestamp': int(time.time() * 1000), **{key: value for key, value in result.items() if key != 'answer'}})
                    conv['message_count'] = len(conv['messages'])
            emit({'type': 'complete', **result})
        except (BrokenPipeError, ConnectionResetError):
            pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8485)
    args = parser.parse_args()
    print(f'Synthetic accuracy fixture listening on http://127.0.0.1:{args.port}', flush=True)
    ThreadingHTTPServer(('127.0.0.1', args.port), Handler).serve_forever()
