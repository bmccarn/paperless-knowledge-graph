#!/usr/bin/env python3
"""Offline source-level behavior checks: AST-loaded definitions, no app startup/I/O."""
import ast
import asyncio
import json
import logging
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[2]

def definitions(path, ns=None):
    ns = dict(ns or {})
    tree = ast.parse((ROOT / path).read_text(), filename=path)
    nodes = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Assign, ast.AnnAssign)):
            nodes.append(node)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            if not isinstance(node, ast.ImportFrom) or not (node.module or '').startswith('app'):
                nodes.append(node)
    exec(compile(ast.Module(nodes, type_ignores=[]), path, 'exec'), ns)
    return ns

def selected_class(path, class_name, methods, ns):
    tree = ast.parse((ROOT / path).read_text(), filename=path)
    original = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name)
    original.body = [n for n in original.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in methods]
    original.decorator_list = []
    exec(compile(ast.Module([original], type_ignores=[]), path, 'exec'), ns)
    return ns[class_name]

E = definitions('app/evidence.py')
Q = definitions('app/query_quality.py', {'evidence_exact_term_hits': E['exact_term_hits']})
ns = {**E, **Q, 'Any': Any, 'asyncio': asyncio, 'json': json, 'logger': logging.getLogger('offline')}
out = {}

# Unchecked references survive normalization and count as source-supported claims.
bad_claim = {'claim': 'Premium is $999.', 'support_status': 'supported',
             'document_id': 999999, 'evidence_id': 'NOT-IN-PACK', 'evidence_excerpt': 'FABRICATED QUOTE'}
ledger = E['normalize_claim_ledger']({'claims': [bad_claim]})
out['unvalidated_ledger_reference'] = ledger
assert ledger['claims'][0]['evidence_id'] == 'NOT-IN-PACK'
assert ledger['summary']['supported'] == 1

# The actual supporting text can be in a selected chunk but absent from its excerpt.
tail_pack = E['build_evidence_pack']('What is the premium?', {}, [{'document_id': 1, 'chunk_index': 0, 'title': 'Policy', 'content': 'x' * 1300 + ' PREMIUM_IS_321'}], [{'document_id': 1}])
formatted = E['format_evidence_pack_for_llm'](tail_pack)
out['selected_chunk_evidence_tail_lost'] = {'full_item_contains_fact': 'PREMIUM_IS_321' in tail_pack['items'][0]['content'], 'serialized_evidence_contains_fact': 'PREMIUM_IS_321' in formatted}
assert out['selected_chunk_evidence_tail_lost']['full_item_contains_fact']
assert not out['selected_chunk_evidence_tail_lost']['serialized_evidence_contains_fact']

# A contradictory verifier/ledger result is promoted to high claim support.
sources = [{'document_id': i, 'date': '2020-01-01', 'title': 'Insurance policy premium', 'excerpt': 'insurance policy premium'} for i in range(1, 6)]
coverage = {'average_source_quality': .95, 'structured_fact_count': 20, 'date_signal_counts': {'expiration_date': 1}}
unsupported = E['normalize_claim_ledger']({'claims': [{'claim': 'Premium is $999.', 'support_status': 'unsupported'}]})
grade = Q['compute_evidence_grade']('What is my insurance policy premium?', {'requires_current': True}, sources, {}, {'status': 'verified'}, {'coverage': coverage}, unsupported)
out['contradictory_audits'] = grade
assert grade['dimensions']['claim_support'] == .9
assert grade['level'] == 'high'

# Current-state metadata resolves an explicitly expired, very old source.
expired = [{'document_id': 1, 'date': '2001-01-01', 'title': 'Expired policy', 'excerpt': 'This insurance policy expired in 2002.'}]
current = Q['current_state_summary']({'requires_current': True}, expired)
out['expired_current_state'] = current
assert current['status'] == 'resolved' and current['superseded_source_count'] == 1

Query = selected_class('app/query.py', 'QueryEngine', {'_verify_repair_and_grade', '_append_evidence_limits', '_blend_confidence', '_extract_timeline_events'}, ns)

class FakeAudit:
    def __init__(self, verification, ledger=None, timeline=None):
        self.verification = verification
        self.ledger = ledger
        self.timeline = timeline
        self.repair_calls = 0
    async def verify_answer(self, *args):
        return self.verification
    async def extract_claim_ledger(self, *args):
        return self.ledger
    async def repair_answer(self, *args):
        self.repair_calls += 1
        return None
    async def extract_timeline(self, *args):
        return self.timeline

async def run():
    query = Query()
    async def empty_pack(*args, **kwargs):
        return {'items': [], 'coverage': {}, 'source_documents': []}
    async def empty_retrieve(*args):
        return {}
    query._build_evidence_pack = empty_pack
    query._retrieve = empty_retrieve
    query._merge_context = lambda a, b: {}
    query._build_sources = lambda *args, **kwargs: []
    query._format_doc_context = lambda *args, **kwargs: ''
    query._format_graph_context = lambda *args, **kwargs: ''
    answer = 'Your current insurance premium is $999.'
    ns['strands_orchestrator'] = FakeAudit(None)
    result = await query._verify_repair_and_grade('What is my premium?', answer, {}, [], {}, 'strict')
    out['strict_empty_evidence_verifier_unavailable'] = {'answer': result[0], 'verification': result[1], 'trust': result[2]['score']}
    assert result[0] == answer and result[1]['status'] == 'not_run'

    # Missing-evidence-only verifier results do not invoke repair or append limits.
    audit = FakeAudit({'status': 'needs_review', 'missing_evidence': ['No policy present.'], 'unsupported_claims': [], 'stale_or_conflicting_claims': []})
    ns['strands_orchestrator'] = audit
    result = await query._verify_repair_and_grade('What is my premium?', answer, {}, [], {}, 'strict')
    out['missing_evidence_without_repair'] = {'answer': result[0], 'repair_calls': audit.repair_calls}
    assert result[0] == answer and audit.repair_calls == 0

    # New unsupported ledger findings arrive after the last repair decision.
    audit = FakeAudit({'status': 'verified'}, {'claims': [{'claim': answer, 'support_status': 'unsupported'}]})
    ns['strands_orchestrator'] = audit
    result = await query._verify_repair_and_grade('What is my premium?', answer, {}, [], {}, 'strict')
    out['unsupported_ledger_after_repair'] = {'answer': result[0], 'repair_calls': audit.repair_calls, 'ledger': result[4]}
    assert result[0] == answer and audit.repair_calls == 0

    # Timeline records accept unknown documents and dates without validation.
    ns['strands_orchestrator'] = FakeAudit(None, timeline=[{'date': '2099-02-31', 'title': 'Invented event', 'document_id': 999999}])
    timeline, trace = await query._extract_timeline_events('History?', {}, [], 'timeline')
    out['unvalidated_timeline'] = {'events': timeline, 'trace': trace}
    assert timeline[0]['document_id'] == 999999 and trace[0]['status'] == 'ok'

    # Capture the actual verifier prompt without invoking any model.
    ons = {'Any': Any, 'json': json}
    Orchestrator = selected_class('app/strands_orchestrator.py', 'StrandsQueryOrchestrator', {'verify_answer'}, ons)
    orchestrator = Orchestrator()
    orchestrator.enabled = True
    captured = {}
    async def capture(**kwargs):
        captured.update(kwargs)
        return {'status': 'verified'}
    orchestrator._json_agent = capture
    await orchestrator.verify_answer('Question', 'x' * 6001 + ' UNSUPPORTED_TAIL_CLAIM', [], 'y' * 8001 + ' CRITICAL_EVIDENCE_TAIL', {})
    out['verifier_truncation'] = {'answer_tail_present': 'UNSUPPORTED_TAIL_CLAIM' in captured['prompt'], 'evidence_tail_present': 'CRITICAL_EVIDENCE_TAIL' in captured['prompt']}
    assert not any(out['verifier_truncation'].values())

    # Exercise actual extraction prompt construction with a capture-only client.
    extraction_ns = {'Any': Any, 'json': json}
    extract_tree = ast.parse((ROOT / 'app/extractor.py').read_text())
    wanted = {'METADATA_EXTRACTION_PROMPTS', 'GENERIC_METADATA_PROMPT', 'ENTITY_EXTRACTION_PROMPT', 'RELATIONSHIP_EXTRACTION_PROMPT'}
    assignments = [n for n in extract_tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in wanted for t in n.targets)]
    exec(compile(ast.Module(assignments, type_ignores=[]), 'app/extractor.py', 'exec'), extraction_ns)
    prompts = []
    async def fake_create(**kwargs):
        prompts.append(kwargs['messages'][0]['content'])
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='{}'))])
    async def fake_retry(fn, **kwargs):
        return await fn()
    extraction_ns['_repair_json'] = json.loads
    extraction_ns['_extract_json_with_retry'] = fake_retry
    ExtractPrompts = selected_class('app/extractor.py', 'EntityExtractor', {'_pass1_metadata_extraction', '_pass2_entity_extraction', '_pass3_relationship_extraction'}, extraction_ns)
    extractor = ExtractPrompts()
    extractor.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=fake_create)))
    extractor.model = 'capture-only'
    text = 'x' * 31000 + ' ONLY_RELEVANT_FACT_AT_TAIL'
    await extractor._pass1_metadata_extraction('Long document', text, 'generic')
    await extractor._pass2_entity_extraction('Long document', text, {})
    await extractor._pass3_relationship_extraction('Long document', text, {'entities': []})
    out['extraction_tail_omitted'] = {'tail_present_by_pass': ['ONLY_RELEVANT_FACT_AT_TAIL' in p for p in prompts], 'scope': 'actual prompt constructors with capture-only completion client'}
    assert not any(out['extraction_tail_omitted']['tail_present_by_pass'])

    # Documented split decision cannot veto the merger: the method never reads it.
    rns = {'logger': logging.getLogger('offline'), 'advanced_match_score': lambda *a: 1.0,
           'should_auto_merge': lambda *a: True, 'pick_canonical_name': lambda a, b: a,
           'LLM_TIEBREAKER_LOW': .6}
    Resolver = selected_class('app/entity_resolver.py', 'EntityResolver', {'resolve_all_entities'}, rns)
    calls = []
    async def persons():
        return [{'uuid': 'left', 'name': 'John Smith'}, {'uuid': 'right', 'name': 'John Smith'}]
    async def orgs():
        return []
    async def decisions():
        calls.append('read_decisions')
        return [{'left_uuid': 'left', 'right_uuid': 'right', 'decision': 'split'}]
    async def merge(**kwargs):
        calls.append({'merge': kwargs})
    rns['graph_store'] = SimpleNamespace(get_all_persons=persons, get_all_organizations=orgs)
    rns['embeddings_store'] = SimpleNamespace(get_entity_review_decisions=decisions)
    resolver = Resolver()
    resolver._merge_nodes = merge
    report = await resolver.resolve_all_entities()
    out['split_decision_ignored'] = {'decisions_read': 'read_decisions' in calls, 'merged': report['total_merged'], 'scope': 'actual merger orchestration; deterministic eligible scorer and in-memory stores'}
    assert report['total_merged'] == 1 and 'read_decisions' not in calls

asyncio.run(run())

# Extraction can reintroduce a relationship endpoint absent from verified entities.
ens = {'Any': Any, 'logger': logging.getLogger('offline')}
tree = ast.parse((ROOT / 'app/extractor.py').read_text())
coerce = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_coerce_text')
exec(compile(ast.Module([coerce], type_ignores=[]), 'app/extractor.py', 'exec'), ens)
Extractor = selected_class('app/extractor.py', 'EntityExtractor', {'_combine_results', '_find_entity_type'}, ens)
combined = Extractor()._combine_results({}, {'entities': [{'name': 'Alice Example', 'type': 'Person'}]}, {'relationships': [{'from_entity': 'Alice Example', 'to_entity': 'Fabricated Person', 'relationship_type': 'WORKS_WITH', 'confidence': .99}]})
out['unchecked_relationship_endpoint'] = combined['implied_relationships']
assert combined['implied_relationships'][0]['to_entity'] == 'Fabricated Person'

# Steward thresholds are bypassed by the final return.
sns = {'Any': Any}
tree = ast.parse((ROOT / 'app/entity_steward.py').read_text())
chooser = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'choose_recommendation')
exec(compile(ast.Module([chooser], type_ignores=[]), 'app/entity_steward.py', 'exec'), sns)
rec = sns['choose_recommendation']({'recommendation': 'review', 'risk': 'high'}, {'recommendation': 'merge', 'confidence': .01, 'risk': 'high'})
out['steward_threshold_bypass'] = rec
assert rec == 'merge'

# Canonical evals can pass with empty sources and an unaudited, fabricated answer.
eval_ns = definitions('scripts/eval_harness.py')
cases = json.loads((ROOT / 'evals/canonical_questions.json').read_text())
passes = []
for case in cases:
    forged = {'answer': ' '.join(case.get('required_terms', [])), 'sources': [], 'confidence': .9,
              'source_summary': {'trust_score': .6, 'verification_status': 'not_run'},
              'trace': [{'anything': True}], 'timeline_events': [{'date': '2099-02-31'}]}
    passes.append(eval_ns['score_case'](case, forged))
out['canonical_evals_accept_fabricated_zero_source_response'] = {'passed': sum(r['passed'] for r in passes), 'total': len(passes)}
assert all(r['passed'] for r in passes)

print(json.dumps(out, indent=2))
