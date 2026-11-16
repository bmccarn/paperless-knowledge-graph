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
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import threading
import time
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from app.answer_delivery import render_verified_answer
from app.timeline import project_timeline
from app.relationship_families import classify_relationship

NODES = [
    {'labels':['Person'], 'props':{'uuid':'person-1','name':'Alex Example','description':'Synthetic fixture. <b>This is literal source text.</b>'}},
    {'labels':['Organization'], 'props':{'uuid':'org-1','name':'Example Utility','description':'Organization appearing in two example documents.'}},
    {'labels':['Document'], 'props':{'paperless_id':101,'title':'Utility statement — January','date':'2026-01-31','doc_type':'financial_statement'}},
    {'labels':['Document'], 'props':{'paperless_id':102,'title':'Utility statement — February','date':'2026-02-28','doc_type':'financial_statement'}},
]
RELS = [
    {'start':'person-1','end':'org-1','type':'CUSTOMER_OF','props':{'source_doc':101,'weight':1,'implied':True}},
    {'start':'doc-101','end':'org-1','type':'INVOICED_BY','props':{'source_doc':101,'weight':1}},
    {'start':'doc-102','end':'org-1','type':'INVOICED_BY','props':{'source_doc':102,'weight':1}},
]
def identity(node):
    return node['props'].get('uuid') or f"doc-{node['props']['paperless_id']}"
RELATIONSHIP_QUOTE = 'Alex is a customer. <img src=x onerror="window.__relationship_xss=1">'
RELS[0]['props'].update(source_doc_ids=[101], inferred=True, support_records=[json.dumps({
    'source_doc': 101, 'inferred': True,
    'evidence_json': json.dumps([{'start': 0, 'end': len(RELATIONSHIP_QUOTE), 'quote': RELATIONSHIP_QUOTE}]),
    'rationale': ['The source names Alex as the customer of Example Utility.'],
})])

# The neighborhood graph serves only the summary node route, so the sample graph and search stay unchanged.
# Rowan Vale is the owner and a hub (more than 100 links outside evidence) across nine families.
HUB = 'nb-rowan'
NB_ENTITIES = {HUB: ('Person', 'Rowan Vale')}
NB_LINKS = []  # (start, type, end, [(doc_id, inferred, confidence)])
_nb_doc = iter(range(9030, 9300))


def nb_link(start, rel_type, end, documents=1, inferred=(), confidence=0.9):
    NB_LINKS.append((start, rel_type, end, [(doc, index in inferred, confidence)
                                            for index, doc in enumerate(next(_nb_doc) for _ in range(documents))]))


for family, rel_type, label, named, fillers in [
    ('parties', 'EMPLOYED_BY', 'Organization', [('Marlow Freight Co.', 9), ('Juniper Okafor', 7, 'Person', 'SIGNED_BY')], 22),
    ('money', 'PAID_TO', 'FinancialItem', [('Harbor Mutual Insurance', 8, 'Organization', 'PREMIUM_PAID_TO'),
                                           ('Checking account 4417', 6)], 20),
    ('coverage', 'COVERS', 'InsurancePolicy', [('Policy HM-2208 (auto)', 6), ('Policy HO-5130 (home)', 4)], 8),
    ('health', 'DIAGNOSED_WITH', 'Condition', [('Hypertension', 5), ('Lipid panel, March 2026', 3, 'MedicalResult', 'ORDERED_BY')], 8),
    ('service', 'FILED_WITH', 'Organization', [('County Tax Office', 5), ('Port Ellery Library', 2, 'Organization', 'PARTICIPATED_IN')], 6),
    ('places', 'LIVES_IN', 'Address', [('14 Larkspur Ct', 6), ('Port Ellery', 4, 'Location', 'LOCATED_IN')], 6),
    ('time', 'EFFECTIVE_FROM', 'DateEvent', [('1 Apr 2026', 3), ('1 Oct 2026', 2, 'DateEvent', 'EXPIRES_ON')], 4),
    ('loose', 'RELATED_TO', 'Person', [('Avery Brooks', 3), ('Riverside 10K', 2, 'Event', 'ASSOCIATED_WITH')], 6),
    ('other', 'OWNS_VEHICLE', 'Product', [('2019 Subaru Outback', 4), ('Telematics plug TX-2', 2, 'System', 'USES_DEVICE')], 6),
]:
    for index, (name, documents, *custom) in enumerate(named):
        uid = f'nb-{family}-{index}'
        NB_ENTITIES[uid] = (custom[0] if custom else label, name)
        rel = custom[1] if custom else rel_type
        assert classify_relationship(rel)[0] == family, rel
        if name == 'Policy HM-2208 (auto)':
            nb_link(HUB, rel, uid, documents, inferred={0})  # mixed: stated in 5, inferred in 1
        elif name == 'Avery Brooks':
            nb_link(HUB, rel, uid, documents, inferred=range(documents))  # inferred in every record
        elif name == 'Riverside 10K':
            nb_link(HUB, rel, uid, documents, confidence=None)  # no score
        else:
            nb_link(HUB, rel, uid, documents)
    for index in range(fillers):
        uid = f'nb-{family}-filler-{index:02d}'
        NB_ENTITIES[uid] = (label, f'{family.capitalize()} fixture {index + 1:02d}')
        nb_link(HUB, rel_type, uid)
# The two-family neighbor: 2 documents as a party, 8 in money, so it sits in money.
nb_link('nb-money-0', 'ISSUED_BY', HUB, 2)
# Documents are evidence or a document hop, never drawn.
NB_LINKS.extend((f'doc-{doc}', 'MENTIONS', HUB, [(doc, False, None)]) for doc in range(9000, 9012))
NB_LINKS.append(('doc-9012', 'PREPARED_BY', HUB, [(9012, False, 0.8)]))
# A leaf with only mentions, and an entity with many mentions and few links (not a hub).
NB_ENTITIES.update({'nb-leaf': ('Product', 'Spare key tag'), 'nb-pharmacy': ('Organization', 'Lakeshore Pharmacy')})
NB_LINKS.extend((f'doc-{doc}', 'MENTIONS', 'nb-leaf', [(doc, False, None)]) for doc in (9020, 9021))
NB_LINKS.extend((f'doc-{doc}', 'MENTIONS', 'nb-pharmacy', [(doc, False, None)]) for doc in range(9100, 9220))
for rel_type, target in [('PRESCRIBED_BY', 'nb-health-0'), ('LOCATED_IN', 'nb-places-1'), ('PAID_TO', 'nb-money-1'),
                         ('FILED_WITH', 'nb-service-0'), ('RELATED_TO', 'nb-loose-0')]:
    nb_link('nb-pharmacy', rel_type, target)
# A small focus whose neighbors have a name with markup characters, no name, and a label outside the kinds.
NB_ENTITIES.update({'nb-desk': ('Organization', 'Front desk'), 'nb-markup': ('Organization', '<b>Acme</b> & "Sons" <i>'),
                    'nb-unnamed': ('Organization', ''), 'nb-gadget': ('Gadget', 'Loaner gadget')})
for rel_type, target in [('PAID_TO', 'nb-markup'), ('EMPLOYED_BY', 'nb-unnamed'), ('OWNS_VEHICLE', 'nb-gadget')]:
    nb_link('nb-desk', rel_type, target, 2)
# A renewal notice whose one link is stated in one document and inferred in another.
NB_ENTITIES.update({'nb-renewal': ('Contract', 'Renewal notice 2026'), 'nb-car': ('Product', '2014 Honda Fit')})
nb_link('nb-renewal', 'COVERS', 'nb-car', 2, inferred={1})
# Links between the hub's drawn neighbors, for the curves in Focus: one inferred, one parallel pair, and one link to a
# member of the parties pill, which is never drawn. Only GET /graph/subgraph reads them, so the summaries, document
# counts and search order of every other page stay as they were; their document ids come after every other nb_link.
NB_CROSS = []
for start, rel_type, end, documents, inferred in [
    ('nb-parties-1', 'EMPLOYED_BY', 'nb-parties-0', 4, ()), ('nb-parties-1', 'RELATED_TO', 'nb-parties-0', 1, ()),
    ('nb-coverage-0', 'ISSUED_BY', 'nb-money-0', 5, ()), ('nb-money-1', 'PAID_TO', 'nb-money-0', 3, ()),
    ('nb-coverage-0', 'COVERS', 'nb-other-0', 3, ()), ('nb-coverage-0', 'EFFECTIVE_FROM', 'nb-time-0', 2, ()),
    ('nb-coverage-0', 'EXPIRES_ON', 'nb-time-1', 1, ()), ('nb-places-0', 'LOCATED_IN', 'nb-places-1', 2, ()),
    ('nb-service-0', 'LOCATED_IN', 'nb-places-1', 1, ()), ('nb-service-1', 'LOCATED_IN', 'nb-places-1', 1, ()),
    ('nb-other-1', 'USES_DEVICE', 'nb-other-0', 1, {0}), ('nb-health-1', 'RELATED_TO', 'nb-health-0', 1, {0}),
    ('nb-loose-0', 'PARTICIPATED_IN', 'nb-loose-1', 2, ()), ('nb-parties-filler-00', 'EMPLOYED_BY', 'nb-parties-0', 1, ()),
]:
    NB_CROSS.append((start, rel_type, end, [(next(_nb_doc), index in inferred, 0.9) for index in range(documents)]))


# An island for Explore that no other entity links to, so every other page keeps its counts. The policy has 10
# links, 8 on the start ring; the car has 8 new neighbors (one unnamed) and loops back to the holder; the loan leads
# to hop 3; the insurer links to four same-named address records; shop, DMV and holder link to the town, which only
# GET /graph/subgraph reveals. The landing search leaves these ids out. Documents 9262 to 9300 are in no other graph.
EX_DOCS = list(range(9262, 9301))
NB_ENTITIES.update({
    'ex-policy': ('InsurancePolicy', 'Policy KT-9030 (auto)'), 'ex-insurer': ('Organization', 'Bayview Mutual'),
    'ex-holder': ('Person', 'Lena Ortiz'), 'ex-car': ('Product', '2021 Mazda CX-5'),
    'ex-premium': ('FinancialItem', 'Premium, May 2026, $588'), 'ex-town': ('Location', 'Calder Bay'),
    'ex-prior': ('InsurancePolicy', 'Policy KT-8841 (2025)'), 'ex-start': ('DateEvent', '1 Jun 2026'),
    'ex-end': ('DateEvent', '1 Dec 2026'), 'ex-agent': ('Person', 'Owen Pratt'),
    'ex-bank': ('Organization', 'Tidewater Credit Union'), 'ex-loan': ('FinancialItem', 'Car loan CL-7710'),
    'ex-shop': ('Organization', 'Bayline Auto Body'), 'ex-dmv': ('Organization', 'DMV, Calder Bay office'),
    'ex-recall': ('Event', 'Recall notice, 2025'), 'ex-dealer': ('Organization', 'Northshore Mazda'),
    'ex-tires': ('Product', 'Set of tires, 2026'), 'ex-plate': ('FinancialItem', ''),
    'ex-servicer': ('Organization', 'Saltmarsh Lending'), 'ex-payment': ('FinancialItem', 'Loan payment, Oct 2026'),
    'ex-payoff': ('DateEvent', 'Loan payoff, 2029'), 'ex-hq': ('Address', '12 Wharf Rd'),
    **{f'ex-pier-{index}': ('Address', '40 Pier St') for index in range(1, 5)},
})
for start, rel_type, end, documents, inferred in [
    ('ex-policy', 'ISSUED_BY', 'ex-insurer', range(0, 6), ()), ('ex-policy', 'PARTY_TO', 'ex-holder', range(0, 5), ()),
    ('ex-policy', 'COVERS', 'ex-car', (0, 1, 2, 6), ()), ('ex-premium', 'APPLIES_TO', 'ex-policy', (3, 4, 7), ()),
    ('ex-policy', 'GARAGED_IN', 'ex-town', (0, 1), ()), ('ex-policy', 'RELATED_TO', 'ex-prior', (8, 9), ()),
    ('ex-policy', 'EFFECTIVE_FROM', 'ex-start', (0, 1), ()), ('ex-policy', 'EXPIRES_ON', 'ex-end', (0,), ()),
    ('ex-policy', 'SIGNED_BY', 'ex-agent', (2,), ()), ('ex-policy', 'ASSOCIATED_WITH', 'ex-bank', (6,), (0,)),
    ('ex-car', 'LOAN_FROM', 'ex-loan', range(10, 15), ()), ('ex-car', 'SERVICED_BY', 'ex-shop', (15, 16, 17), ()),
    ('ex-car', 'FILED_WITH', 'ex-dmv', (18, 19), ()), ('ex-car', 'SUBJECT_OF', 'ex-recall', (20, 21), (0, 1)),
    ('ex-holder', 'OWNER_OF', 'ex-car', (2, 6), ()), ('ex-car', 'ASSOCIATED_WITH', 'ex-bank', (6, 10), ()),
    ('ex-car', 'CONTRACTED_WITH', 'ex-dealer', (22,), ()), ('ex-car', 'RELATED_TO', 'ex-tires', (23,), ()),
    ('ex-car', 'RELATED_TO', 'ex-plate', (24,), ()),
    ('ex-loan', 'PAID_TO', 'ex-servicer', (25, 26, 27), ()), ('ex-loan', 'LOAN_FROM', 'ex-bank', (10,), ()),
    ('ex-payment', 'PAYMENT_FOR', 'ex-loan', (28,), ()), ('ex-loan', 'EXPIRES_ON', 'ex-payoff', (29, 30), ()),
    ('ex-servicer', 'LOCATED_AT', 'ex-hq', (31,), ()),
    ('ex-shop', 'LOCATED_IN', 'ex-town', (32,), ()), ('ex-dmv', 'LOCATED_IN', 'ex-town', (33, 34), ()),
    ('ex-holder', 'RESIDES_AT', 'ex-town', (35,), ()), ('ex-insurer', 'ISSUED_BY', 'ex-prior', (8,), ()),
    *[('ex-insurer', 'LOCATED_AT', f'ex-pier-{index}', (36 + index % 3,), ()) for index in range(1, 5)],
]:
    NB_LINKS.append((start, rel_type, end, [(EX_DOCS[doc], index in inferred, 0.9) for index, doc in enumerate(documents)]))

# The sample person and organization also open as a neighborhood, for the inspector's source checks.
NB_ENTITIES.update({'person-1': ('Person', 'Alex Example'), 'org-1': ('Organization', 'Example Utility')})
NB_LINKS.append(('person-1', 'CUSTOMER_OF', 'org-1', [(101, True, None)]))
NB_LINKS.extend((f'doc-{doc}', 'INVOICED_BY', 'org-1', [(doc, False, None)]) for doc in (101, 102))

# Two more Persons match the owner setting, as extraction duplicates do; the rule chose Rowan Vale (nb-rowan).
# The first is linked to the owner through documents the owner already has, so the evidence shelf is unchanged.
OWNER_MATCHES = {'nb-rowan-dup-1': range(9230, 9244), 'nb-rowan-dup-2': range(9250, 9261)}
for uid, docs in OWNER_MATCHES.items():
    NB_ENTITIES[uid] = ('Person', 'Rowan Vale')
    NB_LINKS.extend((f'doc-{doc}', 'MENTIONS', uid, [(doc, False, None)]) for doc in docs)
NB_LINKS.append((HUB, 'RELATED_TO', 'nb-rowan-dup-1', [(doc, False, 0.9) for doc in range(9000, 9004)]))


def nb_node(uid):
    if uid.startswith('doc-'):
        doc = document(int(uid[4:]))
        return 'Document', doc['title']
    return NB_ENTITIES[uid]


def nb_fields(uid):
    rows = [row for row in NB_LINKS if uid in (row[0], row[2])]
    links = sum(classify_relationship(row[1])[0] != 'evidence' for row in rows)
    return {'evidence_doc_count': len({doc for row in rows for doc, _, _ in row[3]}), 'link_count': links,
            'is_hub': links > 100, 'is_owner': uid == HUB}


def nb_summary(uid):
    """The shape of GET /graph/node/{id}?summary=1 for the synthetic neighborhood graph."""
    label, name = NB_ENTITIES[uid]
    links, documents = [], set()
    for start, rel_type, end, records in NB_LINKS:
        if uid not in (start, end):
            continue
        other = end if start == uid else start
        inferred = sum(flag for _, flag, _ in records)
        documents.update(doc for doc, _, _ in records)
        neighbor_label, neighbor_name = nb_node(other)
        links.append({
            'key': f'{start}~{rel_type}~{end}', 'type': rel_type, 'family': classify_relationship(rel_type)[0],
            'direction': 'out' if start == uid else 'in',
            'support_state': 'supported' if not inferred else 'inferred' if inferred == len(records) else 'mixed',
            'stated_count': len(records) - inferred, 'inferred_count': inferred,
            'records': [{'doc_id': doc, 'inferred': flag, 'confidence': score} for doc, flag, score in records],
            'neighbor': {'id': other, 'label': neighbor_label, 'name': neighbor_name,
                         **{k: v for k, v in nb_fields(other).items() if k != 'is_hub'}}})
    return {'focus': {'id': uid, 'label': label, 'name': name, 'aliases': [], **nb_fields(uid)},
            'links': sorted(links, key=lambda link: link['key']),
            'documents': {str(doc): {key: document(doc)[key] for key in ('title', 'date', 'doc_type')} for doc in sorted(documents)},
            'owner_status': 'chosen',
            'owner_duplicates': [{'id': other, 'label': 'Person', 'name': NB_ENTITIES[other][1],
                                  'evidence_doc_count': nb_fields(other)['evidence_doc_count']}
                                 for other in OWNER_MATCHES] if uid == HUB else []}


def nb_paths(a, b, max_len, k, include_owner, allow_inferred, candidates=1000):
    """GET /graph/paths over the neighborhood graph, by the backend's rules: semantic links between entities, every
    match of the owner setting left out except an endpoint, shorter lengths first, at most `candidates` paths checked."""
    if a not in NB_ENTITIES or b not in NB_ENTITIES:
        return {'paths': [], 'exhausted': True, 'nodes': {}, 'links_detail': {}}
    blocked = set() if include_owner else ({HUB, *OWNER_MATCHES} - {a, b})
    edges = {}
    for start, rel_type, end, records in NB_LINKS + NB_CROSS:
        if (start.startswith('doc-') or end.startswith('doc-') or classify_relationship(rel_type)[0] == 'evidence'
                or (not allow_inferred and all(flag for _, flag, _ in records))):
            continue
        for here, there in ((start, end), (end, start)):
            edges.setdefault(here, []).append((there, f'{start}~{rel_type}~{end}', start, rel_type, end, records))
    found, checked = [], 0
    for length in range(1, max_len + 1):
        stack = [(a, [a], [])]
        while stack and checked < candidates:
            node, nodes, links = stack.pop()
            if len(links) == length:
                if node == b:
                    checked += 1
                    found.append((nodes, links))
                continue
            for there, *link in sorted(edges.get(node, []), key=lambda edge: edge[1], reverse=True):
                if there not in nodes and there not in blocked and (there == b) == (len(links) + 1 == length):
                    stack.append((there, nodes + [there], links + [link]))
        if checked >= candidates:
            break
    rank = lambda path: (-min(len(link[4]) for link in path[1]), len(path[1]),
                         sum(all(f for _, f, _ in link[4]) for link in path[1]), [link[0] for link in path[1]])
    found = sorted(found, key=rank)[:k]
    ids = {node for nodes, _ in found for node in nodes} or {a, b}
    support = lambda records: {'support_state': 'supported' if not any(f for _, f, _ in records) else 'inferred' if all(f for _, f, _ in records) else 'mixed',
                               'stated_count': sum(not f for _, f, _ in records), 'inferred_count': sum(f for _, f, _ in records)}
    return {
        'paths': [{'nodes': nodes, 'links': [link[0] for link in links], 'length': len(links),
                   'weakest_weight': min(len(link[4]) for link in links),
                   'has_inferred': any(all(f for _, f, _ in link[4]) for link in links)} for nodes, links in found],
        'exhausted': checked < candidates,
        'nodes': {uid: {'id': uid, 'label': NB_ENTITIES[uid][0], 'name': NB_ENTITIES[uid][1], **nb_fields(uid)} for uid in sorted(ids)},
        'links_detail': {key: {'source': start, 'type': rel_type, 'target': end, 'family': classify_relationship(rel_type)[0],
                               'weight': len(records), **support(records)}
                         for _, links in found for key, start, rel_type, end, records in links},
    }


def overview_archive():
    """GET /graph/overview for a synthetic archive shaped like a real one: few links between entities, hubs whose
    neighbors do not link to each other, near-duplicate copies around one organization, records without a name, pairs
    and triples, and entities linked only to the owner. Only this route reads it, so search and the other pages keep
    their data. Four group leads are neighborhood ids, so "Open in Focus" resolves."""
    state = [20261115]

    def random():
        state[0] = (state[0] * 1664525 + 1013904223) % 4294967296
        return state[0] / 4294967296

    def uid(seed):
        digest = hashlib.sha256(f'overview-{seed}'.encode()).hexdigest()
        return f'{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}'
    nodes, pairs = {}, {}
    family = {name: bit for bit, name in enumerate(
        ['parties', 'money', 'coverage', 'health', 'service', 'places', 'time', 'doclinks', 'loose', 'other'])}

    def node(key, label, name, documents=None):
        if key not in nodes:
            nodes[key] = [key if key in NB_ENTITIES else uid(key), label, name,
                          documents if documents is not None else 1 + int(random() ** 3 * 6)]
        return key

    def link(a, b, families, weight=None, support=0):
        pair = tuple(sorted((a, b)))
        weight = weight if weight is not None else 1 + int(random() * 3)
        old = pairs.get(pair)
        mask = sum(1 << family[f] for f in families)
        pairs[pair] = [weight, support, mask] if old is None else [old[0] + weight, 1 if old[1] != support else support, old[2] | mask]
    owner = node(HUB, 'Person', 'Rowan Vale', nb_fields(HUB)['evidence_doc_count'])
    groups = [
        # lead key, label, name, member kinds (label, name or '' for a record without a name, families)
        ('nb-money-0', 'Organization', 'Harbor Mutual Insurance', 46, [
            ('InsurancePolicy', 'Policy HM-{n} (auto)', ['coverage']), ('FinancialItem', '', ['money']),
            ('Product', 'Vehicle {n}', ['coverage']), ('Person', 'Claims adjuster {n}', ['parties'])]),
        ('nb-parties-0', 'Organization', 'Marlow Freight Co.', 38, [
            ('FinancialItem', '', ['money']), ('Person', 'Dispatcher {n}', ['parties']), ('Event', 'Shift review {n}', ['time'])]),
        ('ov-bayline', 'Organization', 'Bayline Property Group', 34, [('Address', '2 Harbor Way', ['places'])]),
        ('nb-service-0', 'Organization', 'County Tax Office', 24, [
            ('FinancialItem', 'Tax bill {n}', ['money']), ('DateEvent', 'Due {n} Apr', ['time']), ('Contract', 'Assessment {n}', ['service'])]),
        ('ov-clinic', 'Organization', 'Port Ellery Clinic', 21, [
            ('Condition', 'Condition {n}', ['health']), ('MedicalResult', 'Lab result {n}', ['health']), ('Person', 'Dr. Example {n}', ['parties'])]),
        # More than 10 entities, with tied document counts below the lead.
        ('nb-pharmacy', 'Organization', 'Lakeshore Pharmacy', 13, [('FinancialItem', 'Prescription receipt {n}', ['money'])]),
    ]
    minor = ['Riverside 10K', 'Northgate Credit Union', 'Lakeside HOA', 'Ellis and Park LLP', 'Benefits portal',
             'Fitness club', 'Cedar Grove School', 'Summit Auto Repair', 'Greenway Utilities', 'Oak Street Library']
    hubs = []
    for index, (key, label, name, size, kinds) in enumerate(groups):
        lead = node(key, label, name, nb_fields(key)['evidence_doc_count'] + 20 if key in NB_ENTITIES else 30)
        hubs.append(lead)
        members = []
        for n in range(size - 1):
            member_label, member_name, families = kinds[n % len(kinds)]
            text = member_name.format(n=1000 + n * 7) if '{n}' in member_name else member_name
            documents = 3 if key == 'nb-pharmacy' else None
            members.append(node(f'{key}-{n}', member_label, text, documents))
            link(lead, members[-1], families, support=2 if n % 9 == 4 else 0)
        # A few links among a hub's neighbors; most neighbors of a hub do not link to each other.
        for n in range(0, len(members) - 3, 6):
            link(members[n], members[n + 3], kinds[0][2], 1, 2 if n % 12 else 0)
        if index < 3:
            link(owner, lead, ['parties'], 9)
    for index, name in enumerate(minor):
        lead = node(f'ov-minor-{index}', ['Organization', 'Organization', 'Person', 'System'][index % 4], name, 12 - index // 2)
        for n in range(3 + index % 5):
            label = ['Person', 'FinancialItem', 'Event', 'Contract', 'Address'][(index + n) % 5]
            link(lead, node(f'ov-minor-{index}-{n}', label, '' if label == 'FinancialItem' else f'{name} item {n + 1}'), ['loose'])
    # Bridges between groups, drawn dotted.
    for a, b in [(hubs[0], 'nb-parties-0-4'), (hubs[3], 'ov-clinic-2'), ('ov-minor-1', hubs[1]), ('nb-money-0-7', 'ov-minor-4')]:
        link(a, b, ['parties'], 1, 1)
    small_names = ['Spare key tag', 'Storage unit 14', 'Parking permit', 'Library card', 'Gym locker', 'Bike lock',
                   'Garden plot 3', 'Boat slip 22', 'Mailbox 7', 'Water meter', 'Gas meter', 'Alarm code card',
                   'Visitor pass', 'Toll tag', 'Transit card', 'Warranty card', 'Recycling bin', 'Smoke detector']
    for index, name in enumerate(small_names):
        a = node(f'ov-small-{index}', ['Product', 'Contract', 'System'][index % 3], name)
        b = node(f'ov-small-{index}-b', ['Organization', 'FinancialItem', 'Person'][index % 3], '' if index % 3 == 1 else f'{name} issuer')
        link(a, b, ['other'], 1 + index % 2)
        if index % 4 == 0:
            link(b, node(f'ov-small-{index}-c', 'DateEvent', f'Renewal {index + 1}'), ['time'], 1)
    for index in range(12):
        link(owner, node(f'ov-owner-only-{index}', ['Person', 'Organization', 'Event'][index % 3], f'Owner contact {index + 1}'), ['parties'], 2)
    degree = {}
    for a, b in pairs:
        degree[a] = degree.get(a, 0) + 1
        degree[b] = degree.get(b, 0) + 1
    ordered = sorted(nodes, key=lambda key: nodes[key][0])
    index = {key: at for at, key in enumerate(ordered)}
    links = sorted([sorted((index[a], index[b])) + value for (a, b), value in pairs.items()])
    return {'version': hashlib.sha256(json.dumps([nodes[key][0] for key in ordered] + [list(pair) for pair in sorted(pairs)]).encode()).hexdigest(),
            'columns': ['id', 'label', 'name', 'evidence_doc_count', 'link_count'],
            'nodes': [nodes[key] + [degree.get(key, 0) + nodes[key][3]] for key in ordered],
            'links': links, 'link_columns': ['source', 'target', 'weight', 'support', 'families'],
            'owner': index[owner], 'unlinked': 412, 'truncated': False}


OVERVIEW = overview_archive()

LOCK = threading.RLock()
NOW = '2026-09-04T12:00:00+00:00'
clock_tick = 0
DOCS = [dict(node['props']) for node in NODES if 'Document' in node['labels']]
DOCS[0].update(title='January premium statement', doc_type='insurance')
DOCS.extend({'paperless_id': 9000 + index, 'title': f'Synthetic archive {index:03d}',
             'doc_type': 'invoice' if index % 2 == 0 else 'medical', 'date': '2026-09-01'}
            for index in range(301))
# Stored dates come in several forms; Documents shows each as "01 Sep 2026". 9015 and 9022 are in no graph.
DOCS[17]['date'], DOCS[24]['date'] = 'September 1, 2026', '1 SEP 26'
SEARCH_NODES = copy.deepcopy(NODES) + [
    {'labels': ['Organization'], 'props': {'uuid': f'synthetic-search-{index:03d}',
        'name': f'Search Fixture Entity {index:03d}', 'description': 'Synthetic pageable graph search fixture.'}}
    for index in range(120)]
FEEDBACK = []
CONVERSATIONS = {}
TASKS = {}
PROCESSED = {doc['paperless_id']: NOW for doc in DOCS}
REVIEWED = set()
MERGES = []
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
    global clock_tick
    clock_tick += 1
    return (datetime.fromisoformat(NOW) + timedelta(seconds=clock_tick)).isoformat()


def document(doc_id):
    return next((doc for doc in DOCS if doc['paperless_id'] == doc_id), None)


def detail(doc_id):
    doc = document(doc_id)
    reports = [copy.deepcopy(row) for row in FEEDBACK if row['document_id'] == doc_id]
    content = SOURCE_TEXT if doc_id == 101 else f"Synthetic source text for {doc['title']}."
    return {'paperless': {'id': doc_id, **doc, 'content': content},
            'graph': {'document': {**doc, 'correspondent': 'Example Utility' if doc_id == 101 else 7},
                      'entities': [{'labels': node['labels'], 'properties': node['props']}
                                   for node in NODES[:2]] if doc_id == 101 else [],
                      'relationships': [
                          {'rel_type': 'ISSUED_BY', 'props': NODES[1]['props'], 'rel_props': {'source_doc': 101}},
                          {'rel_type': 'MENTIONS', 'props': NODES[0]['props'], 'rel_props': {}},
                          {'rel_type': 'COVERS', 'props': NODES[0]['props'], 'rel_props': {'source_doc': 101, 'inferred': True}},
                          {'rel_type': 'RELATED_TO', 'props': NODES[1]['props'],
                           'rel_props': {'support_records': [json.dumps({'source_doc': 101}),
                                                            json.dumps({'source_doc': 102, 'inferred': True})]}},
                      ] if doc_id == 101 else []},
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
            # Audited sources carry no type, like app/query.py; the type comes from the evidence pack.
            'sources': [{'document_id': 101, 'title': 'January premium statement', 'excerpt': SOURCE_TEXT}],
            'query_plan': {'mode': 'strict', 'intent': 'lookup', 'domain': 'insurance', 'requires_current': False},
            'verification': {'status': 'verified', 'supported_claims': [claim], 'unsupported_claims': [],
                             'stale_or_conflicting_claims': [], 'missing_evidence': []},
            'claim_ledger': {'complete': True, 'candidate_text': claim, 'unitization': 'prose_v1',
                             'claims': [{'id': 'unit-0', 'claim': claim, 'start': 0,
                              'end': len(claim), 'status': 'supported', 'references': [reference],
                              'document_id': 101, 'source_title': 'January premium statement',
                              'evidence_ids': ['doc-101-chunk-0'], 'evidence_quote': quote,
                              'temporal_scope': 'historical'}],
                             'summary': {'total': 1, 'audited': 1, 'supported': 1, 'unsupported': 0, 'conflicting': 0, 'missing': 0,
                                         'unchecked': 0, 'support_ratio': 1, 'audit_coverage': 1}},
            'source_summary': {'source_count': 1, 'trust_score': 0.8, 'trust_level': 'high',
                               'verification_status': 'verified', 'audit_status': 'claim_audited',
                               'claim_summary': {'supported': 1},
                               'trust_dimensions': {'claim_support': 1, 'audit_coverage': 1}},
            'evidence_pack': {'items': [{'id': 'doc-101-chunk-0', 'document_id': 101, 'chunk_index': 0,
                                       'title': 'January premium statement', 'doc_type': 'insurance', 'excerpt': SOURCE_TEXT}],
                              'coverage': {'evidence_item_count': 1, 'source_document_count': 1}},
            'evidence': {'score': 0.8, 'level': 'high'},
            'source_dates': {'101': '2026-01-31'},
            'finalization': {'status': 'supported', 'disposition': 'supported', 'complete': True, 'answer_verified': True},
            'trace': [{'step': 'source_audit', 'status': 'ok', 'detail': 'Synthetic complete source support assessment'}],
            'timeline_events': []}


# app.answer_finalization.ABSTENTION; that module needs backend packages the frontend job does not install.
ABSTENTION = ('I could not verify a complete answer from the retrieved source text. '
              'Please review the source documents or narrow the question before relying on specific facts.')
# The backend's missing-evidence sentence for each withheld disposition.
WITHHELD = {
    'unsupported': 'Not every material claim has validated source support.',
    'incomplete': 'Not every material claim has validated source support.',
    'current_unresolved': 'The audited claims do not establish current-world status or a sufficiently scoped source report.',
    'timeout': 'The source audit exceeded its time budget.',
    'audit_failed': 'The source audit was unavailable or returned invalid data.',
}
SECOND_SOURCE = {'document_id': 102, 'title': 'Utility statement — February', 'doc_type': 'financial_statement',
                 'chunk_index': 0, 'excerpt': 'Synthetic source text for Utility statement — February.', 'date': '2026-02-28'}
DRAFT_MARKER = 'WITHHELD_DRAFT_MARKER_$777'


def answer_state(result, question):
    """Apply a `state-<name>` token from the question; Fast mode is always unaudited."""
    claim = result['claim_ledger']['claims'][0]['claim']
    final, ledger = result['finalization'], result['claim_ledger']
    if result['mode'] == 'quick':
        result['answer'] = 'Unaudited quick answer — verify the source documents before relying on it.\n\n' + claim
        final.update(disposition='unaudited', complete=False, answer_verified=False)
        result['verification'].update(status='unaudited')
        result['evidence'].update(score=0.25, level='low', audit_status='unaudited')
        result['confidence'] = 0.25
    elif 'state-documented' in question:
        result['sources'].append(copy.deepcopy(SECOND_SOURCE))
        result['source_dates'] = {'101': '2026-01-31', '102': '2026-02-28'}
        result['answer'] += '\n\nThese are documented facts. Current status as of 2026-09-04 is not established by the retrieved evidence.'
        final.update(disposition='qualified', documented_qualification=True)
        result['evidence'].update(score=0.65, level='medium', audit_status='qualified')
        result['confidence'] = 0.65
    elif 'state-retry' in question:
        result['answer'] = 'The document index changed while this answer was being checked. Retry after indexing finishes.'
        result['claim_ledger'] = {'claims': [], 'summary': {}, 'complete': False}
        result['source_dates'] = {}
        final.update(disposition='corpus_changed', complete=False, answer_verified=False, cited_document_ids=[])
        result['verification'].update(status='corpus_changed', supported_claims=[],
                                      missing_evidence=['The document index changed during the source check.'])
        result['evidence'].update(score=0.0, level='low', audit_status='corpus_changed')
        result['confidence'] = 0.0
    else:
        disposition = next((key for key in WITHHELD if f'state-withheld-{key}' in question), None)
        if disposition is None:
            return
        # The draft and the stale summary still read as verified; only the disposition decides the state.
        result['answer'] = ABSTENTION + ' ' + DRAFT_MARKER
        result['sources'].append(copy.deepcopy(SECOND_SOURCE))
        result['source_dates'] = {'101': '2026-01-31', '102': '2026-02-28'}
        ledger.pop('candidate_text')
        ledger['claims'][0].update(claim=DRAFT_MARKER, status='unsupported')
        final.update(disposition=disposition, complete=False, answer_verified=False, cited_document_ids=[])
        result['verification'].update(status=disposition, supported_claims=[], unsupported_claims=[DRAFT_MARKER],
                                      missing_evidence=[WITHHELD[disposition]])
        result['evidence'].update(score=0.0, level='low', audit_status=disposition)
        result['confidence'] = 0.0


def claims_answer(result):
    """Three verified claims: one ends in an emoji, one sits under a heading and a bold label, one source has no date."""
    sentences = ['The policy covers the house on Elm Street \U0001F3E0.',
                 '## Premium\n**Amount:** The January statement lists a premium of $25.',
                 'The February statement lists a balance of $0.']
    candidate = '\n\n'.join(sentences)
    sources = [(101, 'January premium statement', 'January statement: the premium is $25.'),
               (101, 'January premium statement', 'January statement: the premium is $25.'),
               (102, SECOND_SOURCE['title'], 'February statement: the balance is $0. No payment is due this month. '
                'The next statement will list any new charges, credits and adjustments made to the account '
                'after the closing date shown above.')]
    claims = []
    for index, (sentence, (doc_id, title, quote)) in enumerate(zip(sentences, sources)):
        start = candidate.index(sentence)  # Python counts code points, like the backend ledger.
        claims.append({'id': f'u{index + 1}', 'claim': sentence, 'start': start, 'end': start + len(sentence),
                       'status': 'supported', 'references': [{'document_id': doc_id, 'quote': quote, 'source_title': title}]})
    result['answer'] = candidate + ' [Document 101](/documents/101)'
    result['sources'].append(copy.deepcopy(SECOND_SOURCE))
    result['claim_ledger'].update(candidate_text=candidate, claims=claims)
    result['claim_ledger']['summary'].update(total=3, audited=3, supported=3)
    result['verification']['supported_claims'] = sentences
    result['source_dates'] = {'101': '2026-01-31'}


# Evidence shaped like real retrieval: whole indexed chunks whose first lines are the document header.
POLICY_HEADER = ('Cedar Example Insurance Company Auto Policy Declarations Policy number: CX-AUTO-440192 '
                 'Named insured: Jordan Example, 14 Birch Lane, Springfield Policy period: March 1, 2025 to '
                 'March 1, 2026, 12:01 a.m. standard time at the address of the named insured. Agent: Morgan Example, '
                 'Cedar Example Agency. Vehicle: 2021 Solara Wagon, garaged at the address above. ')
RENEWAL_HEADER = POLICY_HEADER.replace('Declarations', 'Renewal Declarations').replace(
    'March 1, 2025 to March 1, 2026', 'March 1, 2026 to March 1, 2027')
REAL_SOURCES = [
    (9001, 'Auto policy declarations 2025', '2025-03-01',
     POLICY_HEADER + 'Coverages and limits apply as shown on this page. Liability: $100,000 each person. '
     'Collision deductible: $500. Comprehensive deductible: $250. Six-month premium: $612.00. '
     'Premium is due in full on the effective date unless an installment plan is selected.'),
    (9002, 'Auto policy renewal 2026', '2026-03-01',
     RENEWAL_HEADER + 'Coverages and limits apply as shown on this page. Liability: $100,000 each person. '
     'Collision deductible: $1,000, raised from $500 at the request of the named insured. '
     'Comprehensive deductible: $250. Six-month premium: $655.00.'),
    (9003, 'Insurance card 2025', '2025-03-01',
     'Cedar Example Insurance Company proof of insurance card. Keep this card in the vehicle. Policy number: '
     'CX-AUTO-440192. Effective March 1, 2025. Expires March 1, 2026. Vehicle: 2021 Solara Wagon.'),
    (9004, 'Premium billing statement', '2025-02-10',
     'Cedar Example Insurance Company billing statement. Statement date: February 10, 2025. Account: '
     'Jordan Example, 14 Birch Lane, Springfield. Policy number: CX-AUTO-440192. Previous balance: $0.00. '
     'Payments received: none. Six-month premium for the term beginning March 1, 2025: $612.00. Amount due by March 1, 2025: $612.00.'),
    (9005, 'Agent welcome letter', None,
     'Dear Jordan Example, thank you for choosing Cedar Example Agency for your auto insurance. Your declarations '
     'page lists your coverages, limits and deductibles. Contact Morgan Example with any questions about your policy.'),
    (9006, 'Payment confirmation', '2025-02-27',
     'Cedar Example Insurance Company payment confirmation. Confirmation number: PAY-20250227-0192. Policy number: '
     'CX-AUTO-440192. Payer: Jordan Example. Payment method: bank transfer. Amount paid: $612.00 on February 27, 2025.'),
]


def real_answer(result):
    """A list of four claims over long passages; claim 1 has six references, two of which state no value."""
    sentences = [
        'The Cedar Example auto policy declarations for the March 1, 2025 to March 1, 2026 term list a six-month premium of $612.00.',
        'The renewal declarations for the March 1, 2026 to March 1, 2027 term list a six-month premium of $655.00.',
        'The March 1, 2025 to March 1, 2026 declarations list a collision deductible of $500.',
        'The renewal raised the collision deductible from $500 to $1,000 at the request of the named insured.',
    ]
    candidate = '\n'.join('- ' + sentence for sentence in sentences)
    by_id = {doc_id: (title, quote) for doc_id, title, _, quote in REAL_SOURCES}
    ref = lambda doc_id: {'document_id': doc_id, 'source_title': by_id[doc_id][0], 'quote': by_id[doc_id][1]}
    # The backend's order; the two references without the amount come first.
    references = [[9003, 9001, 9005, 9004, 9006, 9002], [9002], [9001], [9002]]
    claims = []
    for index, (sentence, ids) in enumerate(zip(sentences, references)):
        start = candidate.index(sentence)
        claims.append({'id': f'u{index + 1}', 'claim': sentence, 'start': start, 'end': start + len(sentence),
                       'status': 'supported', 'references': [ref(doc_id) for doc_id in ids]})
    result['answer'] = candidate
    result['sources'] = [{'document_id': doc_id, 'title': title, 'doc_type': 'insurance', 'excerpt': quote}
                         for doc_id, title, _, quote in REAL_SOURCES]
    result['source_dates'] = {str(doc_id): date for doc_id, _, date, _ in REAL_SOURCES if date}
    result['claim_ledger'].update(candidate_text=candidate, claims=claims)
    result['claim_ledger']['summary'].update(total=4, audited=4, supported=4)
    result['verification']['supported_claims'] = sentences
    result['entities_found'] = [{'name': 'Jordan Example', 'label': 'Person'}, {'name': 'CX-AUTO-440192', 'label': 'Policy'}]
    result['follow_up_suggestions'] = ['When is the next premium due?', 'What does the comprehensive deductible cover?']


def saved_conversations():
    """Three saved answers that only exist as stored messages: re-verification fails, legacy, and pre-candidate."""
    question = 'What premium does the January statement list?'
    verified = final_answer(question)
    verified.pop('source_dates')
    unverifiable = copy.deepcopy(verified)
    unverifiable.update(confidence=0.0, claim_ledger={'claims': [], 'summary': {}, 'complete': False},
                        evidence={'score': 0.0, 'level': 'low', 'audit_status': 'unavailable'})
    unverifiable['finalization'].update(disposition='stored_binding_unavailable', complete=False, answer_verified=False,
                                        cited_document_ids=[])
    unverifiable['verification'].update(status='unavailable', supported_claims=[],
                                        missing_evidence=['Saved verification could not be validated.'])
    legacy = {key: value for key, value in copy.deepcopy(verified).items()
              if key not in ('finalization', 'evidence', 'claim_ledger')}
    legacy.update(answer='The January statement lists a premium of $25.', confidence=0.8,
                  verification={'status': 'needs_review'})
    older = copy.deepcopy(verified)
    older['claim_ledger'].pop('candidate_text')
    older['finalization'].update(answer_digest=hashlib.sha256(older['answer'].encode()).hexdigest())
    for title, message in (('Saved answer that fails re-verification', unverifiable),
                           ('Legacy answer without finalization', legacy),
                           ('Older verified answer', older)):
        key = f'synthetic-conversation-{len(CONVERSATIONS) + 1}'
        stored = {'role': 'assistant', 'content': message.pop('answer'), 'query_time_ms': 8400,
                  'source_dates': {'101': '2026-01-31'}, **message}
        CONVERSATIONS[key] = {'id': key, 'title': title, 'message_count': 2, 'last_message_at': NOW,
                              'messages': [{'role': 'user', 'content': question}, stored]}


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
            if route == '/entity-review/merges':
                return self.reply({'merges': MERGES})
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
                entity = get('entity')
                if entity:
                    # The synthetic links stand in for source_doc_ids and MENTIONS.
                    linked = {other for rel in RELS for uid, other in ((rel['start'], rel['end']), (rel['end'], rel['start'])) if uid == entity}
                    matches = [doc for doc in matches if f"doc-{doc['paperless_id']}" in linked]
                facets = {kind: sum(doc['doc_type'] == kind for doc in matches) for kind in sorted({doc['doc_type'] for doc in matches})}
                matches = [doc for doc in matches if not selected_type or doc['doc_type'] == selected_type]
                sort = get('sort', 'title')
                matches.sort(key=lambda doc: (str(doc.get(sort, '')), doc['paperless_id']), reverse=get('direction') == 'desc')
                offset, limit = int(get('offset', '0')), int(get('limit', '25'))
                named = next((node for node in NODES if identity(node) == entity), None) if entity else None
                return self.reply({'scope': 'indexed_documents', 'total': len(matches), 'offset': offset, 'limit': limit,
                                   'has_more': offset + limit < len(matches), 'doc_types': facets,
                                   **({'entity': named and {'uuid': entity, 'name': named['props']['name'], 'labels': named['labels']}} if entity else {}),
                                   'results': [{'labels': ['Document'], 'properties': doc,
                                                'entity_count': sum(f"doc-{doc['paperless_id']}" in (rel['start'], rel['end']) for rel in RELS)}
                                               for doc in matches[offset:offset + limit]]})
            if route == '/graph/initial' or route.startswith('/graph/neighbors/'):
                return self.reply({'nodes': NODES, 'relationships': RELS})
            if route == '/graph/search':
                query = get('q').lower().strip()
                if query == 'fixture-error':
                    return self.reply({'detail': 'Synthetic graph search failure'}, 503)
                if get('exclude_documents') in ('1', 'true'):
                    # The graph landing: entities with the derived fields, the owner flagged but not removed.
                    pool = {**{uid: entity for uid, entity in NB_ENTITIES.items() if not uid.startswith('ex-')},
                            **{identity(node): (node['labels'][0], node['props']['name'])
                                              for node in SEARCH_NODES if 'Document' not in node['labels']}}
                    rows = [{'labels': [label], 'properties': {'uuid': uid, 'name': name},
                             'evidence_doc_count': nb_fields(uid)['evidence_doc_count'], 'matched_alias': None,
                             'is_owner': uid == HUB, 'owner_match': uid in OWNER_MATCHES}
                            for uid, (label, name) in pool.items() if query in name.lower()]
                    if get('sort') == 'documents':
                        rows.sort(key=lambda row: (-row['evidence_doc_count'], row['properties']['name'].lower()))
                    else:
                        # Relevance ties fall back to the uuid, as in the backend; an empty query lists nothing.
                        rows = sorted(rows, key=lambda row: row['properties']['uuid']) if query else []
                    offset, limit = int(get('offset', '0')), int(get('limit', '20'))
                    return self.reply({'query': get('q'), 'owner': HUB, 'total': len(rows), 'offset': offset, 'limit': limit,
                                       'has_more': offset + limit < len(rows), 'results': rows[offset:offset + limit]})
                rows = [{'labels': node['labels'], 'properties': node['props']} for node in SEARCH_NODES
                        if query in json.dumps(node).lower()]
                if get('type') == 'Document' and get('limit') == '12':
                    # Domain searches share a complete synthetic matching set;
                    # browser checks cover paging rather than domain relevance.
                    rows = [{'labels': ['Document'], 'properties': doc} for doc in DOCS]
                offset, limit = int(get('offset', '0')), int(get('limit', '50'))
                return self.reply({'total': len(rows), 'offset': offset, 'limit': limit,
                                   'has_more': offset + limit < len(rows), 'results': rows[offset:offset + limit]})
            if route == '/graph/subgraph':
                # GET /graph/subgraph: the links among 2 to 150 entities, outside evidence and without Document ends.
                ids = [value.strip() for value in get('ids', '').split(',')]
                if not 2 <= len(ids) <= 150 or not all(ids):
                    return self.reply({'detail': 'Supply between 2 and 150 comma-separated entity ids'}, 400)
                links = [{'key': f'{start}~{rel_type}~{end}', 'type': rel_type, 'family': classify_relationship(rel_type)[0],
                          'source': start, 'target': end,
                          'support_state': 'supported' if not any(f for _, f, _ in records) else 'inferred' if all(f for _, f, _ in records) else 'mixed',
                          'stated_count': sum(not f for _, f, _ in records), 'inferred_count': sum(f for _, f, _ in records),
                          'records': [{'doc_id': doc, 'inferred': flag, 'confidence': score} for doc, flag, score in records]}
                         for start, rel_type, end, records in NB_LINKS + NB_CROSS
                         if start in ids and end in ids and not start.startswith('doc-') and not end.startswith('doc-')
                         and classify_relationship(rel_type)[0] != 'evidence']
                total = len(links)
                links = sorted(links, key=lambda link: (-len(link['records']), link['key']))[:600]
                documents = sorted({record['doc_id'] for link in links for record in link['records']})
                return self.reply({'links': links, 'total': total, 'truncated': total > len(links),
                                   'documents': {str(doc): {key: document(doc)[key] for key in ('title', 'date', 'doc_type')} for doc in documents}})
            if route == '/graph/overview':
                return self.reply(OVERVIEW)
            if route == '/graph/paths':
                max_len, k = int(get('max_len', '4')), int(get('k', '5'))
                if not get('a') or not get('b') or not 2 <= max_len <= 4 or not 1 <= k <= 5:
                    return self.reply({'detail': 'Invalid path search'}, 422)
                return self.reply(nb_paths(get('a'), get('b'), max_len, k, get('include_owner') in ('1', 'true'),
                                           get('allow_inferred', '1') in ('1', 'true')))
            if route == '/graph/link':
                # GET /graph/link: one link's records with a synthetic quote naming both ends, and a rationale when inferred.
                start, rel_type, end = get('source'), get('type'), get('target')
                records = next((records for *link, records in NB_LINKS + NB_CROSS if link == [start, rel_type, end]), None)
                if records is None:
                    return self.reply({'detail': 'Link not found'}, 404)
                names = nb_node(start)[1], nb_node(end)[1]
                words = rel_type.replace('_', ' ').lower()
                return self.reply({'records': [{
                    'doc_id': doc, 'inferred': flag, 'confidence': score,
                    'quotes': [RELATIONSHIP_QUOTE if start == 'person-1' else f'{names[0]} {words} {names[1]}, as listed in this record.'],
                    'rationale': (f'The record names {names[0]} and {names[1]} on one account but does not state that '
                                  f'they are linked.') if flag else None} for doc, flag, score in records]})
            if route.startswith('/graph/node/'):
                uid = route.rsplit('/', 1)[1]
                if uid in NB_ENTITIES and get('summary') in ('1', 'true'):
                    return self.reply(nb_summary(uid))
                node = next((node for node in SEARCH_NODES if identity(node) == uid), None)
                if node is None and uid in NB_ENTITIES:
                    label, name = NB_ENTITIES[uid]
                    return self.reply({'labels': [label], 'properties': {'uuid': uid, 'name': name}, 'relationships': []})
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
                global clock_tick
                clock_tick = 0
                FEEDBACK.clear()
                CONVERSATIONS.clear()
                TASKS.clear()
                REVIEWED.clear()
                MERGES.clear()
                PROCESSED.update({doc['paperless_id']: NOW for doc in DOCS})
                return self.reply({'fixture': 'paperless-accuracy-ui-v1', 'status': 'reset'})
            if route == '/_fixture/saved-answers':
                saved_conversations()
                return self.reply({'status': 'seeded'})
            if route in ('/entity-review/merge', '/entity-review/split', '/entity-review/ignore'):
                REVIEWED.add(body.get('left_uuid') or body.get('primary_uuid'))
                if route == '/entity-review/merge':
                    pair = next(row for row in REVIEW_CANDIDATES if row['left']['uuid'] == body['primary_uuid'])
                    MERGES.insert(0, {'merge_id': f'synthetic-merge-{len(MERGES) + 1}', 'primary_uuid': body['primary_uuid'],
                                      'duplicate_uuid': body['duplicate_uuid'], 'primary_name': pair['left']['name'],
                                      'duplicate_name': pair['right']['name'], 'status': 'applied', 'created_at': NOW,
                                      'risk_flags': ['shared_source_document']})
                    # The real route returns the merged entity with its journal id.
                    return self.reply({'status': 'merged', 'entity': {'labels': ['Organization'],
                                       'properties': pair['left'], 'merge_id': MERGES[0]['merge_id']}})
                return self.reply({'status': 'completed'})
            if route == '/entity-review/unmerge':
                merge = next(row for row in MERGES if row['merge_id'] == body['merge_id'])
                merge['status'] = 'reverted'
                return self.reply({'status': 'reverted', 'merge_id': merge['merge_id'], 'reindex_doc_ids': [], 'reindex_task_id': ''})
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
                conv['messages'].append({'role': 'user', 'content': question, 'timestamp': int(datetime.fromisoformat(NOW).timestamp() * 1000)})
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
                result['claim_ledger']['candidate_text'] = claim
                result['verification']['supported_claims'] = [claim]
                result['answer'] += '\n\nPartial answer: some claims could not be verified and were omitted. This does not answer every part of your question.'
                omitted = {'id': 'u2', 'claim': 'The January statement lists a late fee of $5.', 'status': 'unsupported',
                           'rejection_reasons': [], 'value_mismatches': {}, 'omission_reason': 'source_rejection',
                           'dependency_ids': []}
                result['verification'].update(status='partial', partial={'original_total': 2, 'original_supported': 1, 'omitted_count': 1,
                                                                         'omitted_units': [omitted]},
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
            if 'margin-claims' in question.lower():
                claims_answer(result)
            if 'real-answer' in question.lower():
                real_answer(result)
            answer_state(result, question.lower())
            if 'markup' in question.lower():
                markup = '\n\nLiteral source markup: <img src=x onerror="window.__fixture_xss=1"> <script>window.__fixture_xss=2</script>'
                result['answer'] += markup
                result['claim_ledger']['candidate_text'] += markup
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
            if 'source gap coverage' in question.lower() or 'unfinished source check coverage' in question.lower():
                conservation = 'partial' if 'source gap' in question.lower() else 'unavailable'
                result['finalization']['question_coverage'].update(
                    status=conservation, complete=False, conservation_status=conservation)
            with LOCK:
                if conv:
                    conv['messages'].append({'role': 'assistant', 'content': result['answer'],
                                             'timestamp': int(datetime.fromisoformat(NOW).timestamp() * 1000), **{key: value for key, value in result.items() if key != 'answer'}})
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
