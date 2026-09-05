"""Synthetic graph-only UI fixture on 127.0.0.1:8485; no real application data."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import json
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

class Handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204);self.send_header('Access-Control-Allow-Origin','*');self.send_header('Access-Control-Allow-Headers','Content-Type');self.end_headers()
    def do_GET(self):
        parsed=urlparse(self.path);query=parse_qs(parsed.query);route=parsed.path
        code=200
        if route=='/config': data={'paperless_url':'http://localhost:8000'}
        elif route=='/graph/initial' or route.startswith('/graph/neighbors/'):
            data={'nodes':NODES,'relationships':RELS}
        elif route=='/graph/search':
            q=query.get('q',[''])[0].strip().lower()
            data={'results':[{'labels':n['labels'],'properties':n['props']} for n in NODES if q in json.dumps(n).lower()]}
            if q == 'fixture-error': code=503;data={'detail':'Synthetic search failure'}
        elif route.startswith('/graph/node/'):
            uid=route.rsplit('/',1)[1];node=next((n for n in NODES if identity(n)==uid),None)
            if node is None: code=404;data={'detail':'No node'}
            else:
                relationships=[]
                for rel in RELS:
                    if uid not in (rel['start'],rel['end']): continue
                    other=rel['end'] if rel['start']==uid else rel['start']
                    neighbor=next(n for n in NODES if identity(n)==other)
                    relationships.append({'rel_type':rel['type'],'rel_props':rel['props'],'direction':'out' if rel['start']==uid else 'in','neighbor_labels':neighbor['labels'],'neighbor_props':neighbor['props']})
                data={'labels':node['labels'],'properties':node['props'],'relationships':relationships}
        else: code=404;data={'detail':'fixture endpoint not implemented'}
        body=json.dumps(data).encode();self.send_response(code);self.send_header('Content-Type','application/json');self.send_header('Access-Control-Allow-Origin','*');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
if __name__ == '__main__':
    ThreadingHTTPServer(('127.0.0.1',8485),Handler).serve_forever()
