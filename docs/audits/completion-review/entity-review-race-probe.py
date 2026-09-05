import asyncio,json,sys
from pathlib import Path
REPO=next(p for p in (Path.cwd(), *Path(__file__).resolve().parents) if (p/'app'/'pipeline.py').exists())
sys.path.insert(0,str(REPO))
from tests.runtime import configure_test_environment
configure_test_environment()
from unittest.mock import patch
from tests.test_entity_decisions import MemoryGraph,MemoryDecisions,person,resolver_module
async def main():
    graph=MemoryGraph([person('left','John Smith',[11]),person('right','John Smith',[22])])
    decisions=MemoryDecisions(); resolver=resolver_module.EntityResolver()
    entered=asyncio.Event(); release=asyncio.Event(); original_find=graph.find_person
    async def paused_find(name):
        entered.set(); await release.wait(); return await original_find(name)
    graph.find_person=paused_find
    with patch.object(resolver_module,'graph_store',graph),patch.object(resolver_module,'embeddings_store',decisions):
        task=asyncio.create_task(resolver.resolve_person('John Smith',22))
        await entered.wait()
        decision=await resolver.record_decision('left','right','split','Distinct same-name people')
        release.set()
        concurrent_result=await task
        fresh_result=await resolver.resolve_person('John Smith',22)
        print(json.dumps({'recorded_decision':decision['decision'],'incoming_document':22,'concurrent_resolution':concurrent_result,'fresh_resolution':fresh_result,'left_documents':graph.nodes['left']['source_doc_ids'],'right_documents':graph.nodes['right']['source_doc_ids']},indent=2))
asyncio.run(main())
