import asyncio,json,sys
from pathlib import Path
REPO = next(p for p in (Path.cwd(), *Path(__file__).resolve().parents) if (p / 'app' / 'pipeline.py').exists())
sys.path.insert(0, str(REPO))
from tests.runtime import configure_test_environment
configure_test_environment()
from tests.test_ingestion import IngestionTests, pipeline
async def main():
    fixture=IngestionTests()
    await fixture.asyncSetUp()
    try:
        fixture.existing()
        fixture.paperless.documents[1]['title']='Corrected document title'
        fixture.paperless.documents[1]['created']='2026-03-01'
        result=await pipeline.sync_documents()
        print(json.dumps({'result':{k:result[k] for k in ('status','processed','skipped','checkpoint_advanced')},'stored_graph':fixture.graph.documents[1],'source_title':fixture.paperless.documents[1]['title'],'source_created':fixture.paperless.documents[1]['created'],'chunk':fixture.embeddings.chunks[1,0],'next_sync':await pipeline.sync_documents()},default=str,indent=2))
    finally:
        fixture.doCleanups()
asyncio.run(main())
