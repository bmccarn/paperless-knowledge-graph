import asyncio,json,sys
from pathlib import Path
REPO = next(p for p in (Path.cwd(), *Path(__file__).resolve().parents) if (p / 'app' / 'pipeline.py').exists())
sys.path.insert(0, str(REPO))
from tests.runtime import configure_test_environment
configure_test_environment()
from tests.test_ingestion import IngestionTests,pipeline
async def main():
    f=IngestionTests(); await f.asyncSetUp()
    async def failed_resolve(): return {'total_merged':0, 'errors':['Failed to merge entity pair: synthetic database failure']}
    f.resolver.resolve_all_entities=failed_resolve
    try:
        result=await pipeline.reindex_all()
        print(json.dumps({k:result[k] for k in ('status','errors','processed','checkpoint_advanced','postprocess_errors')},indent=2))
    finally: f.doCleanups()
asyncio.run(main())
