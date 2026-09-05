import asyncio, json, sys
from pathlib import Path
REPO = next(p for p in (Path.cwd(), *Path(__file__).resolve().parents) if (p / 'app' / 'pipeline.py').exists())
sys.path.insert(0, str(REPO))
from tests.runtime import configure_test_environment
configure_test_environment()
from app.evidence import build_evidence_pack
from app.answer_finalization import AnswerFinalizer
async def main():
    ocr = 'Monthly premium: $321.00 USD.'
    generated_claim = 'Monthly premium: $999.00 USD.'
    generated_summary = f'DOCUMENT SUMMARY — January statement (Type: financial, Doc ID: 101)\n\n{generated_claim}'
    pack = build_evidence_pack('What premium is recorded?', {'mode':'strict'}, [
      {'document_id':101, 'chunk_index':9999, 'title':'January statement', 'doc_type':'financial', 'content':generated_summary}
    ], [])
    class Auditor:
        async def audit_answer_units(self, question, units, spans, plan):
            return {'assessments':[{'unit_id':u['id'], 'status':'supported', 'temporal_scope':'historical', 'references':[{'span_id':spans[0]['span_id'], 'evidence_id':spans[0]['evidence_id'], 'document_id':101, 'quote':generated_claim}]} for u in units]}
    result=await AnswerFinalizer(Auditor()).finalize('What premium is recorded?',generated_claim,pack)
    print(json.dumps({'actual_ocr':ocr,'generated_summary':generated_summary,'retrieved_item_source_quality':pack['items'][0]['source_quality'],'retrieved_chunk_index':pack['items'][0]['chunk_index'],'public_answer':result['answer'],'finalization':result['finalization'],'score':result['evidence']['score']},indent=2))
asyncio.run(main())
