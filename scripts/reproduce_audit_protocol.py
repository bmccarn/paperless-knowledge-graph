#!/usr/bin/env python3
"""Offline protocol reproductions. Not a real-model semantic evaluation.

Exit 1 while either required behavior is absent; never invokes a provider.
"""
import asyncio
import json
from pathlib import Path
import sys
from unittest.mock import AsyncMock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tests.runtime import configure_test_environment
configure_test_environment()
from tests.source_audit_fixtures import decision
from app.answer_finalization import AnswerFinalizer, evidence_spans
from app.strands_orchestrator import StrandsQueryOrchestrator


async def reproduce():
    text = 'The equipment register dated March 20, 2024 records a capacity of 480 units.'
    pack = {'items': [{'id': 'synthetic-register', 'document_id': 1, 'chunk_index': 0,
                       'source_kind': 'ocr', 'content': text, 'source_content': text}]}
    span = evidence_spans(pack, citation_safe=True)[0]
    auditor = StrandsQueryOrchestrator()
    auditor.enabled = True
    def row(**changes):
        return decision(**{'references': [{'span_id': span['span_id']}],
                           'temporal_scope': 'historical', 'temporal_assertion': 'source_observation', **changes})
    invented = row(references=[{'span_id': 'not-a-request-owned-source'}])
    valid = row()
    responses = [json.dumps({'assessments': [invented]}), json.dumps({'assessments': [valid]})]
    with patch.object(auditor, '_text_agent', AsyncMock(side_effect=responses)) as transport:
        result = await AnswerFinalizer(auditor).finalize('What does the register record?', text, pack, evaluated_at='2026-09-09')
    unknown_handles = bool(result['finalization']['answer_verified']) and transport.await_count == 2
    unused = row(comparison_document_ids=[1])
    unused['checks']['comparison'] = 'not_applicable'
    with patch.object(auditor, '_text_agent', AsyncMock(return_value=json.dumps({'assessments': [unused]}))):
        result = await AnswerFinalizer(auditor).finalize('What does the register record?', text, pack, evaluated_at='2026-09-09')
    comparison = bool(result['finalization']['answer_verified'])
    return {'scope': 'offline native protocol and original-source finalizer; no semantic model claim',
            'request_owned_handles_receive_protocol_correction': unknown_handles,
            'unused_comparison_metadata_preserves_supported_historical_observation': comparison,
            'passed': unknown_handles and comparison}


if __name__ == '__main__':
    result = asyncio.run(reproduce())
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result['passed'] else 1)
