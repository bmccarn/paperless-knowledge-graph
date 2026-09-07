"""Work-scaled source auditing, deterministic results and cancellation."""
import asyncio
import unittest
from tests.runtime import configure_test_environment
configure_test_environment()
from app.answer_finalization import AnswerFinalizer
from tests.test_answer_finalization import PACK, SupportedAuditor


class ConcurrentAuditor(SupportedAuditor):
    def __init__(self, delay=0):
        self.active = self.peak = self.calls = 0
        self.delay = delay
        self.started = asyncio.Event()

    async def audit_answer_units(self, *args):
        self.active += 1
        self.calls += 1
        self.peak = max(self.peak, self.active)
        if self.active == 4:
            self.started.set()
        try:
            await self.started.wait()
            await asyncio.sleep(self.delay)
            return await super().audit_answer_units(*args)
        finally:
            self.active -= 1


class AuditExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_56_unit_answer_uses_bounded_workers_and_more_than_one_wave_allowance(self):
        auditor = ConcurrentAuditor(delay=.06)
        answer = "\n".join(["Monthly premium: $321.00 USD."] * 56)
        result = await AnswerFinalizer(auditor, timeout_seconds=.1).finalize("Recorded premium?", answer, PACK)
        self.assertEqual(result["finalization"]["disposition"], "supported")
        self.assertEqual(auditor.calls, 14)
        self.assertEqual(auditor.peak, 4)
        self.assertEqual(auditor.active, 0)
        self.assertEqual([c["id"] for c in result["claim_ledger"]["claims"]], [f"u{i}" for i in range(1, 57)])
        self.assertEqual(result["claim_ledger"]["summary"]["supported"], 56)

    async def test_audit_timeout_cancels_every_worker_and_never_certifies_partial_output(self):
        auditor = ConcurrentAuditor(delay=10)
        answer = "\n".join(["Monthly premium: $321.00 USD."] * 20)
        result = await AnswerFinalizer(auditor, timeout_seconds=.02).finalize("Recorded premium?", answer, PACK)
        self.assertEqual(result["finalization"]["disposition"], "timeout")
        self.assertEqual(auditor.active, 0)
        self.assertFalse(result["finalization"]["complete"])
        self.assertEqual(result["claim_ledger"]["claims"], [])

    async def test_client_cancellation_reaches_active_audit_workers(self):
        auditor = ConcurrentAuditor(delay=10)
        task = asyncio.create_task(AnswerFinalizer(auditor).finalize(
            "Recorded premium?", "\n".join(["Monthly premium: $321.00 USD."] * 20), PACK))
        try:
            await asyncio.wait_for(auditor.started.wait(), timeout=.3)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(auditor.active, 0)
