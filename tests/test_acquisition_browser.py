"""Actual built desktop/mobile query UI over acquisition and controlled originals."""
import os
import unittest
from tests import test_acquisition_query as acquisition
from tests import test_question_presentation_browser as browser


@unittest.skipUnless(os.environ.get('LIVE_FRONTEND_TEST_NODE'), 'Opt-in actual built browser')
class AcquisitionBrowserTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = acquisition.AcquisitionQueryTests.asyncSetUp
    asyncTearDown = acquisition.AcquisitionQueryTests.asyncTearDown
    model = acquisition.AcquisitionQueryTests.model
    exercise = browser.QuestionPresentationBrowserTests.exercise

    async def test_complete_acquisition_in_desktop_mobile_and_saved_answer(self):
        self.expected_acquisition_complete = True
        await self.exercise(False)

    async def test_missing_discovered_original_stays_partial_after_reload(self):
        self.expected_acquisition_complete = False
        self.graph.get_node.return_value['properties']['source_doc_ids'] = [1, 2]
        self.index.missing = {2}
        await self.exercise(False)
