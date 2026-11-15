"""Skip-tag changes are visible without restarting the client."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from tests.runtime import configure_test_environment
configure_test_environment()
from app.paperless import PaperlessClient


class SkipTagTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_cache_expires_and_returned_sets_are_independent(self):
        client = PaperlessClient()
        tags = []
        clock = Mock(return_value=0)
        with patch('app.paperless.time', SimpleNamespace(monotonic=clock)), \
             patch.object(client, 'get_all_tags', AsyncMock(side_effect=lambda: list(tags))) as fetch:
            self.assertEqual(await client.get_skip_tag_ids(), set())
            tags.append({'id': 42, 'name': 'needs-review'})
            clock.return_value = 59
            self.assertEqual(await client.get_skip_tag_ids(), set())
            clock.return_value = 60
            result = await client.get_skip_tag_ids()
            self.assertEqual(result, {42})
            result.clear()
            self.assertEqual(await client.get_skip_tag_ids(), {42})
            self.assertEqual(fetch.await_count, 2)

    async def test_refresh_observes_rename_and_recreated_tag_within_ttl(self):
        client = PaperlessClient()
        with patch.object(client, 'get_all_tags', AsyncMock(side_effect=[
            [{'id': 42, 'name': 'needs-review'}],
            [{'id': 42, 'name': 'reviewed'}],
            [{'id': 73, 'slug': 'needs-review'}],
        ])) as fetch:
            self.assertEqual(await client.get_skip_tag_ids(), {42})
            self.assertEqual(await client.get_skip_tag_ids(refresh=True), set())
            self.assertEqual(await client.get_skip_tag_ids(refresh=True), {73})
            self.assertEqual(fetch.await_count, 3)

    async def test_expired_lookup_failure_does_not_extend_cache(self):
        client = PaperlessClient()
        clock = Mock(return_value=0)
        with patch('app.paperless.time', SimpleNamespace(monotonic=clock)), \
             patch.object(client, 'get_all_tags', AsyncMock(side_effect=[
                 [], RuntimeError('offline'), [{'id': 42, 'name': 'needs-review'}]])):
            await client.get_skip_tag_ids()
            clock.return_value = 60
            with self.assertRaisesRegex(RuntimeError, 'offline'):
                await client.get_skip_tag_ids()
            self.assertEqual(await client.get_skip_tag_ids(), {42})
