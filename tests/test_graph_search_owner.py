"""The entity search names the archive owner, using synthetic rows only."""
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from tests.runtime import configure_test_environment
configure_test_environment()
from app import main
from app.graph import GraphStore


def row(uuid, documents):
    return {'labels': ['Organization'], 'properties': {'uuid': uuid, 'name': uuid}, 'evidence_doc_count': documents}


class SearchOwnerTests(unittest.IsolatedAsyncioTestCase):
    async def search(self, owner_matches):
        store = GraphStore()
        page = {'total': 1, 'offset': 0, 'limit': 1, 'has_more': True, 'results': [row('busy', 120)]}
        with patch.object(main, 'graph_store', store), \
             patch.object(store, 'search_nodes', AsyncMock(return_value=page)), \
             patch.object(store, 'get_owner_matches', AsyncMock(return_value=owner_matches)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
                response = await client.get('/graph/search', params={'q': '', 'sort': 'documents', 'exclude_documents': 1, 'limit': 1})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    async def test_owner_is_named_even_when_its_row_is_not_on_the_page(self):
        body = await self.search(('chosen', 'owner', [{'uuid': 'owner'}, {'uuid': 'duplicate'}]))
        self.assertEqual(body['owner'], 'owner')
        self.assertEqual([(item['is_owner'], item['owner_match']) for item in body['results']], [(False, False)])

    async def test_owner_is_null_when_the_rule_finds_none(self):
        for status in ('unset', 'not_found'):
            self.assertIsNone((await self.search((status, None, [])))['owner'])


if __name__ == '__main__':
    unittest.main()
