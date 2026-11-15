"""Graph-only family classification and relationship type listing."""
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from tests.runtime import configure_test_environment

configure_test_environment()

from app import main
from app.relationship_families import classify_relationship


SEED_TYPES = {
    'parties': 'WORKS_AT EMPLOYED_BY OFFICER_OF OWNER_OF PREPARED_BY SIGNED_BY AUTHORIZED_BY PARTY_TO CONTRACTED_WITH ISSUED_BY AUTHORED_BY PROVIDER_FOR',
    'money': 'PAID_TO PAID_BY BILLED_BY INVOICED_BY WITHHELD_BY MORTGAGE_WITH LOAN_FROM PREMIUM_PAID_TO INCOME_FROM',
    'coverage': 'COVERS INSURES GOVERNS APPLIES_TO',
    'health': 'PATIENT_OF TREATED_BY DIAGNOSED_WITH HAS_CONDITION PRESCRIBED TAKES ORDERED_BY RESULTED_IN',
    'service': 'SERVED_IN PARTICIPATED_IN STATIONED_AT DEPLOYED_TO ASSIGNED_TO BRANCH_OF_SERVICE RATED_AT FILED_WITH SUBMITTED_TO SERVICE_RECORD_OF',
    'places': 'LOCATED_IN LOCATED_AT LIVES_IN RESIDES_AT HEADQUARTERS_IN MAILING_ADDRESS',
    'time': 'EFFECTIVE_FROM EXPIRES_ON',
    'doclinks': 'REFERENCES SUPERSEDES ATTACHMENT_TO SUPPLEMENT_TO AMENDS CORRECTS',
    'evidence': 'MENTIONS CONTAINS_RESULT',
    'loose': 'RELATED_TO ASSOCIATED_WITH',
}


class FamilyTests(unittest.TestCase):
    def test_every_seed_type_has_an_explicit_family(self):
        for family, names in SEED_TYPES.items():
            for name in names.split():
                with self.subTest(name=name):
                    self.assertEqual(classify_relationship(name), (family, 'explicit'))

    def test_keywords_use_token_prefixes_and_family_order(self):
        for name, family in {
            'CO_BORROWER_ON': 'parties', 'GARAGED_AT': 'places',
            'PAYROLL_BANK': 'money', 'DISCHARGED_FROM': 'health',
            'PAYROLL_EMPLOYER': 'parties', 'RENEWED_ON': 'time',
            'ASSOCIATES_WITH': 'loose', 'REFERENCED_BY': 'doclinks',
        }.items():
            with self.subTest(name=name):
                self.assertEqual(classify_relationship(name), (family, 'keyword'))

    def test_substrings_and_unknown_types_stay_unmapped(self):
        for name in ('ATTENDED', 'OPERATED_BY', 'RECOVERED_FROM', 'AVAILABLE_AT', 'ENDORSED_BY', 'UNKNOWN'):
            with self.subTest(name=name):
                self.assertEqual(classify_relationship(name), ('other', 'unmapped'))


class RelationshipTypeRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_endpoint_classifies_every_counted_type(self):
        rows = [
            {'type': 'MENTIONS', 'count': 12},
            {'type': 'PAYROLL_BANK', 'count': 2},
            {'type': 'RECOVERED_FROM', 'count': 1},
        ]
        with patch.object(main.graph_store, 'get_relationship_types', AsyncMock(return_value=rows)) as query:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app), base_url='http://test') as client:
                response = await client.get('/graph/relationship-types')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [
            {**rows[0], 'family': 'evidence', 'assigned_by': 'explicit'},
            {**rows[1], 'family': 'money', 'assigned_by': 'keyword'},
            {**rows[2], 'family': 'other', 'assigned_by': 'unmapped'},
        ])
        query.assert_awaited_once_with()

    async def test_empty_graph_returns_an_empty_list(self):
        with patch.object(main.graph_store, 'get_relationship_types', AsyncMock(return_value=[])):
            self.assertEqual(await main.graph_relationship_types(), [])
