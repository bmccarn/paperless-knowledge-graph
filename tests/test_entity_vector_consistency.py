import unittest
from app.entity_vector_consistency import classify_entity_vector


class EntityVectorConsistencyTests(unittest.TestCase):
    def setUp(self):
        self.graph = {"uuid": "synthetic", "name": "Cedar Analytics", "labels": ["Organization"]}
        self.vector = {"entity_uuid": "synthetic", "entity_name": "Cedar Analytics",
                       "entity_type": "Organization", "dimension": 3072}

    def test_source_spelling_variants_keep_separate_literal_diagnostic(self):
        for spelling in ("CEDAR ANALYTICS", "Cedar  Analytics", "cedar analytics"):
            result = classify_entity_vector({**self.vector, "entity_name": spelling}, self.graph)
            self.assertTrue(result["accepted"])
            self.assertEqual(result["name_agreement"], "orthographic")
        self.assertEqual(classify_entity_vector(self.vector, self.graph)["name_agreement"], "verbatim")

    def test_other_identities_and_raw_aliases_cannot_pass(self):
        graph = {**self.graph, "aliases": ["Willow Research"]}
        result = classify_entity_vector({**self.vector, "entity_name": "Willow Research"}, graph)
        self.assertEqual(result["problems"], ["name"])
        accepted = classify_entity_vector({**self.vector, "entity_name": "Willow Research"}, graph,
                                          verified_aliases=["Willow Research"])
        self.assertTrue(accepted["accepted"])
        self.assertEqual(accepted["name_agreement"], "verified_alias")

    def test_uuid_type_dimension_are_independent_of_spelling(self):
        for key, value, problem in (("entity_uuid", "different", "uuid"),
                                    ("entity_type", "Person", "type"), ("dimension", 1536, "dimension")):
            result = classify_entity_vector({**self.vector, key: value}, self.graph)
            self.assertFalse(result["accepted"])
            self.assertIn(problem, result["problems"])
        self.assertFalse(classify_entity_vector(self.vector, {**self.graph, "labels": ["Organization", "Person"]})["accepted"])
        self.assertEqual(classify_entity_vector(self.vector, None)["problems"], ["orphan_vector"])

    def test_only_documented_legacy_type_representations_are_compatible(self):
        for vector_type, label in (("Document", "DocumentRef"), ("InsurancePolicy", "Insurancepolicy"),
                                    ("FinancialItem", "Financialitem"), ("DateEvent", "Dateevent")):
            result = classify_entity_vector({**self.vector, "entity_type": vector_type}, {**self.graph, "labels": [label]})
            self.assertTrue(result["accepted"])
            self.assertTrue(result["legacy_type_representation"])
        self.assertFalse(classify_entity_vector({**self.vector, "entity_type": "organization"}, self.graph)["accepted"])

