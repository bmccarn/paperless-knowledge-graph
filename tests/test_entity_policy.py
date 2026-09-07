"""Pure, synthetic identity policy matrix; runnable without application clients."""
import unittest

from app.entity_policy import (name_key, display_name, coreference_span,
    trusted_aliases, source_alias_record, human_alias_record, context_bound_name, initialism_expansions, has_local_alias_definition)
from app.extraction_evidence import (validate_entities, validate_relationships,
    reconcile_entities, adjudicate_types)
from app.entity_bindings import DocumentBindings


class OrthographyTests(unittest.TestCase):
    def test_positive_orthographic_matrix(self):
        for kind, left, right in [
            ("Person", "José García", "Jose\u0301 Garci\u0301a"),
            ("Person", "  Alice   Example ", "alice example"),
            ("Person", "Example, Alice", "Alice Example"),
            ("Person", "Dr. Alice Example", "Alice Example"),
            ("Person", "Alice J. Example", "Alice J Example"),
            ("Person", "D’Arcy Jones", "D'Arcy Jones"),
            ("Organization", "Example Widgets, Inc.", "Example Widgets Inc"),
            ("Organization", "A.B.C. Research LLC", "ABC Research LLC"),
            ("Organization", "Example L.L.C.", "Example LLC"),
            ("Organization", "U S Department of Energy", "U.S. Department of Energy"),
            ("Organization", "Example Widgets Incorporated", "Example Widgets Inc"),
            ("Organization", "Example Widgets Limited Liability Company", "Example Widgets LLC"),
        ]:
            with self.subTest(kind=kind, left=left, right=right):
                self.assertEqual(name_key(left, kind), name_key(right, kind))

    def test_negative_distinguishing_token_matrix(self):
        for kind, left, right in [
            ("Person", "Jose Garcia", "José García"),
            ("Person", "Alice J Example", "Alice Jane Example"),
            ("Person", "Alice Example Jr.", "Alice Example Sr."),
            ("Person", "Anne-Marie Clark", "Anne Marie Clark"),
            ("Person", "Alice Example", "Alicia Example"),
            ("Person", "Li Wei", "Wei Li"),
            ("Organization", "US Department of Energy", "Department of Defense"),
            ("Organization", "Example Bank", "Example Insurance"),
            ("Organization", "Example Inc", "Example LLC"),
            ("Organization", "Example Holdings", "Example"),
            ("Organization", "Orion Card/Harbor Bank USA", "Orion"),
            ("Organization", "Example, Inc.", "Inc Example"),
            ("Product", "Widget-10", "Widget 10"),
            ("System", "example.org", "exampleorg"),
        ]:
            with self.subTest(kind=kind, left=left, right=right):
                self.assertNotEqual(name_key(left, kind), name_key(right, kind))
                self.assertEqual(display_name(left), left)

    def test_context_bound_initials_acronyms_not_every_short_brand(self):
        self.assertTrue(context_bound_name("NES", "Organization", "abbreviation"))
        self.assertTrue(context_bound_name("MERS", "Condition", "abbreviation"))
        for name, kind in [("J Smith", "Person"), ("Alice", "Person")]:
            self.assertTrue(context_bound_name(name, kind))
        for name, kind in [("Acme", "Organization"), ("ACME", "Organization"), ("acme", "Organization"),
                           ("Asthma", "Condition"), ("ASTHMA", "Condition"), ("Paris", "Location"), ("Alice Example", "Person")]:
            self.assertFalse(context_bound_name(name, kind))


class CoreferenceTests(unittest.TestCase):
    def test_positive_source_expansions_and_explicit_aliases(self):
        for left, right, source in [
            ("Network Entity Systems", "NES", "Network Entity Systems (NES) signed."),
            ("Network Entity Systems", "NES", "NES (Network Entity Systems) signed."),
            ("Network Entity Systems Inc.", "NES", "Network Entity Systems Inc. (NES) signed."),
            ("Department of Example Affairs", "DEA", "Department of Example Affairs (DEA) signed."),
            ("Example Widgets LLC", "Bright Tools", "Example Widgets LLC doing business as Bright Tools filed."),
            ("Alice Jane Example", "A J Example", "Alice Jane Example, also known as A J Example signed."),
            ("José García", "Jose Garcia", "José García also known as Jose Garcia signed."),
        ]:
            with self.subTest(source=source):
                span = coreference_span(left, right, source)
                self.assertIsNotNone(span)
                self.assertEqual(source[span["start"]:span["end"]], span["quote"])

    def test_negative_comention_brand_issuer_subsidiary_and_negation(self):
        for left, right, source in [
            ("Orion", "Harbor Bank USA", "Orion card issued by Harbor Bank USA."),
            ("Orion", "Orion Finance LLC", "Orion owns subsidiary Orion Finance LLC."),
            ("Example Services", "Example Bank", "Example Services (Example Bank) collaborated."),
            ("Network Entity Systems", "NES", "Network Entity Systems and NES are listed."),
            ("Network Entity Systems", "NES", "Not Network Entity Systems also known as NES."),
            ("Network Entity Systems", "NES", "Network Entity Systems is not also known as NES."),
            ("Network Entity Systems", "NES", "The claim Network Entity Systems also known as NES was false."),
            ("Alice Example", "Alice Smyth", "Alice Example also known as Alice Smyth's agent."),
            ("Network Entity Systems", "NEX", "Network Entity Systems (NEX)"),
            ("Department of Energy", "DOE", "US Department of Energy (DOE) signed."),
            ("National Bank", "NB", "The National Bank (NB) signed."),
            ("Example Bank", "Bright Tools", "Parent Example Bank doing business as Bright Tools."),
        ]:
            with self.subTest(source=source):
                self.assertIsNone(coreference_span(left, right, source))

    def test_local_acronym_expansions_are_distinct_not_model_guesses(self):
        source = "Network Entity Systems (NES). New Era Services (NES)."
        self.assertEqual(set(initialism_expansions("NES", source)), {"Network Entity Systems", "New Era Services"})
        self.assertEqual(initialism_expansions("NES", "Network Entity Systems and NES are listed."), [])
        self.assertFalse(has_local_alias_definition("Alice Example", "Person (Alice Example) signed."))
        # Parenthetical ambiguity is conservative and invariant to typography.
        self.assertEqual(has_local_alias_definition("Acme", "Supplier (Acme) signed."),
                         has_local_alias_definition("ACME", "Supplier (ACME) signed."))
        self.assertTrue(has_local_alias_definition("DOE", "US Department of Energy (DOE) signed."))
        self.assertEqual(initialism_expansions("NB", "The National Bank (NB) signed."), ["The National Bank"])
        self.assertEqual(initialism_expansions("DOE", "US Department of Energy (DOE) signed."), [])
        self.assertEqual(initialism_expansions("NES", "Network Entity Systems (NES). Network Entity Systems (NES)."), ["Network Entity Systems"])

    def test_markdown_emphasis_is_not_part_of_a_local_expansion(self):
        for marker in ("**",):
            with self.subTest(marker=marker):
                source = f"{marker}Network Entity Systems (NES){marker} signed."
                self.assertEqual(initialism_expansions("NES", source), ["Network Entity Systems"])
                self.assertEqual(initialism_expansions("DOE", f"{marker}US Department of Energy (DOE){marker} signed."), [])
                self.assertEqual(initialism_expansions("NES", f"Not {marker}Network Entity Systems (NES){marker} signed."), [])
                self.assertEqual(initialism_expansions("DOE", "US **Department of Energy (DOE)** signed."), [])
                self.assertEqual(initialism_expansions("NES", "Parent **Network Entity Systems (NES)** signed."), [])
                ambiguous = source + f"\n{marker}New Era Services (NES){marker} signed."
                self.assertEqual(set(initialism_expansions("NES", ambiguous)), {"Network Entity Systems", "New Era Services"})

    def test_legacy_aliases_never_gain_authority_by_existence_or_score(self):
        node = {"name": "Department of Defense", "aliases": ["US Department of Energy"],
                "alias_records": ["broken-json", {"alias": "US Department of Energy", "score": .999,
                                  "type": "Organization", "provenance": "legacy_unknown"}]}
        self.assertEqual(trusted_aliases(node, "Organization", 42, ""), [])

    def test_human_alias_provenance_requires_exact_canonical_binding_and_type(self):
        record = human_alias_record("Alice Example", "Alice Smyth", "Person", "review-1", review_method="entity_review_api")
        node = {"name": "Alice Example", "alias_records": [record], "aliases": ["Poison"]}
        self.assertEqual(trusted_aliases(node, "Person", 42, ""), ["Alice Smyth"])
        self.assertEqual(trusted_aliases(node, "Organization", 42, ""), [])
        self.assertEqual(trusted_aliases({**node, "name": "Other Person"}, "Person", 42, ""), [])
        record["review_id"] = ""
        self.assertEqual(trusted_aliases(node, "Person", 42, ""), [])

    def test_human_alias_record_requires_explicit_method_not_just_a_review_label(self):
        for method in (None, "legacy_unknown", "auto_dedup", "entity_steward"):
            with self.subTest(method=method):
                with self.assertRaisesRegex(ValueError, "explicit review origin"):
                    human_alias_record("Alice Example", "Alice Smyth", "Person", "review-1", review_method=method)
                record = {"alias": "Alice Smyth", "canonical_name": "Alice Example", "type": "Person",
                          "provenance": "human_review", "review_id": "review-1", "review_method": method}
                self.assertEqual(trusted_aliases({"name": "Alice Example", "alias_records": [record]}, "Person", 11, ""), [])

    def test_source_alias_is_scoped_by_document_revision_policy_and_quote(self):
        from tests.test_entity_review_closure import reviewed_entities, proofs
        from app.entity_policy import verified_coreference
        source = "Network Entity Systems (NES) signed."
        receipt = proofs(reviewed_entities("Network Entity Systems", "NES", source))
        span = verified_coreference("Network Entity Systems", "NES", "Organization", source, receipt)
        record = source_alias_record("Network Entity Systems", "NES", "Organization", 11, source, span)
        node = {"name": "Network Entity Systems", "alias_records": [record]}
        self.assertEqual(trusted_aliases(node, "Organization", 11, source), ["NES"])
        for doc_id, text in [(22, source), (11, source + " Revised."), (11, "")]:
            self.assertEqual(trusted_aliases(node, "Organization", doc_id, text), [])
        for field in ("quote_hash", "policy", "source_hash", "start", "end"):
            with self.subTest(field=field):
                changed = {**record, field: "stale"}
                self.assertEqual(trusted_aliases({**node, "alias_records": [changed]}, "Organization", 11, source), [])


def entity(name, kind, source, hint=""):
    quote = source
    return {"name": name, "type": kind, "confidence": .95, "description": "Synthetic source entity",
            "identity_hint": hint, "evidence_quote": quote}


class AggregationTests(unittest.TestCase):
    def test_distinct_same_name_types_and_qualified_homonyms_survive(self):
        issues = []
        first = "Jordan, employee ID E-101, signed."
        second = "Jordan, employee ID E-202, signed."
        a = validate_entities([entity("Jordan", "Person", first, "E-101")], first, 0, issues)
        b = validate_entities([entity("Jordan", "Person", second, "E-202")], second, len(first)+1, issues)
        merged = reconcile_entities(a + b + a, issues)
        self.assertEqual(len(merged), 2)
        self.assertEqual(len({item["entity_id"] for item in merged}), 2)
        self.assertEqual(issues, [])

    def test_ungrounded_discriminator_is_not_a_homonym_escape_hatch(self):
        issues = []
        self.assertEqual(validate_entities([entity("Jordan", "Person", "Jordan signed.", "E-101")], "Jordan signed.", 0, issues), [])
        self.assertIn("discriminator", issues[0])

    def test_overlapping_conflicting_types_abstain_not_last_type_wins(self):
        source = "Jordan signed."
        issues = []
        items = validate_entities([entity("Jordan", "Person", source), entity("Jordan", "Organization", source)], source, 0, issues)
        self.assertEqual(reconcile_entities(items, issues), [])
        self.assertIn("Ambiguous", issues[-1])

    def test_ambiguous_name_endpoint_requires_exact_identity_id(self):
        source = "Jordan E-101 signed for Jordan E-202."
        issues = []
        items = validate_entities([entity("Jordan", "Person", source, "E-101"), entity("Jordan", "Person", source, "E-202")], source, 0, issues)
        rel = {"from_entity": "Jordan", "to_entity": "Jordan", "relationship_type": "SIGNED_FOR",
               "confidence": .95, "rationale": "Explicit signature", "evidence_quote": source}
        self.assertEqual(validate_relationships([rel], items, source, 0, issues), [])
        rel.update(from_entity_id=items[0]["entity_id"], to_entity_id=items[1]["entity_id"])
        accepted = validate_relationships([rel], items, source, 0, issues)
        self.assertEqual(len(accepted), 1)
        self.assertNotEqual(accepted[0]["from_entity_id"], accepted[0]["to_entity_id"])
        rel["from_entity_id"] = "fabricated"
        self.assertEqual(validate_relationships([rel], items, source, 0, issues), [])

    def test_unqualified_occurrence_does_not_glue_qualified_homonyms(self):
        source = "Jordan E-101 met Jordan E-202."
        issues = []
        items = validate_entities([entity("Jordan", "Person", source, hint) for hint in ("E-101", "E-202", "")], source, 0, issues)
        self.assertEqual(len(reconcile_entities(items, issues)), 2)


class TypeEvidenceTests(unittest.TestCase):
    def test_name_only_or_invented_alternate_meaning_cannot_change_source_type(self):
        source = "MERS is a mortgage registration company."
        candidates = validate_entities([entity("MERS", "Organization", source)], source, 0, [])
        for quote in (None, "MERS", "MERS is a respiratory syndrome."):
            with self.subTest(quote=quote):
                review = {**entity("MERS", "Condition", source), "type_evidence_quote": quote,
                          "type_rationale": "An alternate meaning from world knowledge"}
                accepted = validate_entities([review], source, 0, [])
                adjudicated = adjudicate_types(candidates, accepted, [review], source, 0, [])
                self.assertEqual(adjudicated[0]["type"], "Organization")
                self.assertEqual(adjudicated[0]["description"], candidates[0]["description"])

    def test_actual_contextual_type_evidence_can_correct_and_is_retained(self):
        source = "NES is a registered company."
        candidates = validate_entities([entity("NES", "Person", source)], source, 0, [])
        review = {**entity("NES", "Organization", source), "type_evidence_quote": source,
                  "type_rationale": "The source calls NES a registered company."}
        accepted = validate_entities([review], source, 0, [])
        adjudicated = adjudicate_types(candidates, accepted, [review], source, 0, [])
        self.assertEqual(adjudicated[0]["type"], "Organization")
        self.assertEqual(adjudicated[0]["type_assessment"]["evidence"]["quote"], source)
        self.assertEqual(adjudicated[0]["type_assessment"]["original_type"], "Person")


class BindingTests(unittest.IsolatedAsyncioTestCase):
    async def test_document_binding_reuses_exact_corrected_type_and_uuid(self):
        class Resolver:
            calls = []
            async def resolve(self, name, kind, doc_id, **kwargs):
                self.calls.append((name, kind, doc_id))
                return f"uuid-{len(self.calls)}"
        source = "Network Entity Systems (NES) signed."
        accepted = validate_entities([entity("NES", "Organization", source)], source, 0, [])
        resolver = Resolver()
        bindings = DocumentBindings(11, {"all_entities": accepted}, source, resolver)
        await bindings.resolve_all()
        for _ in range(20):
            bound = bindings.lookup("NES", "Condition")  # stale metadata guess
            self.assertEqual(str(bound), "uuid-1")
            self.assertEqual(bound.entity_type, "Organization")
        self.assertEqual(len(resolver.calls), 1)
        self.assertIsNone(bindings.lookup("Not in extraction", "Person"))
        self.assertIsNone(bindings.lookup("NES", "Condition", accepted[0]["entity_id"]))
        other = DocumentBindings(22, {"all_entities": []}, "Different source", resolver)
        self.assertIsNone(other.lookup("NES", "Organization"))

    async def test_source_offset_or_quote_tampering_fails_before_resolution(self):
        source = "Jordan signed."
        accepted = validate_entities([entity("Jordan", "Person", source)], source, 0, [])
        accepted[0]["evidence"][0]["start"] = 1
        with self.assertRaisesRegex(ValueError, "exact source"):
            DocumentBindings(11, {"all_entities": accepted}, source, None)


class MergeEntrypointTests(unittest.TestCase):
    def test_production_merge_callers_require_explicit_api_origin(self):
        import ast
        from pathlib import Path
        root = Path(__file__).resolve().parents[1]
        found = set()
        for path in (root / "app").glob("*.py"):
            tree = ast.parse(path.read_text())
            for function in ast.walk(tree):
                if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for call in ast.walk(function):
                    if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute) or call.func.attr != "merge_entities":
                        continue
                    found.add((path.name, function.name, ast.unparse(call.func.value)))
                    self.assertIn("review_method", {keyword.arg for keyword in call.keywords})
                    if path.name == "main.py":
                        method = next(keyword.value for keyword in call.keywords if keyword.arg == "review_method")
                        self.assertIsInstance(method, ast.Constant)
                        self.assertEqual(method.value, "entity_review_api")
        self.assertEqual(found, {("main.py", "entity_review_merge", "entity_resolver"),
                                 ("entity_resolver.py", "merge_entities", "graph_store")})
