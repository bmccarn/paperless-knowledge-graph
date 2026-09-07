"""Document-lifetime identity bindings, never a process-wide name cache."""
from __future__ import annotations

from app.entity_policy import ENTITY_TYPES, name_key, verified_spans, valid_identity_receipt


class ResolvedEntity(str):
    """UUID with its authoritative graph label (legacy processors accept str)."""
    def __new__(cls, uuid, entity_type, name="", description=""):
        instance = super().__new__(cls, uuid)
        instance.entity_type = "DocumentRef" if entity_type == "Document" else entity_type
        instance.name = name
        instance.description = description
        return instance


class DocumentBindings:
    def __init__(self, doc_id, extracted, source, resolver):
        self.doc_id, self.source, self.resolver = doc_id, source, resolver
        self.entities = {}
        self.resolved = {}
        self.relationship_support = {}
        self.authoritative = "all_entities" in extracted
        for index, entity in enumerate(extracted.get("all_entities") or []):
            name, kind = entity["name"], entity["type"]
            if not isinstance(name, str) or not name.strip() or kind not in ENTITY_TYPES:
                raise ValueError("Invalid accepted identity")
            if source and not verified_spans(name, entity.get("evidence"), source):
                raise ValueError("Accepted entity has no exact source provenance")
            hint = entity.get("identity_hint") or ""
            if hint and (not source or not any(hint in span["quote"] for span in verified_spans(name, entity.get("evidence"), source))):
                raise ValueError("Identity discriminator is not bound to the entity source quote")
            entity_id = entity.get("entity_id") or f"entity-{index}"
            if entity_id in self.entities:
                raise ValueError("Duplicate extraction identity ID")
            self.entities[entity_id] = entity
        self.identity_proofs = []
        for entity in self.entities.values():
            for proof in entity.get("identity_proofs") or []:
                if not isinstance(proof, dict):
                    continue
                left, right = self.entities.get(proof.get("left_id")), self.entities.get(proof.get("right_id"))
                # Extraction IDs preserve qualified identity across windows;
                # reconciliation keeps only the first display capitalization.
                # Do not drop a later denial merely because its case differs.
                if (left and right and left["type"] == right["type"] == proof.get("type")
                        and isinstance(proof.get("left_name"), str) and isinstance(proof.get("right_name"), str)
                        and left["name"].casefold() == proof["left_name"].casefold()
                        and right["name"].casefold() == proof["right_name"].casefold()
                        and valid_identity_receipt(proof, source)
                        and proof not in self.identity_proofs):
                    self.identity_proofs.append(proof)

    async def resolve_all(self):
        # Long explicit forms first, so a new acronym can bind to its expansion
        # without creating an extra UUID. Ties are deterministic, not score based.
        for entity_id in sorted(self.entities, key=lambda key: (-len(self.entities[key]["name"]), key)):
            if entity_id in self.resolved:
                continue
            entity = self.entities[entity_id]
            uuid = await self.resolver.resolve(entity["name"], entity["type"], self.doc_id,
                description=entity.get("description"), source=self.source,
                identity_hint=entity.get("identity_hint") or "",
                identity_proofs=[proof for proof in self.identity_proofs if entity_id in (proof["left_id"], proof["right_id"])],
                name_usage=entity.get("name_usage", ""))
            if not uuid:
                raise ValueError("Accepted identity did not resolve")
            self.resolved[entity_id] = ResolvedEntity(uuid, entity["type"], entity["name"], entity.get("description", ""))

    def lookup(self, name, kind="", entity_id=None):
        if entity_id is not None:
            entity = self.entities.get(entity_id)
            # IDs cannot silently bind a different display name or declared type.
            if not entity or name_key(name, entity["type"]) != name_key(entity["name"], entity["type"]):
                return None
            if kind and kind != entity["type"]:
                return None
            return self.resolved.get(entity_id)
        exact = [key for key, entity in self.entities.items()
                 if name_key(name, entity["type"]) == name_key(entity["name"], entity["type"])]
        typed = [key for key in exact if self.entities[key]["type"] == kind]
        choices = typed or exact
        # A corrected type wins over an old metadata field's type guess, but
        # two supported homonyms cannot be selected from an unqualified name.
        return self.resolved.get(choices[0]) if len(choices) == 1 else None
