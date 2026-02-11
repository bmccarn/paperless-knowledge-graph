import json
import logging

from openai import AsyncOpenAI

from app.config import settings
from app.retry import retry_with_backoff

logger = logging.getLogger(__name__)

# Pass 1 prompts: Focus only on structured metadata extraction per doc type
METADATA_EXTRACTION_PROMPTS = {
    "medical_lab": """Extract structured metadata from this medical lab document. Return a JSON object with:
{{
  "provider": "name of lab/healthcare provider",
  "patient_name": "patient full name",
  "date": "date of results (YYYY-MM-DD if possible)",
  "tests": [
    {{
      "name": "test name",
      "value": "result value", 
      "unit": "unit of measurement",
      "reference_range": "normal range",
      "flag": "H/L/normal or null"
    }}
  ],
  "diagnoses": ["list of diagnoses if mentioned"],
  "ordering_physician": "physician name if mentioned"
}}

Extract all information present. Use null for missing fields. Be thorough with test results.

Document title: {title}
Document content:
{content}""",
    
    "financial_invoice": """Extract structured metadata from this financial/invoice document. Return a JSON object with:
{{
  "vendor": "vendor/company name",
  "invoice_number": "invoice or receipt number", 
  "date": "invoice date (YYYY-MM-DD if possible)",
  "due_date": "due date if mentioned (YYYY-MM-DD if possible)",
  "total_amount": "total amount as number",
  "currency": "currency code (USD, EUR, etc.)",
  "line_items": [
    {{
      "description": "item description",
      "amount": "item amount as number"
    }}
  ],
  "payment_status": "paid/unpaid/partial if mentioned"
}}

Extract all information present. Use null for missing fields.

Document title: {title}
Document content:
{content}""",
    
    "legal_contract": """Extract structured metadata from this legal/contract document. Return a JSON object with:
{{
  "parties": [
    {{
      "name": "party name",
      "role": "role in contract (e.g., buyer, seller, licensor)"
    }}
  ],
  "contract_type": "type of contract",
  "effective_date": "start date (YYYY-MM-DD if possible)",
  "expiration_date": "end date if mentioned (YYYY-MM-DD if possible)", 
  "terms_summary": "brief summary of key terms",
  "obligations": [
    {{
      "party": "party name",
      "obligation": "description of obligation"
    }}
  ],
  "renewal_info": "renewal terms if mentioned"
}}

Extract all information present. Use null for missing fields.

Document title: {title}
Document content:
{content}""",
    
    "insurance": """Extract structured metadata from this insurance document. Return a JSON object with:
{{
  "provider": "insurance company name",
  "policy_number": "policy number",
  "policyholder": "policyholder name",
  "coverage_type": "type of coverage (health, auto, home, life, etc.)",
  "premium": "premium amount as number",
  "effective_date": "start date (YYYY-MM-DD if possible)",
  "expiration_date": "end date (YYYY-MM-DD if possible)",
  "covered_items": ["list of covered items or categories"]
}}

Extract all information present. Use null for missing fields.

Document title: {title}
Document content:
{content}""",
    
    "government_tax": """Extract structured metadata from this tax/government document. Return a JSON object with:
{{
  "form_type": "form type (W-2, 1099, 1040, etc.)",
  "tax_year": "tax year",
  "filer_name": "name of the filer",
  "filing_status": "filing status if mentioned",
  "total_income": "total income as number",
  "tax_owed": "tax owed as number", 
  "tax_paid": "tax paid as number",
  "preparer": "tax preparer name if mentioned"
}}

Extract all information present. Use null for missing fields.

Document title: {title}
Document content:
{content}""",
    
    "property_home": """Extract structured metadata from this property/home document. Return a JSON object with:
{{
  "property_address": "full property address",
  "parties": [
    {{
      "name": "party name", 
      "role": "role (buyer, seller, owner, inspector, etc.)"
    }}
  ],
  "document_type": "specific type (deed, inspection, mortgage, etc.)",
  "date": "document date (YYYY-MM-DD if possible)",
  "amount": "monetary amount if applicable as number",
  "description": "brief description of the document purpose"
}}

Extract all information present. Use null for missing fields.

Document title: {title}
Document content:
{content}""",
}

GENERIC_METADATA_PROMPT = """Extract structured metadata from this document. Return a JSON object with:
{{
  "people": [
    {{
      "name": "person's full name",
      "role": "their role or relationship to the document"
    }}
  ],
  "organizations": [
    {{
      "name": "organization name",
      "type": "type of organization"
    }}
  ],
  "dates": [
    {{
      "date": "date value (YYYY-MM-DD if possible)",
      "description": "what this date represents"
    }}
  ],
  "key_facts": ["list of key facts or data points"],
  "summary": "brief summary of the document"
}}

Extract all information present. Use null for missing fields. Be thorough.

Document title: {title}
Document content:
{content}"""

# Pass 2 prompt: Universal entity extraction and typing
ENTITY_EXTRACTION_PROMPT = """Extract and properly type ALL entities from this document. You will receive the raw document text and structured metadata from Pass 1.

Valid entity types: Person, Organization, Location, System, Product, Document, Event

Return a JSON object with:
{{
  "entities": [
    {{
      "name": "entity name",
      "type": "Person/Organization/Location/System/Product/Document/Event",
      "confidence": 0.95,
      "description": "brief description of the entity"
    }}
  ]
}}

Entity typing guidelines:
- Person: Individual people (patients, employees, signatories, etc.)
- Organization: Companies, hospitals, agencies, departments, etc.
- Location: Cities, states, countries, addresses, buildings
- System: Databases, software systems, platforms, applications
- Product: Specific products, services, plans, policies
- Document: Referenced documents, forms, reports
- Event: Meetings, appointments, procedures, transactions

Be thorough - identify ALL entities mentioned. A database should be System, a city should be Location, etc.
Include high confidence scores (0.8+) for clearly identifiable entities.

Document title: {title}

Structured metadata from Pass 1:
{metadata}

Raw document content:
{content}"""

# Pass 3 prompt: Relationship inference
RELATIONSHIP_EXTRACTION_PROMPT = """Infer relationships between entities based on document context. You will receive the entity list from Pass 2 and the document context.

Return a JSON object with:
{{
  "relationships": [
    {{
      "from_entity": "source entity name",
      "to_entity": "target entity name", 
      "relationship_type": "RELATIONSHIP_TYPE",
      "confidence": 0.8,
      "description": "brief explanation of why this relationship exists"
    }}
  ]
}}

Relationship types should be UPPER_SNAKE_CASE (Neo4j compatible). Common types:
- WORKS_AT, EMPLOYED_BY, EMPLOYS
- PATIENT_OF, TREATED_BY, PROVIDES_CARE_TO  
- CUSTOMER_OF, VENDOR_FOR, SERVES
- OWNS, OWNED_BY, MANAGES
- LOCATED_AT, LOCATED_IN
- AUTHORED_BY, CREATED_BY
- INSURED_BY, COVERS
- CONTRACTED_WITH, PARTY_TO
- RELATED_TO (generic fallback)

Only infer relationships that can be reasonably inferred from the document context.
Include confidence scores based on how explicit the relationship is in the document.

Document title: {title}

Entities from Pass 2:
{entities}

Document context:
{content}"""

FALLBACK_PROMPT = """This document may be difficult to parse. Extract whatever basic information you can find.
Return a JSON object with:
{{
  "document_type": "best guess at what type of document this is",
  "people": ["list of any person names mentioned"],
  "organizations": ["list of any organization names mentioned"],
  "dates": ["list of any dates mentioned"],
  "summary": "one-sentence description of what this document appears to be",
  "confidence": 0.5
}}

Be forgiving of OCR errors and formatting issues. Extract anything identifiable.

Document title: {title}
Document content:
{content}"""


class EntityExtractor:
    def __init__(self):
        self.client = AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
        self.model = settings.gemini_model

    async def extract(self, title: str, content: str, doc_type: str) -> dict:
        """Extract entities and relationships using 3-pass pipeline."""
        try:
            # Pass 1: Structured Metadata Extraction
            metadata = await self._pass1_metadata_extraction(title, content, doc_type)
            if not metadata:
                logger.warning(f"Pass 1 failed for '{title}', falling back")
                return await self._fallback_extract(title, content)
                
            # Pass 2: Entity Extraction & Typing
            entities = await self._pass2_entity_extraction(title, content, metadata)
            if not entities:
                logger.warning(f"Pass 2 failed for '{title}', falling back") 
                return await self._fallback_extract(title, content)
                
            # Pass 3: Relationship Inference
            relationships = await self._pass3_relationship_extraction(title, content, entities)
            
            # Combine results in format expected by pipeline.py
            result = self._combine_results(metadata, entities, relationships)
            result["extraction_method"] = "3-pass"
            
            return result
            
        except Exception as e:
            logger.error(f"3-pass extraction failed for '{title}': {e}")
            return await self._fallback_extract(title, content)

    async def _pass1_metadata_extraction(self, title: str, content: str, doc_type: str) -> dict:
        """Pass 1: Extract structured metadata specific to document type."""
        prompt_template = METADATA_EXTRACTION_PROMPTS.get(doc_type, GENERIC_METADATA_PROMPT)
        truncated = content[:8000]
        prompt = prompt_template.format(title=title, content=truncated)

        async def _call():
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            return json.loads(response.choices[0].message.content)

        try:
            result = await retry_with_backoff(_call, operation=f"pass1_metadata:{doc_type}")
            logger.debug(f"Pass 1 completed for '{title}' (type: {doc_type})")
            return result
        except Exception as e:
            logger.error(f"Pass 1 metadata extraction failed for '{title}': {e}")
            return {}

    async def _pass2_entity_extraction(self, title: str, content: str, metadata: dict) -> dict:
        """Pass 2: Extract and type all entities."""
        truncated = content[:8000] 
        metadata_str = json.dumps(metadata, indent=2)
        prompt = ENTITY_EXTRACTION_PROMPT.format(
            title=title, 
            metadata=metadata_str, 
            content=truncated
        )

        async def _call():
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            return json.loads(response.choices[0].message.content)

        try:
            result = await retry_with_backoff(_call, operation="pass2_entities")
            entity_count = len(result.get("entities", []))
            logger.debug(f"Pass 2 extracted {entity_count} entities for '{title}'")
            return result
        except Exception as e:
            logger.error(f"Pass 2 entity extraction failed for '{title}': {e}")
            return {}

    async def _pass3_relationship_extraction(self, title: str, content: str, entities: dict) -> dict:
        """Pass 3: Infer relationships between entities."""
        truncated = content[:6000]  # Leave room for entity list
        entities_str = json.dumps(entities.get("entities", []), indent=2)
        prompt = RELATIONSHIP_EXTRACTION_PROMPT.format(
            title=title,
            entities=entities_str,
            content=truncated
        )

        async def _call():
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            return json.loads(response.choices[0].message.content)

        try:
            result = await retry_with_backoff(_call, operation="pass3_relationships")
            rel_count = len(result.get("relationships", []))
            logger.debug(f"Pass 3 inferred {rel_count} relationships for '{title}'")
            return result
        except Exception as e:
            logger.error(f"Pass 3 relationship extraction failed for '{title}': {e}")
            return {"relationships": []}  # Non-critical failure

    def _combine_results(self, metadata: dict, entities: dict, relationships: dict) -> dict:
        """Combine 3-pass results into format expected by pipeline.py."""
        result = dict(metadata)  # Start with metadata
        
        # Convert entities to people/organizations format for backward compatibility
        people = []
        organizations = []
        all_entities = entities.get("entities", [])
        
        for entity in all_entities:
            entity_type = entity.get("type", "")
            name = entity.get("name", "")
            if not name:
                continue
                
            if entity_type == "Person":
                people.append({
                    "name": name,
                    "role": entity.get("description", ""),
                    "confidence": entity.get("confidence", 0.8)
                })
            elif entity_type == "Organization":
                organizations.append({
                    "name": name, 
                    "type": entity.get("description", ""),
                    "confidence": entity.get("confidence", 0.8)
                })
        
        # Add people/organizations to result for pipeline compatibility
        if people:
            result["people"] = people
        if organizations:
            result["organizations"] = organizations
            
        # Convert relationships to implied_relationships format
        implied_relationships = []
        for rel in relationships.get("relationships", []):
            from_entity = rel.get("from_entity", "")
            to_entity = rel.get("to_entity", "")
            if not from_entity or not to_entity:
                continue
                
            # Find entity types from the entities list
            from_type = self._find_entity_type(from_entity, all_entities)
            to_type = self._find_entity_type(to_entity, all_entities)
            
            implied_relationships.append({
                "from_entity": from_entity,
                "from_type": from_type,
                "to_entity": to_entity, 
                "to_type": to_type,
                "relationship": rel.get("relationship_type", "RELATED_TO"),
                "confidence": rel.get("confidence", 0.7)
            })
        
        if implied_relationships:
            result["implied_relationships"] = implied_relationships
            
        # Store all entities for enhanced processing
        result["all_entities"] = all_entities
        
        # Add overall confidence
        entity_confidences = [e.get("confidence", 0.8) for e in all_entities if e.get("confidence")]
        if entity_confidences:
            result["confidence"] = sum(entity_confidences) / len(entity_confidences)
        else:
            result["confidence"] = 0.8
            
        return result
    
    def _find_entity_type(self, entity_name: str, entities: list) -> str:
        """Find the type of an entity from the entities list."""
        for entity in entities:
            if entity.get("name", "") == entity_name:
                return entity.get("type", "Person")
        # Fallback heuristics
        if any(w in entity_name.lower() for w in ["inc", "llc", "corp", "dept", "department", "agency", "company", "bank", "university"]):
            return "Organization"
        return "Person"

    async def _fallback_extract(self, title: str, content: str) -> dict:
        """Simpler fallback extraction for docs that fail 3-pass pipeline."""
        truncated = content[:4000]
        prompt = FALLBACK_PROMPT.format(title=title, content=truncated)

        try:
            async def _call():
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                )
                return json.loads(response.choices[0].message.content)

            result = await retry_with_backoff(_call, operation="fallback_extract")
            logger.info(f"Fallback extraction succeeded for '{title}'")

            # Convert fallback format to generic format
            converted = {
                "people": [],
                "organizations": [],
                "dates": [],
                "summary": result.get("summary", ""),
                "confidence": result.get("confidence", 0.3),
                "fallback_extraction": True,
                "extraction_method": "fallback"
            }
            for name in (result.get("people") or []):
                if isinstance(name, str) and name.strip():
                    converted["people"].append({"name": name.strip(), "role": "", "confidence": 0.4})
            for name in (result.get("organizations") or []):
                if isinstance(name, str) and name.strip():
                    converted["organizations"].append({"name": name.strip(), "type": "", "confidence": 0.4})
            for date_str in (result.get("dates") or []):
                if isinstance(date_str, str) and date_str.strip():
                    converted["dates"].append({"date": date_str.strip(), "description": ""})

            return converted
        except Exception as e:
            logger.error(f"Fallback extraction also failed: {e}")
            return {}


extractor = EntityExtractor()