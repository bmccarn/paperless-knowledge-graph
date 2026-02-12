import json
import logging
import re

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
    
    "military": """Extract structured metadata from this military document. Return a JSON object with:
{{
  "service_member": "full name of the service member",
  "rank": "military rank (e.g., A1C, SSgt, CPT)",
  "branch": "branch of service (Air Force, Army, Navy, Marines, Coast Guard, Space Force)",
  "unit": "assigned unit or squadron",
  "base": "military installation or base name",
  "date": "document date (YYYY-MM-DD if possible)",
  "document_type": "specific type (DD-214, PCS orders, EPR/OPR, training record, medical, promotion, VA rating decision, VA benefits letter, etc.)",
  "afsc_mos": "AFSC or MOS code if mentioned",
  "period_of_service": "service dates if mentioned",
  "key_details": "brief summary of the document's key information — include any disability rating percentages and decisions",
  "disability_ratings": [
    {{
      "condition": "name of the disability/condition",
      "percentage": "individual rating percentage as a number (e.g. 50)",
      "effective_date": "effective date (YYYY-MM-DD if possible)",
      "status": "service-connected, non-service-connected, permanent, etc."
    }}
  ],
  "combined_rating": "combined/overall disability rating percentage if stated (number only, e.g. 100)",
  "combined_rating_effective_date": "effective date of combined rating (YYYY-MM-DD if possible)",
  "permanent_and_total": "true if the document states permanent and total disability status, false otherwise, null if not mentioned",
  "rating_decisions": [
    {{
      "decision": "brief description of the rating decision",
      "effective_date": "effective date",
      "previous_rating": "previous rating if mentioned",
      "new_rating": "new rating if mentioned"
    }}
  ],
  "benefits": [
    {{
      "benefit_type": "type of benefit (DEA/Chapter 35, CHAMPVA, pension, etc.)",
      "eligibility": "eligible/not eligible",
      "effective_date": "effective date"
    }}
  ],
  "monthly_payment": "monthly payment amount if stated",
  "organizations": [
    {{
      "name": "military organization name",
      "type": "type (squadron, wing, division, command, VA regional office, etc.)"
    }}
  ],
  "locations": [
    {{
      "name": "location name (base, city, country)",
      "context": "context (stationed, deployed, TDY, etc.)"
    }}
  ],
  "conditions": [
    {{
      "name": "medical condition or disability name",
      "status": "service-connected/non-service-connected/pending",
      "details": "any additional details (permanent, static, etc.)"
    }}
  ]
}}

CRITICAL extraction rules for VA/military documents:
- The COMBINED/TOTAL disability rating is the most important field. Search the ENTIRE document for it.
- Look for: "increased your rating to X percent", "combined evaluation of X percent", "total disability rating of X percent", "rating to 100 percent"
- If the document mentions "permanent and total disability status" or "DEA/Chapter 35 eligibility", the combined rating is almost certainly 100%.
- Extract INDIVIDUAL condition ratings separately in disability_ratings array.
- The combined_rating field should be the FINAL overall combined percentage, NOT an individual condition percentage.
- If a document says a condition was increased to 50% AND that this increased the overall rating to 100%, then combined_rating = "100", NOT "50".
- "Permanent and total" status is critical — set permanent_and_total to "true" if mentioned ANYWHERE in the document.
- Effective dates for all ratings and benefit changes.
- Monthly payment amounts.
- Any CHANGES to ratings (increases, decreases, new grants).

Extract all information present. Use null for missing fields. Military abbreviations should be preserved as-is.

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

# Pass 2 prompt: Entity extraction with precision focus, few-shot examples, and exclusion rules
ENTITY_EXTRACTION_PROMPT = """You are an entity extraction system for a knowledge graph. Extract only NAMED, SPECIFIC entities from this document. Prefer precision over recall — when in doubt, skip it.

Valid entity types: Person, Organization, Location, System, Product, Document, Event, Condition

=== FEW-SHOT EXAMPLE ===

DOCUMENT SNIPPET:
"John Doe visited Dr. Sarah Johnson at Quest Diagnostics on March 15, 2024. His CBC panel results showed elevated WBC. He was diagnosed with PTSD and prescribed Gabapentin 300mg. His DD-214 confirms service during Operation Desert Storm. He filed his claim through eBenefits."

CORRECT entity extractions:
- "John Doe" (Person) — named individual, full name
- "Dr. Sarah Johnson" (Person) — named individual
- "Quest Diagnostics" (Organization) — named company
- "Charlotte, NC" (Location) — specific named place
- "PTSD" (Condition) — named medical condition
- "Gabapentin 300mg" (Product) — specific named product
- "DD-214" (Document) — specific named form
- "Operation Desert Storm" (Event) — specific named event
- "eBenefits" (System) — named software platform

WRONG extractions to AVOID:
- "the patient" → generic role, not a named person
- "tax preparer" → generic role without a proper name
- "VR&E Officer" → generic role title, not a specific person
- "30 percent" → a numeric value, not an entity
- "90% combined rating" → a numeric value, not an entity
- "your area" → vague reference, not a specific place
- "the facility" → generic reference, not named
- "Certification Issue Date" → form field label, not an entity
- "Date of Issue" → form field label
- "Direct Review" → procedural term, not an event
- "Evidence Submission" → process description, not an event
- "Section 3" → document section header
- "How VA Combines Percentages" → descriptive phrase
- "psychiatric care" → generic concept, not a product
- "anxiety medications" → generic category, not a named product
- "disaster area" → generic phrase, not a specific location
- "lab results" → generic concept, not a specific document
- "blood draw" → procedure description, not an entity

=== DO NOT EXTRACT ===
- Generic roles without proper names ("tax preparer", "officer", "physician", "VR&E Officer", "Veterans Law Judge")
- Form field labels ("Date of Issue", "Reference Number", "Certification Issue Date", "Certification Expiration Date")
- Percentages, dollar amounts, or numeric values ("30 percent", "90% combined rating", "$1,500")
- Vague/generic locations ("your area", "the facility", "disaster area", "the hospital")
- Process descriptions or procedural terms ("Direct Review", "Evidence Submission", "Hearing", "background investigations")
- Document section headers ("Section 3", "Part A", "Chapter 2")
- Descriptive phrases that aren't proper nouns ("How VA Combines Percentages", "psychiatric care", "anxiety medications")
- Common English words or generic nouns

=== TYPE-BY-TYPE EXTRACTION GUIDANCE ===

Think through each type systematically:

1. PERSON: Named individuals ONLY. Must have a proper name (first name, last name, or both).
   YES: "John Doe", "Dr. Sarah Johnson", "John A. Smith"
   NO: "tax preparer", "VR&E Officer", "the physician", "Veteran", "applicant"

2. ORGANIZATION: Named entities with proper names.
   YES: "Quest Diagnostics", "Department of Veterans Affairs", "Bank of America", "82nd Airborne Division"
   NO: "the hospital", "the bank", "insurance company", "military unit"

3. LOCATION: Specific named places only.
   YES: "Charlotte, NC", "Fort Bragg", "Walter Reed Medical Center"
   NO: "your area", "the facility", "disaster area", "home address"

4. CONDITION: Named medical conditions, diagnoses, symptoms, injuries, disabilities.
   YES: "PTSD", "sleep apnea", "lumbar strain", "migraine headaches", "anxiety disorder"
   NO: "feeling tired", "pain", "symptoms", "health issues"

5. DOCUMENT: Specific named forms, documents, or publications.
   YES: "DD-214", "SF-86", "Form W-2", "VA Form 21-526EZ"
   NO: "the form", "application", "the letter", "paperwork"

6. PRODUCT: Named commercial products, medications with specific names/dosages, named services.
   YES: "Gabapentin 300mg", "Sertraline 50mg", "CPAP machine"
   NO: "anxiety medications", "pain pills", "medical equipment"

7. SYSTEM: Named software, databases, or platforms.
   YES: "eBenefits", "MyHealtheVet", "VBMS", "e-QIP"
   NO: "the website", "online portal", "the database"

8. EVENT: Specific named events, operations, or incidents with identifiable names/dates.
   YES: "Operation Desert Storm", "9/11 attacks", "Hurricane Katrina"
   NO: "Direct Review", "Evidence Submission", "Hearing", "the appointment"

=== CANONICAL NAME GUIDANCE ===
Use the most complete, properly-cased form of each name found in the document:
- Prefer full names over abbreviated forms (e.g. "John Doe" over "DOE")
- Prefer "Department of Veterans Affairs" over "VA" (unless VA is the only form used)
- Prefer "Charlotte, NC" over "charlotte" or "CHARLOTTE NC"

Return a JSON object:
{{
  "entities": [
    {{
      "name": "entity name (canonical form)",
      "type": "Person/Organization/Location/System/Product/Document/Event/Condition",
      "confidence": 0.95,
      "description": "brief description of the entity in context"
    }}
  ]
}}

Only include entities with confidence >= 0.8. If unsure about an entity, skip it entirely.

Document title: {title}

Structured metadata from Pass 1:
{metadata}

Raw document content:
{content}"""

# Pass 3 prompt: Relationship inference with constrained patterns
RELATIONSHIP_EXTRACTION_PROMPT = """Infer relationships between the provided entities based on document context.

IMPORTANT RULES:
- Only create relationships between entities that exist in the entity list below. Do NOT invent new entities.
- Only infer relationships that can be reasonably inferred from the document context.
- Include confidence scores based on how explicit the relationship is in the document.

=== VALID RELATIONSHIP PATTERNS ===

These are the allowed source_type → relationship → target_type patterns:

- Person → WORKS_AT / EMPLOYED_BY → Organization
- Person → PATIENT_OF / TREATED_BY → Organization
- Person → LOCATED_AT / LIVES_IN → Location
- Person → DIAGNOSED_WITH / HAS_CONDITION → Condition
- Person → PRESCRIBED / TAKES → Product
- Person → SERVED_IN / PARTICIPATED_IN → Event
- Person → USES → System
- Person → FILED / SUBMITTED → Document
- Organization → LOCATED_IN → Location
- Organization → PROVIDES → Product
- Document → AUTHORED_BY / SIGNED_BY → Person
- Document → ISSUED_BY / FROM → Organization
- Document → REFERENCES → Document
- Product → TREATS → Condition

Relationship types should be UPPER_SNAKE_CASE (Neo4j compatible).
You may also use: RELATED_TO as a generic fallback, but prefer specific types.

Return a JSON object with:
{{
  "relationships": [
    {{
      "from_entity": "source entity name (must match entity list exactly)",
      "to_entity": "target entity name (must match entity list exactly)", 
      "relationship_type": "RELATIONSHIP_TYPE",
      "confidence": 0.8,
      "description": "brief explanation of why this relationship exists"
    }}
  ]
}}

Document title: {title}

Entities from Pass 2:
{entities}

Document context:
{content}"""

# Pass 4 prompt: Verification/critique of extracted entities
VERIFICATION_PROMPT = """You are a quality reviewer for a knowledge graph entity extraction system. Review the following entity list extracted from a document and REMOVE any that are not real, specific, named entities.

REMOVE entities that are:
1. Generic descriptions rather than named entities (e.g., "tax preparer", "the physician", "VR&E Officer")
2. Form field labels (e.g., "Date of Issue", "Certification Issue Date", "Reference Number")
3. Numbers, percentages, or dollar amounts masquerading as entities (e.g., "30 percent", "90% combined rating")
4. Vague references (e.g., "your area", "the facility", "disaster area")
5. Process descriptions or procedural terms (e.g., "Direct Review", "Evidence Submission")
6. Document section headers (e.g., "Section 3", "Part A")
7. Descriptive phrases that aren't proper nouns (e.g., "How VA Combines Percentages")
8. Duplicates or near-duplicates (keep the most complete version)

KEEP entities that are:
- Real named people (with actual proper names)
- Specific named organizations, companies, agencies
- Specific named places (cities, bases, buildings)
- Named medical conditions and diagnoses
- Named products/medications with specific names
- Specific named documents/forms (DD-214, SF-86, etc.)
- Named software systems/platforms
- Named events/operations

Document title: {title}

Entity list to review:
{entities}

Return a JSON object with ONLY the validated entities (remove all junk):
{{
  "entities": [
    {{
      "name": "entity name",
      "type": "entity type",
      "confidence": 0.95,
      "description": "description"
    }}
  ]
}}"""


def _repair_json(raw_text: str) -> dict:
    """Attempt to parse and repair common JSON issues from LLM output."""
    if not raw_text or not raw_text.strip():
        return {}
    
    text = raw_text.strip()
    
    # 1. Try standard parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # 2. Strip markdown code fences
    text = re.sub(r'^```(?:json)?\s*\n?', '', text, flags=re.MULTILINE)
    text = re.sub(r'\n?```\s*$', '', text, flags=re.MULTILINE)
    text = text.strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # 3. Fix trailing commas before } or ]
    text = re.sub(r',\s*([}\]])', r'\1', text)
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # 4. Try to fix single quotes to double quotes (carefully)
    # Only do this if there are no double quotes at all (suggesting single-quote JSON)
    if '"' not in text and "'" in text:
        fixed = text.replace("'", '"')
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass
    
    # 5. Try extracting the first JSON object from the text
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            candidate = match.group(0)
            # Fix trailing commas in the extracted object too
            candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    
    # Give up - raise with context
    raise json.JSONDecodeError(f"Failed to repair JSON", raw_text[:200], 0)


class EntityExtractor:
    def __init__(self):
        self.client = AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
        self.model = settings.gemini_model

    async def extract(self, title: str, content: str, doc_type: str) -> dict:
        """Extract entities and relationships using 4-pass pipeline."""
        # Pass 1: Structured Metadata Extraction
        metadata = await self._pass1_metadata_extraction(title, content, doc_type)
        logger.debug(f"Pass 1 completed for '{title}' (type: {doc_type})")
        
        # Pass 2: Entity Extraction & Typing
        entities = await self._pass2_entity_extraction(title, content, metadata)
        entity_count = len(entities.get("entities", []))
        logger.debug(f"Pass 2 extracted {entity_count} entities for '{title}'")
        
        # Pass 4: Verification (critique & refine) - runs between pass 2 and pass 3
        verified_entities = await self._pass4_verification(title, entities)
        verified_count = len(verified_entities.get("entities", []))
        if verified_count < entity_count:
            logger.info(f"Pass 4 verification removed {entity_count - verified_count} junk entities for '{title}' ({entity_count} -> {verified_count})")
        
        # Pass 3: Relationship Inference (uses verified entities)
        relationships = await self._pass3_relationship_extraction(title, content, verified_entities)
        rel_count = len(relationships.get("relationships", []))
        logger.debug(f"Pass 3 inferred {rel_count} relationships for '{title}'")
        
        # Combine results in format expected by pipeline.py
        result = self._combine_results(metadata, verified_entities, relationships)
        result["extraction_method"] = "4-pass"
        
        return result

    async def _pass1_metadata_extraction(self, title: str, content: str, doc_type: str) -> dict:
        """Pass 1: Extract structured metadata specific to document type."""
        prompt_template = METADATA_EXTRACTION_PROMPTS.get(doc_type, GENERIC_METADATA_PROMPT)
        truncated = content[:30000]
        prompt = prompt_template.format(title=title, content=truncated)

        async def _call():
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            return _repair_json(response.choices[0].message.content)

        return await retry_with_backoff(_call, operation=f"pass1_metadata:{doc_type}")

    async def _pass2_entity_extraction(self, title: str, content: str, metadata: dict) -> dict:
        """Pass 2: Extract and type all entities."""
        truncated = content[:30000]
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
            return _repair_json(response.choices[0].message.content)

        return await retry_with_backoff(_call, operation="pass2_entities")

    async def _pass3_relationship_extraction(self, title: str, content: str, entities: dict) -> dict:
        """Pass 3: Infer relationships between entities."""
        truncated = content[:20000]  # Leave room for entity list
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
            return _repair_json(response.choices[0].message.content)

        return await retry_with_backoff(_call, operation="pass3_relationships")

    async def _pass4_verification(self, title: str, entities: dict) -> dict:
        """Pass 4: Verify and filter extracted entities (Extract-Critique-Refine pattern)."""
        entity_list = entities.get("entities", [])
        if not entity_list:
            return entities
        
        # Skip verification for very small entity lists (nothing to filter)
        if len(entity_list) <= 3:
            return entities
        
        entities_str = json.dumps(entity_list, indent=2)
        prompt = VERIFICATION_PROMPT.format(
            title=title,
            entities=entities_str,
        )

        async def _call():
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            return _repair_json(response.choices[0].message.content)

        try:
            verified = await retry_with_backoff(_call, operation="pass4_verification")
            # Sanity check: verification should not ADD entities, only remove them
            verified_names = {e.get("name", "").lower() for e in verified.get("entities", [])}
            original_names = {e.get("name", "").lower() for e in entity_list}
            # If verification added new entities, that's wrong - fall back to original
            new_entities = verified_names - original_names
            if new_entities:
                logger.warning(f"Pass 4 tried to add new entities: {new_entities} — using original list")
                return entities
            return verified
        except Exception as e:
            logger.warning(f"Pass 4 verification failed: {e} — using unverified entities")
            return entities

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


extractor = EntityExtractor()
