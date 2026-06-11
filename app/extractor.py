import json
import logging
import re
from typing import Any

from openai import AsyncOpenAI

from app.config import settings
from app.retry import retry_with_backoff

logger = logging.getLogger(__name__)


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        for item in value:
            text = _coerce_text(item)
            if text:
                return text
        return ""
    if isinstance(value, dict):
        return " ".join(_coerce_text(item) for item in value.values()).strip()
    return str(value).strip()

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
ENTITY_EXTRACTION_PROMPT = """You are an entity extraction system for a personal knowledge graph. Extract only NAMED, SPECIFIC entities from this document. Prefer precision over recall — when in doubt, skip it.

Valid entity types:
- **Person**: Named individuals with proper names (first/last/both)
- **Organization**: Named companies, agencies, institutions, military units, banks, law firms
- **Location**: Named cities, states, countries, military bases, buildings, addresses
- **Condition**: Named medical conditions, diagnoses, disabilities, symptoms
- **Product**: Named commercial products, medications with dosages, specific equipment
- **System**: Named software platforms, databases, portals, websites
- **Event**: Named events, operations, incidents, wars, disasters (NOT generic processes)
- **Document**: Named specific forms, publications, regulations (DD-214, Form W-2, SF-86)
- **FinancialItem**: Named accounts, funds, specific financial instruments, tax line items with amounts
- **InsurancePolicy**: Named insurance plans, coverage types with policy numbers
- **Contract**: Named agreements, leases, service contracts between specific parties
- **DateEvent**: Specific named periods, deadlines, or milestones (NOT raw dates like "2025-01-15")
- **Address**: Full mailing/physical addresses (street + city/state/zip)

=== FEW-SHOT EXAMPLE ===

DOCUMENT SNIPPET:
"Blake McCarn filed Form 1040 for tax year 2025, prepared by Michael T. Dulin, CPA, PA in Matthews, NC. Total income of $468,173 from AllCloud ($236,128 W-2), Wells Fargo ($98,773 W-2), and RapidRoute Solutions LLC ($133,214 S-Corp). Mortgage interest of $30,072 paid to PHH Mortgage. Federal tax liability: $72,545."

CORRECT extractions:
- "Blake McCarn" (Person) — taxpayer/filer
- "Michael T. Dulin, CPA, PA" (Organization) — tax preparation firm
- "Matthews, NC" (Location) — preparer location
- "AllCloud" (Organization) — employer
- "Wells Fargo" (Organization) — employer
- "RapidRoute Solutions LLC" (Organization) — S-Corp business
- "PHH Mortgage" (Organization) — mortgage lender
- "Form 1040" (Document) — specific tax form
- "Form W-2" (Document) — wage statement

WRONG extractions to AVOID:
- "the taxpayer" → generic role, not a named person
- "tax preparer" → generic role without a proper name
- "30 percent" → a numeric value, not an entity
- "your area" → vague reference, not a specific place
- "Certification Issue Date" → form field label
- "Direct Review" → procedural term, not an event
- "$468,173" → raw dollar amount, not an entity
- "2025-01-15" → raw date, not a DateEvent
- "psychiatric care" → generic concept, not a product
- "lab results" → generic concept, not a document

=== DO NOT EXTRACT ===
- Generic roles without proper names ("tax preparer", "officer", "physician")
- Form field labels ("Date of Issue", "Reference Number", "Line 24")
- Raw percentages, dollar amounts, or numeric values ("30 percent", "$72,545")
- Vague/generic locations ("your area", "the facility")
- Process descriptions or procedural terms ("Direct Review", "Evidence Submission")
- Document section headers ("Section 3", "Part A")
- Descriptive phrases that aren't proper nouns
- Raw dates ("2025-01-15", "January 15, 2026") — only extract named periods/milestones
- Common English words or generic nouns
- IRS form numbers that appear only as checkbox references or "attach if applicable" lines (e.g., "Form 2441", "Form 8839" when they only appear on a 1040 checklist without actual filed data)

=== TYPE-BY-TYPE EXTRACTION GUIDANCE ===

1. **Person**: Must have a proper name. YES: "John Doe", "Dr. Sarah Johnson". NO: "the physician", "applicant"
2. **Organization**: Named entities with proper names. YES: "AllCloud", "USAA", "82nd Airborne Division". NO: "the bank", "insurance company"
3. **Location**: Specific named places. YES: "Charlotte, NC", "Fort Bragg". NO: "your area", "the facility"
4. **Condition**: Named medical conditions. YES: "PTSD", "sleep apnea", "lumbar strain". NO: "pain", "symptoms"
5. **Product**: Named products/medications. YES: "Gabapentin 300mg", "CPAP machine". NO: "anxiety medications"
6. **System**: Named software/platforms. YES: "eBenefits", "MyHealtheVet". NO: "the website", "online portal"
7. **Event**: Specific named events. YES: "Operation Desert Storm". NO: "the appointment", "Hearing"
8. **Document**: Specific named forms that were actually filed or contain data. YES: "DD-214", "Form W-2", "Schedule A (Form 1040)" (when itemized deductions are present). NO: "the form", "paperwork", "Form 2441" (when only referenced as a checkbox on 1040)
9. **FinancialItem**: When a document contains a specific financial figure tied to a named source, extract the SOURCE as an Organization, NOT the dollar amount as an entity.
   - For tax returns: Only extract forms/schedules that were ACTUALLY FILED (have data filled in). Do NOT extract form numbers that appear only as checkbox references, line references, or "see instructions" mentions on Form 1040. For example, if Schedule A has itemized deductions filled in, extract it. If Form 2441 appears only as "Attach Form 2441" with no data, skip it.
10. **Address**: Full street addresses only. YES: "5589 Galloway Drive, Midland, NC 28107". NO: "NC", "28107"
11. **Contract**: Only extract if a specific agreement is named. YES: "Deed of Trust #2024-001234". NO: "the agreement"
12. **DateEvent**: Only named milestones. YES: "Gulf War Era", "2025 Filing Season". NO: "January 15, 2025"

=== CANONICAL NAME GUIDANCE ===
Use the most complete, properly-cased form found in the document:
- Prefer full names: "John A. Doe" over "DOE"
- Prefer "Department of Veterans Affairs" over "VA" (unless VA is the only form used)
- Prefer "Charlotte, NC" over "charlotte" or "CHARLOTTE NC"

Return a JSON object:
{{{{
  "entities": [
    {{{{
      "name": "entity name (canonical form)",
      "type": "Person/Organization/Location/System/Product/Document/Event/Condition/FinancialItem/InsurancePolicy/Contract/DateEvent/Address",
      "confidence": 0.95,
      "description": "brief description of the entity in context"
    }}}}
  ]
}}}}

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
- Only infer relationships that can be reasonably supported by the document context.
- Include confidence scores based on how explicit the relationship is in the document.
- Use UPPER_SNAKE_CASE for relationship types (Neo4j compatible).

=== RELATIONSHIP GUIDANCE ===

Use consistent, general, and timeless relationship types. Prefer specific types over generic ones.

**Employment & Roles:**
- WORKS_AT, EMPLOYED_BY — person works at an organization
- OFFICER_OF, OWNER_OF — person has ownership/officer role
- PREPARED_BY — document prepared by a person or firm
- SIGNED_BY, AUTHORIZED_BY — document signed/authorized by person

**Financial:**
- PAID_TO, PAID_BY — payment between parties
- BILLED_BY, INVOICED_BY — billing relationship
- WITHHELD_BY — tax withholding by employer
- MORTGAGE_WITH, LOAN_FROM — lending relationship
- PREMIUM_PAID_TO — insurance premium payments
- INCOME_FROM — income source relationship

**Medical & Health:**
- PATIENT_OF, TREATED_BY — patient-provider relationship
- DIAGNOSED_WITH, HAS_CONDITION — person has medical condition
- PRESCRIBED, TAKES — medication relationship
- ORDERED_BY — test ordered by physician
- RESULTED_IN — test resulted in finding

**Military & Government:**
- SERVED_IN, PARTICIPATED_IN — military service
- STATIONED_AT, DEPLOYED_TO — location assignment
- ASSIGNED_TO — unit assignment
- BRANCH_OF_SERVICE — service branch
- RATED_AT — disability rating
- FILED_WITH, SUBMITTED_TO — filing relationship
- ISSUED_BY — document issued by agency

**Legal & Contracts:**
- PARTY_TO, CONTRACTED_WITH — contract parties
- COVERS, INSURES — insurance coverage
- GOVERNS, APPLIES_TO — regulatory relationship
- EFFECTIVE_FROM, EXPIRES_ON — temporal bounds

**Location & Association:**
- LOCATED_IN, LOCATED_AT — physical location
- LIVES_IN, RESIDES_AT — residential location
- HEADQUARTERS_IN — organization HQ
- MAILING_ADDRESS — address association

**Document:**
- REFERENCES, SUPERSEDES — document cross-references
- ATTACHMENT_TO, SUPPLEMENT_TO — document hierarchy
- AMENDS, CORRECTS — document revisions

**General (use sparingly):**
- RELATED_TO — only when no specific type fits
- ASSOCIATED_WITH — loose association
- MENTIONS — document mentions entity (avoid if a more specific type applies)

You may create relationship types beyond these examples when the document context clearly supports a specific, meaningful connection. Keep types general and reusable — prefer "INCOME_FROM" over "RECEIVED_W2_WAGES_FROM".

=== ANTI-PATTERNS (avoid these) ===
- Do NOT create relationships between entities that merely appear in the same document without a stated connection
- Do NOT use overly specific types like "RECEIVED_QUARTERLY_TAX_ESTIMATE_FROM" — simplify to "PAID_TO" or "ESTIMATED_TAX_TO"
- Do NOT duplicate the same relationship with slight wording variations
- Prefer MENTIONS as a last resort only — if a more meaningful relationship exists, use it

Return a JSON object with:
{{{{
  "relationships": [
    {{{{
      "from_entity": "source entity name (must match entity list exactly)",
      "to_entity": "target entity name (must match entity list exactly)",
      "relationship_type": "RELATIONSHIP_TYPE",
      "confidence": 0.8,
      "description": "brief explanation of why this relationship exists"
    }}}}
  ]
}}}}

Document title: {title}

Entities from Pass 2:
{entities}

Document context:
{content}"""

# Pass 4 prompt: Verification/critique of extracted entities
VERIFICATION_PROMPT = """You are a quality reviewer for a knowledge graph entity extraction system. Review the following entity list extracted from a document and REMOVE any that are not real, specific, named entities.

VALID ENTITY TYPES: Person, Organization, Location, System, Product, Document, Event, Condition, FinancialItem, InsurancePolicy, Contract, DateEvent, Address

REMOVE entities that are:
1. Generic descriptions rather than named entities (e.g., "tax preparer", "the physician", "VR&E Officer")
2. Form field labels (e.g., "Date of Issue", "Certification Issue Date", "Reference Number")
3. Numbers, percentages, or dollar amounts masquerading as entities (e.g., "30 percent", "90% combined rating", "$72,545")
4. Vague references (e.g., "your area", "the facility", "disaster area")
5. Process descriptions or procedural terms (e.g., "Direct Review", "Evidence Submission")
6. Document section headers (e.g., "Section 3", "Part A")
7. Descriptive phrases that aren't proper nouns (e.g., "How VA Combines Percentages")
8. Duplicates or near-duplicates (keep the most complete version)
9. Raw dates that aren't named periods (e.g., "2025-01-15", "January 15, 2026")
10. Standalone zip codes, state abbreviations, or partial addresses

KEEP entities that are:
- Real named people (with actual proper names)
- Specific named organizations, companies, agencies
- Specific named places (cities, bases, buildings)
- Named medical conditions and diagnoses
- Named products/medications with specific names
- Specific named documents/forms (DD-214, SF-86, etc.)
- Named software systems/platforms
- Named events/operations
- Full street addresses (for Address type)
- Named insurance policies or coverage plans

ALSO CHECK entity types — if an entity is valid but assigned the WRONG type, correct it:
- "USAA" labeled as Person → should be Organization
- "Fort Bragg" labeled as Person → should be Location
- "PTSD" labeled as Event → should be Condition
- "Gabapentin 300mg" labeled as Person → should be Product

Document title: {title}

Entity list to review:
{entities}

Return a JSON object with ONLY the validated entities (remove all junk, correct wrong types):
{{{{
  "entities": [
    {{{{
      "name": "entity name",
      "type": "entity type",
      "confidence": 0.95,
      "description": "description"
    }}}}
  ]
}}}}"""


def _repair_json(raw_text: str) -> dict:
    """Attempt to parse and repair common JSON issues from LLM output.
    
    Returns a dict on success. Raises json.JSONDecodeError on complete failure.
    If LLM returns a JSON array, wraps it in {"items": [...]}.
    """
    if not raw_text or not raw_text.strip():
        return {}
    
    text = raw_text.strip()
    
    # 1. Try standard parse first
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            logger.debug("_repair_json: LLM returned JSON array, wrapping in dict")
            return {"items": parsed}
        return {}
    except json.JSONDecodeError:
        pass
    
    # 2. Strip markdown code fences (```json ... ``` or ``` ... ```)
    text = re.sub(r'^```(?:json)?\s*\n?', '', text, flags=re.MULTILINE)
    text = re.sub(r'\n?```\s*$', '', text, flags=re.MULTILINE)
    text = text.strip()
    
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {"items": parsed}
        return {}
    except json.JSONDecodeError:
        pass
    
    # 3. Fix trailing commas before } or ]
    text = re.sub(r',\s*([}\]])', r'\1', text)
    
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {"items": parsed}
        return {}
    except json.JSONDecodeError:
        pass
    
    # 4. Try to fix single quotes to double quotes (carefully)
    if '"' not in text and "'" in text:
        fixed = text.replace("'", '"')
        try:
            parsed = json.loads(fixed)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"items": parsed}
        except json.JSONDecodeError:
            pass
    
    # 5. Try extracting the first JSON object from the text
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        try:
            candidate = match.group(0)
            candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    
    # 6. Try extracting a JSON array if no object found
    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        try:
            candidate = match.group(0)
            candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
            parsed = json.loads(candidate)
            if isinstance(parsed, list):
                return {"items": parsed}
        except json.JSONDecodeError:
            pass
    
    # 7. Handle truncated JSON — try closing open braces/brackets
    # Count unmatched openers
    open_braces = text.count('{') - text.count('}')
    open_brackets = text.count('[') - text.count(']')
    if open_braces > 0 or open_brackets > 0:
        patched = text
        # Remove trailing comma if present
        patched = patched.rstrip().rstrip(',')
        # Close open structures
        patched += ']' * max(0, open_brackets) + '}' * max(0, open_braces)
        try:
            parsed = json.loads(patched)
            if isinstance(parsed, dict):
                logger.debug("_repair_json: repaired truncated JSON by closing braces")
                return parsed
            if isinstance(parsed, list):
                return {"items": parsed}
        except json.JSONDecodeError:
            pass
    
    # Give up - raise with context
    raise json.JSONDecodeError(f"Failed to repair JSON", raw_text[:200], 0)

async def _extract_json_with_retry(call_fn, operation: str, max_retries: int = 3) -> dict:
    """Call an LLM function expecting JSON, with type validation and retry.
    
    Handles: dict (pass through), list (wrap in {"items": [...]}),
    string (parse as JSON), None (retry), other types (retry).
    Returns {} on complete failure — never raises.
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            result = await retry_with_backoff(call_fn, operation=f"{operation}_attempt{attempt}")
            
            # Happy path: already a dict
            if isinstance(result, dict):
                return result
            
            # List response: wrap it (Gemini sometimes returns arrays for metadata)
            elif isinstance(result, list):
                logger.info(f"{operation}: LLM returned list (attempt {attempt+1}), wrapping in dict")
                return {"items": result}
            
            elif isinstance(result, str):
                # LLM returned a string — try to parse/repair it as JSON
                try:
                    parsed = _repair_json(result)
                    if isinstance(parsed, dict):
                        return parsed
                except (json.JSONDecodeError, TypeError):
                    pass
                logger.warning(f"{operation}: LLM returned unparseable string (attempt {attempt+1}/{max_retries}), retrying")
                last_error = ValueError(f"Expected dict, got string: {str(result)[:200]}")
                continue
            
            elif result is None:
                logger.warning(f"{operation}: LLM returned None (attempt {attempt+1}/{max_retries}), retrying")
                last_error = ValueError("LLM returned None")
                continue
            else:
                logger.warning(f"{operation}: LLM returned {type(result).__name__} (attempt {attempt+1}/{max_retries}), retrying")
                last_error = ValueError(f"Expected dict, got {type(result).__name__}")
                continue
                
        except json.JSONDecodeError as e:
            logger.warning(f"{operation}: JSON parse failed (attempt {attempt+1}/{max_retries}): {e}")
            last_error = e
            continue
        except Exception as e:
            logger.warning(f"{operation}: Unexpected error (attempt {attempt+1}/{max_retries}): {e}")
            last_error = e
            continue
    
    # All retries exhausted — return empty dict instead of crashing
    logger.error(f"{operation}: All {max_retries} attempts failed, using empty dict. Last error: {last_error}")
    return {}

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

        return await _extract_json_with_retry(_call, operation=f"pass1_metadata:{doc_type}")

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

        return await _extract_json_with_retry(_call, operation="pass2_entities")

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

        return await _extract_json_with_retry(_call, operation="pass3_relationships")

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
            verified = await _extract_json_with_retry(_call, operation="pass4_verification")
            # Sanity check: verification should not ADD entities, only remove them
            verified_names = {_coerce_text(e.get("name", "")).lower() for e in verified.get("entities", [])}
            original_names = {_coerce_text(e.get("name", "")).lower() for e in entity_list}
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
        # Safely convert metadata — guard against non-dict returns from LLM
        if isinstance(metadata, dict):
            result = dict(metadata)
        else:
            logger.warning(f"Metadata extraction returned {type(metadata).__name__}, using empty dict")
            result = {}
        
        # Convert entities to people/organizations format for backward compatibility
        people = []
        organizations = []
        all_entities = entities.get("entities", [])
        
        for entity in all_entities:
            entity_type = _coerce_text(entity.get("type", ""))
            name = _coerce_text(entity.get("name", ""))
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
            from_entity = _coerce_text(rel.get("from_entity", ""))
            to_entity = _coerce_text(rel.get("to_entity", ""))
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
                "relationship": _coerce_text(rel.get("relationship_type", "RELATED_TO")),
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
        entity_name = _coerce_text(entity_name)
        for entity in entities:
            if _coerce_text(entity.get("name", "")) == entity_name:
                return _coerce_text(entity.get("type", "Person")) or "Person"
        # Fallback heuristics
        if any(w in entity_name.lower() for w in ["inc", "llc", "corp", "dept", "department", "agency", "company", "bank", "university"]):
            return "Organization"
        return "Person"


extractor = EntityExtractor()
