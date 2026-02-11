import json
import logging

from openai import AsyncOpenAI

from app.config import settings
from app.retry import retry_with_backoff

logger = logging.getLogger(__name__)

EXTRACTION_PROMPTS = {
    "medical_lab": """Extract structured information from this medical lab document. Return a JSON object with:
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
      "flag": "H/L/normal or null",
      "confidence": 0.95
    }}
  ],
  "diagnoses": ["list of diagnoses if mentioned"],
  "ordering_physician": "physician name if mentioned",
  "confidence": 0.9,
  "implied_relationships": [
    {{
      "from_entity": "entity name",
      "from_type": "Person/Organization/Location/System/Product/Document/Event",
      "to_entity": "entity name",
      "to_type": "Person/Organization/Location/System/Product/Document/Event",
      "relationship": "PATIENT_OF/WORKS_AT/etc",
      "confidence": 0.8
    }}
  ]
}}

Also extract IMPLIED relationships — if a document is FROM an organization TO a person, that implies a relationship even if not explicitly stated. For example, a lab report from Quest Diagnostics for a patient implies a PATIENT_OF relationship.

Extract all information present. Use null for missing fields. Be thorough with test results.
Include a confidence score (0.0-1.0) for the overall extraction and for each test result.

Document title: {title}
Document content:
{content}""",
    "financial_invoice": """Extract structured information from this financial/invoice document. Return a JSON object with:
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
  "payment_status": "paid/unpaid/partial if mentioned",
  "confidence": 0.9,
  "implied_relationships": [
    {{
      "from_entity": "entity name",
      "from_type": "Person/Organization/Location/System/Product/Document/Event",
      "to_entity": "entity name",
      "to_type": "Person/Organization/Location/System/Product/Document/Event",
      "relationship": "CUSTOMER_OF/VENDOR_FOR/etc",
      "confidence": 0.8
    }}
  ]
}}

Also extract IMPLIED relationships — if an invoice is FROM a company TO a person, that implies a CUSTOMER_OF relationship.

Extract all information present. Use null for missing fields. Include a confidence score (0.0-1.0).

Document title: {title}
Document content:
{content}""",
    "legal_contract": """Extract structured information from this legal/contract document. Return a JSON object with:
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
  "renewal_info": "renewal terms if mentioned",
  "confidence": 0.9,
  "implied_relationships": [
    {{
      "from_entity": "entity name",
      "from_type": "Person/Organization/Location/System/Product/Document/Event",
      "to_entity": "entity name",
      "to_type": "Person/Organization/Location/System/Product/Document/Event",
      "relationship": "CONTRACTED_WITH/EMPLOYS/etc",
      "confidence": 0.8
    }}
  ]
}}

Also extract IMPLIED relationships between parties.

Extract all information present. Use null for missing fields. Include a confidence score (0.0-1.0).

Document title: {title}
Document content:
{content}""",
    "insurance": """Extract structured information from this insurance document. Return a JSON object with:
{{
  "provider": "insurance company name",
  "policy_number": "policy number",
  "policyholder": "policyholder name",
  "coverage_type": "type of coverage (health, auto, home, life, etc.)",
  "premium": "premium amount as number",
  "effective_date": "start date (YYYY-MM-DD if possible)",
  "expiration_date": "end date (YYYY-MM-DD if possible)",
  "covered_items": ["list of covered items or categories"],
  "confidence": 0.9,
  "implied_relationships": [
    {{
      "from_entity": "entity name",
      "from_type": "Person/Organization/Location/System/Product/Document/Event",
      "to_entity": "entity name",
      "to_type": "Person/Organization/Location/System/Product/Document/Event",
      "relationship": "INSURED_BY/POLICYHOLDER_OF/etc",
      "confidence": 0.8
    }}
  ]
}}

Also extract IMPLIED relationships — the policyholder has a relationship with the insurance provider.

Extract all information present. Use null for missing fields. Include a confidence score (0.0-1.0).

Document title: {title}
Document content:
{content}""",
    "government_tax": """Extract structured information from this tax/government document. Return a JSON object with:
{{
  "form_type": "form type (W-2, 1099, 1040, etc.)",
  "tax_year": "tax year",
  "filer_name": "name of the filer",
  "filing_status": "filing status if mentioned",
  "total_income": "total income as number",
  "tax_owed": "tax owed as number",
  "tax_paid": "tax paid as number",
  "preparer": "tax preparer name if mentioned",
  "confidence": 0.9,
  "implied_relationships": [
    {{
      "from_entity": "entity name",
      "from_type": "Person/Organization/Location/System/Product/Document/Event",
      "to_entity": "entity name",
      "to_type": "Person/Organization/Location/System/Product/Document/Event",
      "relationship": "EMPLOYED_BY/PREPARED_BY/etc",
      "confidence": 0.8
    }}
  ]
}}

Also extract IMPLIED relationships — e.g., a W-2 implies employment relationship.

Extract all information present. Use null for missing fields. Include a confidence score (0.0-1.0).

Document title: {title}
Document content:
{content}""",
    "property_home": """Extract structured information from this property/home document. Return a JSON object with:
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
  "description": "brief description of the document purpose",
  "confidence": 0.9,
  "implied_relationships": [
    {{
      "from_entity": "entity name",
      "from_type": "Person/Organization/Location/System/Product/Document/Event",
      "to_entity": "entity name",
      "to_type": "Person/Organization/Location/System/Product/Document/Event",
      "relationship": "OWNS/MORTGAGE_WITH/etc",
      "confidence": 0.8
    }}
  ]
}}

Also extract IMPLIED relationships between parties.

Extract all information present. Use null for missing fields. Include a confidence score (0.0-1.0).

Document title: {title}
Document content:
{content}""",
}

GENERIC_PROMPT = """Extract structured information from this document. Return a JSON object with:
{{
  "people": [
    {{
      "name": "person's full name",
      "role": "their role or relationship to the document",
      "confidence": 0.9
    }}
  ],
  "organizations": [
    {{
      "name": "organization name",
      "type": "type of organization",
      "confidence": 0.9
    }}
  ],
  "dates": [
    {{
      "date": "date value (YYYY-MM-DD if possible)",
      "description": "what this date represents"
    }}
  ],
  "key_facts": ["list of key facts or data points"],
  "summary": "brief summary of the document",
  "confidence": 0.9,
  "implied_relationships": [
    {{
      "from_entity": "entity name",
      "from_type": "Person/Organization/Location/System/Product/Document/Event",
      "to_entity": "entity name",
      "to_type": "Person/Organization/Location/System/Product/Document/Event",
      "relationship": "relationship type",
      "confidence": 0.8
    }}
  ]
}}

Also extract IMPLIED relationships — if the document is FROM an organization TO a person, or mentions entities in context that implies a relationship, include it.

Extract all information present. Use null for missing fields. Be thorough.
Include a confidence score (0.0-1.0) for each entity and the overall extraction.

Document title: {title}
Document content:
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
        """Extract structured entities and relationships from a document."""
        prompt_template = EXTRACTION_PROMPTS.get(doc_type, GENERIC_PROMPT)
        truncated = content[:8000]
        prompt = prompt_template.format(title=title, content=truncated)

        try:
            async def _call():
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                )
                return json.loads(response.choices[0].message.content)

            result = await retry_with_backoff(_call, operation=f"extract:{doc_type}")
            return result
        except Exception as e:
            logger.error(f"Extraction failed for doc_type={doc_type}: {e}")
            # Try fallback extraction
            return await self._fallback_extract(title, content)

    async def _fallback_extract(self, title: str, content: str) -> dict:
        """Simpler fallback extraction for docs that fail primary extraction."""
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
