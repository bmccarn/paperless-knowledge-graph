import json
import logging

from google import genai

from app.config import settings

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
  "payment_status": "paid/unpaid/partial if mentioned"
}}

Extract all information present. Use null for missing fields.

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
  "renewal_info": "renewal terms if mentioned"
}}

Extract all information present. Use null for missing fields.

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
  "covered_items": ["list of covered items or categories"]
}}

Extract all information present. Use null for missing fields.

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
  "preparer": "tax preparer name if mentioned"
}}

Extract all information present. Use null for missing fields.

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
  "description": "brief description of the document purpose"
}}

Extract all information present. Use null for missing fields.

Document title: {title}
Document content:
{content}""",
}

GENERIC_PROMPT = """Extract structured information from this document. Return a JSON object with:
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


class EntityExtractor:
    def __init__(self):
        self.client = genai.Client(api_key=settings.gemini_api_key)
        self.model = settings.gemini_model

    async def extract(self, title: str, content: str, doc_type: str) -> dict:
        """Extract structured entities and relationships from a document."""
        prompt_template = EXTRACTION_PROMPTS.get(doc_type, GENERIC_PROMPT)
        # Limit content to avoid token limits
        truncated = content[:8000]
        prompt = prompt_template.format(title=title, content=truncated)

        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config={"response_mime_type": "application/json"},
            )
            result = json.loads(response.text)
            return result
        except Exception as e:
            logger.error(f"Extraction failed for doc_type={doc_type}: {e}")
            return {}


extractor = EntityExtractor()
