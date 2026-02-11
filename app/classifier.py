import json
import logging

from openai import AsyncOpenAI

from app.config import settings
from app.retry import retry_with_backoff

logger = logging.getLogger(__name__)

VALID_TYPES = [
    "medical_lab",
    "financial_invoice",
    "legal_contract",
    "insurance",
    "property_home",
    "government_tax",
    "personal",
    "work",
    "military",
]

CLASSIFICATION_PROMPT = """You are a document classification system. Analyze the following document content and classify it into exactly ONE of these categories:

- medical_lab: Medical lab results, blood work, pathology reports, diagnostic tests
- financial_invoice: Invoices, bills, receipts, financial statements
- legal_contract: Contracts, agreements, legal documents, terms of service
- insurance: Insurance policies, claims, coverage documents
- property_home: Property deeds, home inspection reports, mortgage documents, real estate
- government_tax: Tax forms, tax returns, government filings, W-2s, 1099s
- personal: Personal correspondence, identification documents, personal records
- work: Employment documents, work correspondence, professional documents
- military: Military service records, orders, evaluations, DD-214s, deployment records, training records, military medical, AF forms, PCS orders

Respond with a JSON object containing:
- "doc_type": one of the category names listed above
- "confidence": a float between 0.0 and 1.0 indicating your confidence

Document title: {title}

Document content (first 3000 chars):
{content}
"""


class DocumentClassifier:
    def __init__(self):
        self.client = AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
        self.model = settings.gemini_model

    async def classify(self, title: str, content: str) -> dict:
        """Classify a document into one of the predefined types."""
        truncated = content[:3000]
        prompt = CLASSIFICATION_PROMPT.format(title=title, content=truncated)

        try:
            async def _call():
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                )
                return json.loads(response.choices[0].message.content)

            result = await retry_with_backoff(_call, operation="classify")
            doc_type = result.get("doc_type", "personal")
            if doc_type not in VALID_TYPES:
                logger.warning(f"Invalid doc_type '{doc_type}' returned, defaulting to 'personal'")
                doc_type = "personal"
            return {
                "doc_type": doc_type,
                "confidence": float(result.get("confidence", 0.5)),
            }
        except Exception as e:
            logger.error(f"Classification failed: {e}")
            return {"doc_type": "personal", "confidence": 0.0}


classifier = DocumentClassifier()
