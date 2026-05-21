"""Strands-backed agent helpers for query planning and verification.

The query engine owns retrieval, ranking, and source handling. Strands is used
only for bounded agent decisions where the model can improve quality: planning,
timeline extraction, and evidence verification.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)

try:
    from strands import Agent
    from strands.models.litellm import LiteLLMModel

    STRANDS_AVAILABLE = True
except Exception as exc:  # pragma: no cover - depends on optional runtime package
    Agent = None
    LiteLLMModel = None
    STRANDS_AVAILABLE = False
    _STRANDS_IMPORT_ERROR = str(exc)
else:
    _STRANDS_IMPORT_ERROR = ""


class StrandsQueryOrchestrator:
    """Small, stateless wrapper around Strands Agents."""

    def __init__(self):
        self.enabled = bool(settings.strands_enabled and STRANDS_AVAILABLE)

    @property
    def status(self) -> dict[str, Any]:
        if self.enabled:
            return {"status": "healthy", "enabled": True, "provider": "strands", "model": settings.strands_model or settings.gemini_model}
        return {
            "status": "degraded" if settings.strands_enabled else "disabled",
            "enabled": False,
            "provider": "fallback",
            "reason": "disabled" if not settings.strands_enabled else _STRANDS_IMPORT_ERROR or "unavailable",
        }

    async def plan_query(self, question: str, mode: str, conversation_context: str = "") -> dict[str, Any] | None:
        if not self.enabled:
            return None

        prompt = f"""Return only strict JSON for a personal-document query plan.

Question: {question}
Mode: {mode}
Conversation context:
{conversation_context or "(none)"}

JSON schema:
{{
  "intent": "lookup|current_state|timeline|compare|broad_inventory",
  "domain": "insurance|tax|mortgage|medical|vehicle|legal|financial|military|property|mixed|general",
  "requires_current": true,
  "needs_timeline": false,
  "must_answer_current_vs_historical": true,
  "required_doc_types": ["policy", "statement"],
  "subqueries": [
    {{"role": "primary", "query": "..."}},
    {{"role": "current_state", "query": "..."}}
  ],
  "reasoning": "short explanation"
}}

Rules:
- For Quick mode, keep subqueries minimal.
- For Deep mode, create 3-5 targeted retrieval subqueries.
- For Timeline mode, include effective date, expiration date, statement period, revision, and chronological subqueries.
- Prefer current/latest checks for insurance, tax, mortgage, legal, financial, medical, vehicle, and VA/military questions.
"""
        return await self._json_agent(
            name="query_planner",
            system_prompt=(
                "You are a cautious retrieval planner for a private document knowledge graph. "
                "You do not answer the user. You produce compact JSON plans only."
            ),
            prompt=prompt,
            max_tokens=1600,
        )

    async def extract_timeline(self, question: str, context: str) -> list[dict[str, Any]]:
        if not self.enabled:
            return []

        prompt = f"""Extract a deterministic timeline from the retrieved document context.

Question: {question}

Context:
{context[:18000]}

Return only JSON:
{{
  "events": [
    {{
      "date": "YYYY-MM-DD or exact text if not normalized",
      "title": "short event title",
      "summary": "what changed or happened",
      "document_id": 123,
      "source_title": "document title",
      "status": "current|expired|superseded|historical|unknown"
    }}
  ]
}}

Rules:
- Use document dates, effective dates, expiration dates, statement periods, and revision dates.
- Keep events tied to a source document.
- Do not invent dates.
"""
        result = await self._json_agent(
            name="timeline_analyst",
            system_prompt=(
                "You extract sourced chronological events from document context. "
                "You never infer dates not present in the context."
            ),
            prompt=prompt,
            max_tokens=2200,
        )
        events = result.get("events", []) if isinstance(result, dict) else []
        return [event for event in events if isinstance(event, dict)]

    async def verify_answer(
        self,
        question: str,
        answer: str,
        sources: list[dict[str, Any]],
        source_context: str,
        plan: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not self.enabled:
            return None

        source_list = [
            {
                "document_id": s.get("document_id"),
                "title": s.get("title"),
                "date": s.get("date"),
                "doc_type": s.get("doc_type"),
                "excerpt": s.get("excerpt"),
            }
            for s in sources[:12]
        ]
        prompt = f"""Verify this answer against the provided source context.

Question: {question}
Plan: {json.dumps(plan, default=str)[:3000]}

Answer:
{answer[:6000]}

Sources:
{json.dumps(source_list, default=str)[:6000]}

Source context:
{source_context[:8000]}

Return only JSON:
{{
  "status": "verified|needs_review",
  "supported_claims": ["claim -> source title"],
  "unsupported_claims": ["claim"],
  "stale_or_conflicting_claims": ["claim"],
  "missing_evidence": ["needed evidence"],
  "notes": ["short note"],
  "confidence_adjustment": -0.1
}}

Rules:
- Flag any claim that is not clearly supported by the sources.
- For current-state questions, flag old/expired/superseded sources used as current.
- Do not judge writing style; only evidence support.
- Be compact: maximum 8 supported claims, 8 unsupported claims, 8 stale/conflicting claims, 5 missing evidence items, and 3 notes.
- Use short phrases, not paragraphs.
"""
        return await self._json_agent(
            name="evidence_verifier",
            system_prompt=(
                "You are an evidence verifier. You compare answer claims to source excerpts "
                "and return compact JSON. You do not rewrite the answer."
            ),
            prompt=prompt,
            max_tokens=3600,
        )

    async def _json_agent(self, name: str, system_prompt: str, prompt: str, max_tokens: int) -> dict[str, Any]:
        try:
            agent = Agent(
                name=name,
                model=self._model(max_tokens=max_tokens),
                system_prompt=system_prompt,
                callback_handler=None,
            )
            result = await agent.invoke_async(prompt)
            return _extract_json(str(result))
        except Exception as exc:
            logger.warning("Strands %s failed: %s", name, exc)
            return {}

    def _model(self, max_tokens: int):
        model_id = settings.strands_model or settings.gemini_model
        return LiteLLMModel(
            client_args={
                "api_key": settings.litellm_api_key or "unused",
                "api_base": settings.litellm_url,
                "use_litellm_proxy": True,
            },
            model_id=model_id,
            params={
                "max_tokens": max_tokens,
                "temperature": settings.strands_temperature,
            },
        )


def _extract_json(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    try:
        return json.loads(text)
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            return {}
    return {}


strands_orchestrator = StrandsQueryOrchestrator()
