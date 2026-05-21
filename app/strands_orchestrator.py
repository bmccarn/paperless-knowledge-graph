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
- For Strict mode, include original-source, contradiction/supersession, and exact-value subqueries.
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
- Prefer exact evidence IDs/excerpts from the source context when possible.
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

    async def extract_claim_ledger(
        self,
        question: str,
        answer: str,
        evidence_context: str,
        verification: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.enabled:
            return None

        prompt = f"""Create an atomic claim ledger for this answer.

Question: {question}

Answer:
{answer[:9000]}

Evidence context:
{evidence_context[:18000]}

Verifier notes:
{json.dumps(verification or {}, default=str)[:3000]}

Return only JSON:
{{
  "claims": [
    {{
      "claim": "single factual claim",
      "support_status": "supported|partial|unsupported|conflicting|unknown",
      "document_id": 123,
      "source_title": "source document title",
      "evidence_id": "evidence id from context",
      "evidence_excerpt": "short exact supporting excerpt",
      "date": "date/effective period if relevant",
      "source_quality": "original|direct|summary|weak|unknown",
      "notes": "short note"
    }}
  ]
}}

Rules:
- Split compound sentences into separate factual claims.
- Claims with numbers, dates, names, statuses, balances, coverage, diagnoses, lab values, or legal/tax facts must have source support.
- If the evidence pack contains support, cite the evidence ID and document.
- If support is missing or only inferred, mark partial/unsupported instead of guessing.
- Maximum 40 claims.
"""
        return await self._json_agent(
            name="claim_ledger",
            system_prompt=(
                "You build claim ledgers for evidence-grounded answers. "
                "You classify each factual claim by source support and never invent citations."
            ),
            prompt=prompt,
            max_tokens=5200,
        )

    async def repair_answer(
        self,
        question: str,
        answer: str,
        evidence_context: str,
        verification: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not self.enabled:
            return None

        prompt = f"""Repair this answer so it is source-faithful.

Question: {question}

Original answer:
{answer[:9000]}

Evidence context:
{evidence_context[:18000]}

Verifier findings:
{json.dumps(verification, default=str)[:5000]}

Return only JSON:
{{
  "answer": "repaired answer",
  "changed": true,
  "notes": ["what changed"]
}}

Rules:
- Preserve supported details that answer the user's question.
- Remove unsupported precise values if no support exists in evidence.
- If a useful claim is only partially supported, qualify it explicitly.
- Add a short "Evidence limits" note only when missing evidence materially limits the direct answer. Omit it when the answer already provides the requested facts with support.
- Do not add new facts unless they are directly supported by the evidence context and relevant to the question.
- Remove source/admin details, account/client identifiers, logistics, or adjacent facts when they are merely evidence context and not part of the answer requested.
- Do not mention missing source/admin details, ordering logistics, account/client identifiers, or provider metadata unless the user asked for those details.
- Do not assert that a newer document or record set lacks a value unless the evidence explicitly proves absence. If latest/current status is not fully provable, phrase it as the newest source-backed value found in the retrieved evidence.
"""
        return await self._json_agent(
            name="answer_editor",
            system_prompt=(
                "You are a source-faithful answer editor. You remove or qualify unsupported claims "
                "without making the answer vague."
            ),
            prompt=prompt,
            max_tokens=6200,
        )

    async def review_entity_candidate(self, candidate: dict[str, Any], deterministic: dict[str, Any]) -> dict[str, Any] | None:
        if not self.enabled:
            return None

        prompt = f"""Review whether two knowledge-graph entities should be merged.

Candidate:
{json.dumps(candidate, default=str)[:7000]}

Deterministic signals:
{json.dumps(deterministic, default=str)[:3000]}

Return only JSON:
{{
  "recommendation": "merge|split|review",
  "confidence": 0.0,
  "risk": "low|medium|high",
  "reasons": ["short reason"],
  "required_human_check": true
}}

Rules:
- Prefer review/split for people, medical entities, addresses, legal parties, and ambiguous organizations.
- Recommend merge only when names/identifiers/source context clearly indicate the same real-world entity.
- Similar-looking names alone are not enough for high-risk entity types.
- Never recommend destructive merge when entity types differ.
"""
        return await self._json_agent(
            name="entity_steward",
            system_prompt=(
                "You are a conservative knowledge-graph entity steward. "
                "Bad merges corrupt the graph, so you prefer review unless evidence is clear."
            ),
            prompt=prompt,
            max_tokens=1800,
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
