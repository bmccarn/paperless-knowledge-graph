import json
import logging
import hashlib
import re
import asyncio
from datetime import datetime, timezone
from collections import Counter, defaultdict
from typing import Any

from openai import AsyncOpenAI, RateLimitError

from app.config import settings

def _owner_name():
    return settings.owner_name or "the document owner"

def _owner_context():
    return settings.owner_context or ""
from app.retry import retry_with_backoff
from app.embeddings import chunk_text, embeddings_store
from app.paperless import paperless_client
from app.graph import graph_store
from app.cache import query_cache, vector_cache, graph_cache, normalize_query_key
from app.query_quality import (
    compute_evidence_grade,
    current_state_summary,
    heuristic_plan,
    merge_agent_plan,
    normalize_mode,
    retrieval_queries,
    sort_timeline_events,
    timeline_fallback_events,
    trace_step,
)
from app.evidence import (
    answer_needs_repair,
    build_evidence_pack,
    claim_ledger_from_verification,
    extract_date_signals,
    exact_term_matches as evidence_exact_term_matches,
    exact_term_hits as evidence_exact_term_hits,
    format_evidence_pack_for_llm,
    infer_source_quality,
    is_high_stakes_query,
    normalize_claim_ledger,
    query_terms as evidence_query_terms,
    repair_queries_from_verification,
    structured_fact_count,
)
from app.strands_orchestrator import strands_orchestrator

logger = logging.getLogger(__name__)
QUERY_CACHE_VERSION = "evidence-v15"


class QueryEngine:
    def __init__(self):
        self.client = AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
        self.model = settings.gemini_model
        self._model_override = None

    def _active_model(self, model_override=None):
        return model_override or self._model_override or self.model

    async def _llm_generate(self, prompt: str) -> str:
        try:
            response = await self.client.chat.completions.create(
                model=self._active_model(),
                messages=[{"role": "user", "content": prompt}],
            )
            return response.choices[0].message.content
        except (RateLimitError, Exception) as e:
            if isinstance(e, RateLimitError) or "429" in str(e) or "rate" in str(e).lower():
                logger.warning(f"Rate limited on {self._active_model()}, falling back to {settings.fallback_model}")
                response = await self.client.chat.completions.create(
                    model=settings.fallback_model,
                    messages=[{"role": "user", "content": prompt}],
                )
                return response.choices[0].message.content
            raise

    async def _llm_json(self, prompt: str) -> any:
        """LLM call expecting JSON response. Retries with fallback on parse errors or rate limits."""
        models_to_try = [self._active_model(), settings.fallback_model]
        last_error = None
        for model in models_to_try:
            try:
                response = await self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                )
                content = response.choices[0].message.content
                if not content or not content.strip():
                    logger.warning(f"Empty response from {model} for JSON call, trying next model")
                    continue
                return json.loads(content)
            except json.JSONDecodeError as e:
                logger.warning(f"JSON parse error from {model}: {e}, trying next model")
                last_error = e
                continue
            except (RateLimitError, Exception) as e:
                if isinstance(e, RateLimitError) or "429" in str(e) or "rate" in str(e).lower():
                    logger.warning(f"Rate limited on {model}, trying next model")
                    last_error = e
                    continue
                raise
        raise last_error or ValueError("All models failed for JSON call")

    async def _llm_generate_stream(self, prompt: str):
        """Yield answer chunks via streaming, with fallback on rate limit."""
        try:
            stream = await self.client.chat.completions.create(
                model=self._active_model(),
                messages=[{"role": "user", "content": prompt}],
                stream=True,
            )
            async for chunk in stream:
                if chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content
        except (RateLimitError, Exception) as e:
            if isinstance(e, RateLimitError) or "429" in str(e) or "rate" in str(e).lower():
                logger.warning(f"Rate limited on {self._active_model()}, falling back to {settings.fallback_model}")
                stream = await self.client.chat.completions.create(
                    model=settings.fallback_model,
                    messages=[{"role": "user", "content": prompt}],
                    stream=True,
                )
                async for chunk in stream:
                    if chunk.choices[0].delta.content:
                        yield chunk.choices[0].delta.content
            else:
                raise

    # ── Orchestration ─────────────────────────────────────────────────

    def _conversation_context(self, conversation_history: list = None) -> str:
        if not conversation_history:
            return ""
        lines = []
        for msg in conversation_history[-6:]:
            role = "User" if msg.get("role") == "user" else "Assistant"
            lines.append(f"{role}: {msg.get('content', '')[:600]}")
        return "\n".join(lines)

    async def _build_query_plan(self, question: str, mode: str, conversation_history: list = None) -> tuple[dict, list[dict]]:
        mode = normalize_mode(mode)
        trace = [trace_step("mode", "ok", f"{mode} query strategy selected", {"mode": mode})]

        if mode == "quick":
            plan = heuristic_plan(question, mode)
            trace.append(trace_step(
                "planner",
                "ok",
                "Quick mode uses deterministic single-pass planning",
                {"planner": plan.get("planner"), "intent": plan.get("intent"), "domain": plan.get("domain")},
            ))
            return plan, trace

        agent_plan = await strands_orchestrator.plan_query(
            question,
            mode,
            conversation_context=self._conversation_context(conversation_history),
        )
        plan = merge_agent_plan(question, mode, agent_plan)
        trace.append(trace_step(
            "planner",
            "ok" if plan.get("planner") == "strands" else "fallback",
            "Strands planner produced a retrieval plan" if plan.get("planner") == "strands" else "Using heuristic retrieval plan",
            {
                "strands": strands_orchestrator.status,
                "intent": plan.get("intent"),
                "domain": plan.get("domain"),
                "requires_current": plan.get("requires_current"),
                "subquery_count": len(plan.get("subqueries", [])),
            },
        ))
        return plan, trace

    async def _execute_retrieval_plan(self, question: str, plan: dict, mode: str) -> tuple[dict, list[str], bool, list[dict]]:
        mode = normalize_mode(mode)
        trace = []
        max_queries = 1 if mode == "quick" else 8 if mode == "strict" else 6
        queries = retrieval_queries(plan, max_queries=max_queries)
        if mode != "quick" and self._requires_latest_check(question) and not any(q.get("role") == "current_state" for q in queries):
            queries.append({"role": "current_state", "query": self._latest_check_query(question)})
        latest_check_used = any(q.get("role") == "current_state" for q in queries)
        is_broad = bool(plan.get("broad_query")) and mode != "quick"

        async def _retrieve_one(item: dict) -> tuple[dict, dict]:
            if item.get("role") in {"primary", "current_state"}:
                ctx = await self._retrieve(item["query"])
            else:
                ctx = await self._retrieve_light(item["query"])
            return item, ctx

        if mode == "quick":
            item = queries[0] if queries else {"role": "primary", "query": question}
            all_context = await self._retrieve(item["query"])
            trace.append(trace_step("retrieval", "ok", "Single-pass hybrid retrieval completed", {"queries": [item]}))
            return all_context, [], latest_check_used, trace

        broad_decompose_task = asyncio.create_task(self._decompose_query(question)) if is_broad else None
        graph_docs_task = asyncio.create_task(self._retrieve_graph_documents(question)) if is_broad else None

        retrieved = await asyncio.gather(*[_retrieve_one(item) for item in queries])
        all_context: dict | None = None
        for item, ctx in retrieved:
            all_context = ctx if all_context is None else self._merge_context(all_context, ctx)
            trace.append(trace_step(
                "retrieval",
                "ok",
                f"{item.get('role', 'planned')} retrieval completed",
                {
                    "query": item.get("query"),
                    "vector_results": len(ctx.get("vector_results", [])),
                    "keyword_results": len(ctx.get("keyword_results", [])),
                    "graph_nodes": len(ctx.get("graph_nodes", [])),
                },
            ))

        all_context = all_context or {
            "vector_results": [], "keyword_results": [], "entity_results": [],
            "entity_kw_results": [], "graph_nodes": [], "subgraph": {}, "entity_names": [],
        }

        broad_queries = []
        if broad_decompose_task:
            broad_queries = await broad_decompose_task
            if broad_queries:
                logger.info("Broad query: %s sub-queries: %s", len(broad_queries), broad_queries)
                sub_results = await asyncio.gather(
                    *[self._retrieve_light(sq) for sq in broad_queries],
                    return_exceptions=True,
                )
                merged = 0
                for result in sub_results:
                    if isinstance(result, dict):
                        all_context = self._merge_context(all_context, result)
                        merged += 1
                trace.append(trace_step(
                    "broad_decomposition",
                    "ok",
                    f"Ran {merged} decomposed coverage searches",
                    {"queries": broad_queries},
                ))

        if graph_docs_task:
            graph_doc_context = await graph_docs_task
            if graph_doc_context:
                all_context = self._merge_context(all_context, graph_doc_context)
                trace.append(trace_step(
                    "graph_document_coverage",
                    "ok",
                    "Added graph-linked documents for broad coverage",
                    {"vector_results": len(graph_doc_context.get("vector_results", []))},
                ))

        used_queries = [q["query"] for q in queries[1:]] + broad_queries
        return all_context, used_queries, latest_check_used, trace

    async def _gap_review(
        self,
        question: str,
        context: dict,
        conversation_history: list,
        mode: str,
        broad: bool = False,
    ) -> tuple[dict, dict, list[str], list[dict]]:
        trace = []
        if normalize_mode(mode) == "quick":
            entities = context.get("entity_names", [])
            return {"draft_answer": "", "confidence": 0.55, "entities_found": entities, "follow_up_suggestions": []}, context, [], trace

        first_pass = await self._synthesize_with_gaps(question, context, conversation_history)
        follow_ups_used = []
        follow_up_queries = first_pass.get("follow_up_queries", [])[:5 if broad else 3]
        if broad:
            q_lower = question.lower()
            injected = []
            if any(s in q_lower for s in ["mortgage", "payment", "bill", "obligation", "financial"]):
                injected.append("PHH Mortgage current monthly payment amount payment change notice 2025 2026")
            if injected:
                follow_up_queries = injected + [q for q in follow_up_queries if q not in injected]
                follow_up_queries = follow_up_queries[:7]
            if follow_up_queries:
                follow_results = await asyncio.gather(
                    *[self._retrieve_light(follow_up) for follow_up in follow_up_queries],
                    return_exceptions=True,
                )
                for follow_up, extra_context in zip(follow_up_queries, follow_results):
                    if isinstance(extra_context, dict):
                        context = self._merge_context(context, extra_context)
                        follow_ups_used.append(follow_up)
                trace.append(trace_step(
                    "gap_review",
                    "ok",
                    f"Ran {len(follow_ups_used)} broad follow-up searches",
                    {"queries": follow_ups_used},
                ))
        else:
            for follow_up in follow_up_queries:
                extra_context = await self._retrieve(follow_up)
                context = self._merge_context(context, extra_context)
                follow_ups_used.append(follow_up)
                trace.append(trace_step(
                    "gap_review",
                    "ok",
                    "Follow-up retrieval filled an evidence gap",
                    {"query": follow_up, "vector_results": len(extra_context.get("vector_results", []))},
                ))
        if not follow_ups_used:
            trace.append(trace_step(
                "gap_review",
                "ok" if first_pass else "fallback",
                "No additional gap follow-up retrieval was needed",
            ))
        return first_pass, context, follow_ups_used, trace

    async def _expand_planned_graph(self, context: dict, entities_found: list) -> tuple[dict, list[dict]]:
        trace = []
        entity_candidates = []
        for entity in entities_found or []:
            if isinstance(entity, dict):
                name = entity.get("name") or entity.get("label")
            else:
                name = entity
            if name:
                entity_candidates.append(str(name))
        entity_candidates.extend(str(e) for e in context.get("entity_names", []) if e)
        entity_candidates = list(dict.fromkeys(entity_candidates))[:12]

        if entity_candidates:
            graph_context = await self._expand_graph(entity_candidates)
            context = self._merge_context(context, graph_context)
            trace.append(trace_step(
                "graph_expansion",
                "ok",
                f"Expanded {len(entity_candidates)} planned entities through Neo4j",
                {"entities": entity_candidates[:8], "graph_nodes": len(graph_context.get("graph_nodes", []))},
            ))
        else:
            trace.append(trace_step("graph_expansion", "skipped", "No entity candidates found for graph expansion"))
        return context, trace

    async def _extract_timeline_events(
        self,
        question: str,
        context: dict,
        sources: list[dict],
        mode: str,
        broad: bool = False,
    ) -> tuple[list[dict], list[dict]]:
        if normalize_mode(mode) != "timeline":
            return [], []
        doc_context = self._format_doc_context(context, question=question, broad=broad)
        graph_text = self._format_graph_context(context)
        events = await strands_orchestrator.extract_timeline(question, f"{doc_context}\n{graph_text}")
        if events:
            events = sort_timeline_events(events)
            return events[:30], [trace_step("timeline", "ok", f"Extracted and sorted {len(events)} timeline events with Strands")]
        fallback = timeline_fallback_events(sources)
        return fallback, [trace_step("timeline", "fallback", f"Used source-date fallback for {len(fallback)} timeline events")]

    async def _verify_repair_and_grade(
        self,
        question: str,
        answer: str,
        context: dict,
        sources: list[dict],
        plan: dict,
        mode: str,
        broad: bool = False,
        progress_callback: Any | None = None,
    ) -> tuple[str, dict, dict, list[dict], dict, dict, list[dict], dict]:
        verification = None
        trace = []
        evidence_pack = await self._build_evidence_pack(question, context, sources, plan, mode, broad=broad)

        async def emit(stage: str, **payload: Any) -> None:
            if not progress_callback:
                return
            payload.setdefault("stage", stage)
            payload.setdefault("answer", answer)
            payload.setdefault("verification", verification or {})
            payload.setdefault("trace", list(trace))
            payload.setdefault("evidence_pack", evidence_pack)
            payload.setdefault("sources", sources)
            payload.setdefault("context", context)
            await progress_callback(payload)

        preliminary_evidence = compute_evidence_grade(
            question,
            plan,
            sources,
            context,
            {"status": "checking", "unsupported_claims": [], "stale_or_conflicting_claims": []},
            evidence_pack=evidence_pack,
            claim_ledger={},
        )
        await emit("evidence_pack", evidence=preliminary_evidence)

        if normalize_mode(mode) != "quick":
            evidence_context = format_evidence_pack_for_llm(evidence_pack)
            verification = await strands_orchestrator.verify_answer(
                question,
                answer,
                sources,
                evidence_context,
                plan,
            )
            if verification:
                unsupported = len(verification.get("unsupported_claims") or [])
                stale = len(verification.get("stale_or_conflicting_claims") or [])
                status = verification.get("status") or ("needs_review" if unsupported or stale else "verified")
                trace.append(trace_step(
                    "verifier",
                    "ok" if status == "verified" else "needs_review",
                    f"Verifier status: {status}",
                    {"unsupported_claims": unsupported, "stale_or_conflicting_claims": stale},
                ))
                interim_evidence = compute_evidence_grade(
                    question,
                    plan,
                    sources,
                    context,
                    verification,
                    evidence_pack=evidence_pack,
                    claim_ledger={},
                )
                await emit("verifier", evidence=interim_evidence)
                repair_queries = repair_queries_from_verification(question, verification, limit=5)
                if repair_queries and (unsupported or verification.get("missing_evidence")):
                    repaired_context = context
                    used = []
                    for repair_query in repair_queries:
                        extra_context = await self._retrieve(repair_query)
                        repaired_context = self._merge_context(repaired_context, extra_context)
                        used.append(repair_query)
                    context = repaired_context
                    sources = self._build_sources(context, question=question)
                    evidence_pack = await self._build_evidence_pack(question, context, sources, plan, mode, broad=broad)
                    evidence_context = format_evidence_pack_for_llm(evidence_pack)
                    verification = await strands_orchestrator.verify_answer(question, answer, sources, evidence_context, plan)
                    unsupported = len(verification.get("unsupported_claims") or [])
                    stale = len(verification.get("stale_or_conflicting_claims") or [])
                    trace.append(trace_step(
                        "verification_retrieval_repair",
                        "ok" if not unsupported and not stale else "needs_review",
                        f"Ran {len(used)} focused follow-up searches for verifier gaps",
                        {"queries": used, "unsupported_claims": unsupported, "stale_or_conflicting_claims": stale},
                    ))
                    interim_evidence = compute_evidence_grade(
                        question,
                        plan,
                        sources,
                        context,
                        verification,
                        evidence_pack=evidence_pack,
                        claim_ledger={},
                    )
                    await emit("verification_retrieval_repair", evidence=interim_evidence)
                if answer_needs_repair(verification):
                    repaired = await strands_orchestrator.repair_answer(question, answer, evidence_context, verification)
                    repaired_answer = (repaired or {}).get("answer") if isinstance(repaired, dict) else None
                    if repaired_answer and repaired_answer.strip() and repaired_answer.strip() != answer.strip():
                        answer = repaired_answer.strip()
                        trace.append(trace_step(
                            "answer_editor",
                            "ok",
                            "Rewrote answer to remove or qualify unsupported claims",
                            {"notes": (repaired or {}).get("notes", [])[:4]},
                        ))
                        verification = await strands_orchestrator.verify_answer(question, answer, sources, evidence_context, plan)
                        interim_evidence = compute_evidence_grade(
                            question,
                            plan,
                            sources,
                            context,
                            verification,
                            evidence_pack=evidence_pack,
                            claim_ledger={},
                        )
                        await emit("answer_editor", answer=answer, evidence=interim_evidence)
                    else:
                        trace.append(trace_step("answer_editor", "skipped", "No answer repair returned"))
                        answer = self._append_evidence_limits(answer, verification)
                        interim_evidence = compute_evidence_grade(
                            question,
                            plan,
                            sources,
                            context,
                            verification,
                            evidence_pack=evidence_pack,
                            claim_ledger={},
                        )
                        await emit("answer_editor", answer=answer, evidence=interim_evidence)
            else:
                verification = {
                    "status": "not_run",
                    "supported_claims": [],
                    "unsupported_claims": [],
                    "stale_or_conflicting_claims": [],
                    "missing_evidence": [],
                    "notes": ["Verifier unavailable; answer is retrieval-backed but not claim-audited."],
                    "confidence_adjustment": -0.12,
                }
                trace.append(trace_step("verifier", "fallback", "Verifier unavailable; answer is retrieval-backed but not claim-audited"))
                await emit("verifier", evidence=preliminary_evidence)

        claim_ledger_raw = None
        if normalize_mode(mode) != "quick":
            claim_ledger_raw = await strands_orchestrator.extract_claim_ledger(
                question,
                answer,
                format_evidence_pack_for_llm(evidence_pack),
                verification,
            )
        claim_ledger = normalize_claim_ledger(claim_ledger_raw)
        if not claim_ledger.get("claims") and verification:
            claim_ledger = claim_ledger_from_verification(verification)
        if claim_ledger.get("claims"):
            trace.append(trace_step(
                "claim_ledger",
                "ok",
                f"Audited {len(claim_ledger.get('claims', []))} answer claim(s)",
                claim_ledger.get("summary"),
            ))
        else:
            trace.append(trace_step("claim_ledger", "fallback", "Claim ledger unavailable or empty"))

        evidence = compute_evidence_grade(
            question,
            plan,
            sources,
            context,
            verification,
            evidence_pack=evidence_pack,
            claim_ledger=claim_ledger,
        )
        trace.append(trace_step(
            "evidence_grade",
            "ok",
            f"{evidence['level']} trust score ({evidence['score']:.2f})",
            {"reasons": evidence.get("reasons", []), "penalties": evidence.get("penalties", [])},
        ))
        await emit("evidence_grade", evidence=evidence, claim_ledger=claim_ledger)
        return answer, verification or {}, evidence, trace, claim_ledger, evidence_pack, sources, context

    def _append_evidence_limits(self, answer: str, verification: dict[str, Any]) -> str:
        unsupported = verification.get("unsupported_claims") or []
        stale = verification.get("stale_or_conflicting_claims") or []
        missing = verification.get("missing_evidence") or []
        if not unsupported and not stale and not missing:
            return answer
        lines = ["\n\nEvidence limits:"]
        if unsupported:
            lines.append("The verifier could not confirm these claim groups from the retrieved evidence:")
            lines.extend(f"- {claim}" for claim in unsupported[:6])
        if stale:
            lines.append("The verifier found possible stale/conflicting claim groups:")
            lines.extend(f"- {claim}" for claim in stale[:4])
        if missing:
            lines.append("Missing evidence to fully certify the answer:")
            lines.extend(f"- {gap}" for gap in missing[:4])
        return answer.rstrip() + "\n".join(lines)

    def _blend_confidence(self, llm_confidence: float, evidence: dict, verification: dict) -> float:
        evidence_score = float(evidence.get("score", 0.5))
        adjustment = 0.0
        try:
            adjustment = float(verification.get("confidence_adjustment") or 0)
        except Exception:
            adjustment = 0.0
        blended = (0.35 * float(llm_confidence or 0.5)) + (0.65 * evidence_score) + adjustment
        status = verification.get("status") if isinstance(verification, dict) else None
        if status == "checking":
            blended = min(blended, 0.69)
        elif status == "not_run":
            blended = min(blended, 0.74)
        return round(max(0.0, min(1.0, blended)), 3)

    # ── Main query (non-streaming, backward compat) ─────────────────

    # ── Broad query detection & decomposition ─────────────────────

    def _is_broad_query(self, question: str) -> bool:
        """Detect if a query needs decomposition for wider retrieval."""
        broad_signals = [
            "all ", "every ", "complete list", "comprehensive", "everything",
            "all the", "all my", "all of my", "each ", "full list",
            "recurring", "obligations", "summary of", "overview of",
            "what do i owe", "what do i pay", "what are my", "how much do i",
            "compare", "comparison", "breakdown of", "total ",
        ]
        q_lower = question.lower()
        matches = sum(1 for s in broad_signals if s in q_lower)
        multi_category = any(w in q_lower for w in [" and ", " including ", ", ", " or "])
        return matches >= 1 and (multi_category or matches >= 2)

    async def _decompose_query(self, question: str) -> list[str]:
        """Split a broad query into focused sub-queries via LLM."""
        try:
            prompt = f"""Break this broad document search question into 5-8 FOCUSED sub-queries.
Each should target a specific document type/category to maximize retrieval across a personal document archive.
Make each query specific and search-friendly.

Question: {question}

Return JSON: {{"sub_queries": ["focused query 1", "focused query 2", ...]}}"""
            result = await self._llm_json(prompt)
            queries = result.get("sub_queries", [])
            if isinstance(queries, list) and len(queries) >= 3:
                return queries[:8]
        except Exception as e:
            logger.warning(f"Query decomposition failed: {e}")
        return []

    async def query(self, question: str, conversation_history: list = None, model_override: str = None, mode: str = "deep") -> dict:
        """Answer a question using mode-specific retrieval + synthesis."""
        mode = self._normalize_mode(mode)
        self._model_override = model_override
        conv_suffix = ""
        if conversation_history:
            conv_text = " ".join(m.get("content", "")[:50] for m in conversation_history[-4:])
            conv_suffix = hashlib.md5(conv_text.encode()).hexdigest()[:8]
        cache_key = normalize_query_key(f"{QUERY_CACHE_VERSION}:{mode}:{question}{conv_suffix}")
        cached = query_cache.get(cache_key)
        if cached is not None:
            cached["cached"] = True
            return cached

        is_broad = mode != "quick" and self._is_broad_query(question)
        plan, trace = await self._build_query_plan(question, mode, conversation_history)
        if is_broad:
            plan["broad_query"] = True
            trace.append(trace_step("broad_query", "ok", "Broad query coverage enabled"))

        all_context, planned_queries_used, latest_check_used, retrieval_trace = await self._execute_retrieval_plan(question, plan, mode)
        trace.extend(retrieval_trace)

        first_pass, all_context, gap_follow_ups, gap_trace = await self._gap_review(
            question, all_context, conversation_history, mode, broad=is_broad
        )
        trace.extend(gap_trace)

        entities_found = first_pass.get("entities_found", []) or all_context.get("entity_names", [])
        all_context, graph_trace = await self._expand_planned_graph(all_context, entities_found)
        trace.extend(graph_trace)

        sources = self._build_sources(all_context, question=question)
        evidence_pack = await self._build_evidence_pack(question, all_context, sources, plan, mode, broad=is_broad)
        timeline_events, timeline_trace = await self._extract_timeline_events(question, all_context, sources, mode, broad=is_broad)
        trace.extend(timeline_trace)
        final = await self._final_synthesis(
            question,
            all_context,
            first_pass.get("draft_answer", ""),
            conversation_history,
            mode=mode,
            broad=is_broad,
            plan=plan,
            timeline_events=timeline_events,
            evidence_pack=evidence_pack,
        )
        answer, verification, evidence, verify_trace, claim_ledger, evidence_pack, sources, all_context = await self._verify_repair_and_grade(
            question,
            final.get("answer", ""),
            all_context,
            sources,
            plan,
            mode,
            broad=is_broad,
        )
        trace.extend(verify_trace)
        confidence = self._blend_confidence(final.get("confidence", first_pass.get("confidence", 0.5)), evidence, verification)
        source_summary = self._build_source_summary(
            all_context,
            latest_check_used,
            question=question,
            plan=plan,
            evidence=evidence,
            verification=verification,
            timeline_events=timeline_events,
            evidence_pack=evidence_pack,
            claim_ledger=claim_ledger,
        )

        result = {
            "question": question,
            "answer": answer,
            "confidence": confidence,
            "sources": sources,
            "source_summary": source_summary,
            "entities_found": [
                {"name": e} if isinstance(e, str) else e
                for e in entities_found[:30]
            ],
            "graph_nodes_used": len(all_context.get("graph_nodes", [])),
            "follow_up_queries_used": planned_queries_used + gap_follow_ups,
            "iterations": 1 + len(planned_queries_used) + len(gap_follow_ups),
            "mode": mode,
            "query_plan": plan,
            "trace": trace,
            "verification": verification,
            "evidence": evidence,
            "claim_ledger": claim_ledger,
            "evidence_pack": self._public_evidence_pack(evidence_pack),
            "current_state": current_state_summary(plan, sources),
            "timeline_events": timeline_events,
            "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
            "cached": False,
        }

        query_cache.set(cache_key, result)
        return result

    # ── Streaming query (SSE) ───────────────────────────────────────

    async def query_stream(self, question: str, conversation_history: list = None, model_override: str = None, mode: str = "deep"):
        """Stream query response via SSE events."""
        mode = self._normalize_mode(mode)
        self._model_override = model_override
        conv_suffix = ""
        if conversation_history:
            conv_text = " ".join(m.get("content", "")[:50] for m in conversation_history[-4:])
            conv_suffix = hashlib.md5(conv_text.encode()).hexdigest()[:8]
        cache_key = normalize_query_key(f"{QUERY_CACHE_VERSION}:{mode}:{question}{conv_suffix}")
        cached = query_cache.get(cache_key)
        if cached is not None:
            yield {"type": "answer_chunk", "content": cached["answer"]}
            yield {"type": "complete", "sources": cached["sources"],
                   "source_summary": cached.get("source_summary", {}),
                   "entities_found": cached.get("entities_found", []),
                   "confidence": cached.get("confidence", 0.7),
                   "follow_up_suggestions": cached.get("follow_up_suggestions", []),
                   "query_plan": cached.get("query_plan"),
                   "trace": cached.get("trace", []),
                   "verification": cached.get("verification", {}),
                   "evidence": cached.get("evidence", {}),
                   "claim_ledger": cached.get("claim_ledger", {}),
                   "evidence_pack": cached.get("evidence_pack", {}),
                   "current_state": cached.get("current_state", {}),
                   "timeline_events": cached.get("timeline_events", []),
                   "cached": True}
            return

        yield {"type": "status", "message": "Planning query workflow..."}
        is_broad = mode != "quick" and self._is_broad_query(question)
        plan, trace = await self._build_query_plan(question, mode, conversation_history)
        if is_broad:
            plan["broad_query"] = True
            trace.append(trace_step("broad_query", "ok", "Broad query coverage enabled"))
        for step in trace:
            yield {"type": "trace", "step": step}

        strategy_label = "single-pass search" if mode == "quick" else "parallel planned searches"
        if is_broad:
            strategy_label += " plus broad graph coverage"
        yield {"type": "status", "message": f"Running {strategy_label}..."}
        all_context, planned_queries_used, latest_check_used, retrieval_trace = await self._execute_retrieval_plan(question, plan, mode)
        trace.extend(retrieval_trace)
        for step in retrieval_trace:
            yield {"type": "trace", "step": step}

        yield {"type": "status", "message": "Reviewing evidence gaps..."}
        first_pass, all_context, gap_follow_ups, gap_trace = await self._gap_review(
            question, all_context, conversation_history, mode, broad=is_broad
        )
        trace.extend(gap_trace)
        for step in gap_trace:
            yield {"type": "trace", "step": step}

        entities_found = first_pass.get("entities_found", []) or all_context.get("entity_names", [])
        yield {"type": "status", "message": "Expanding related graph context..."}
        all_context, graph_trace = await self._expand_planned_graph(all_context, entities_found)
        trace.extend(graph_trace)
        for step in graph_trace:
            yield {"type": "trace", "step": step}

        sources = self._build_sources(all_context, question=question)
        evidence_pack = await self._build_evidence_pack(question, all_context, sources, plan, mode, broad=is_broad)
        if mode == "timeline":
            yield {"type": "status", "message": "Extracting and sorting timeline events..."}
        timeline_events, timeline_trace = await self._extract_timeline_events(question, all_context, sources, mode, broad=is_broad)
        trace.extend(timeline_trace)
        for step in timeline_trace:
            yield {"type": "trace", "step": step}

        yield {"type": "status", "message": "Synthesizing answer from ranked evidence..."}

        prompt = self._build_final_prompt(
            question,
            mode,
            self._format_doc_context(all_context, question=question, broad=is_broad),
            self._format_graph_context(all_context),
            first_pass.get("draft_answer", ""),
            conversation_history,
            plan=plan,
            timeline_events=timeline_events,
            evidence_pack=evidence_pack,
        )

        answer_chunks = []
        async for chunk in self._llm_generate_stream(prompt):
            answer_chunks.append(chunk)
            yield {"type": "answer_chunk", "content": chunk}

        full_answer = "".join(answer_chunks)
        preliminary_verification = {
            "status": "checking",
            "supported_claims": [],
            "unsupported_claims": [],
            "stale_or_conflicting_claims": [],
            "missing_evidence": [],
            "notes": ["Source support verification is still running."],
        }
        preliminary_evidence = compute_evidence_grade(
            question,
            plan,
            sources,
            all_context,
            preliminary_verification,
            evidence_pack=evidence_pack,
            claim_ledger={},
        )
        yield {"type": "answer_done",
               "answer": full_answer,
               "sources": sources,
               "source_summary": self._build_source_summary(
                   all_context,
                   latest_check_used,
                   question=question,
                   plan=plan,
                   evidence=preliminary_evidence,
                   verification=preliminary_verification,
                   evidence_pack=evidence_pack,
                   timeline_events=timeline_events,
               ),
               "entities_found": [
                   {"name": e} if isinstance(e, str) else e
                   for e in entities_found[:30]
               ],
               "query_plan": plan,
               "trace": trace,
               "evidence_pack": self._public_evidence_pack(evidence_pack),
               "current_state": current_state_summary(plan, sources),
               "timeline_events": timeline_events}

        yield {"type": "status", "message": "Verifying source support and trust score..."}
        base_trace = list(trace)
        progress_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        async def progress_callback(payload: dict[str, Any]) -> None:
            await progress_queue.put(payload)

        def metadata_update_event(payload: dict[str, Any]) -> dict[str, Any]:
            event_context = payload.get("context") or all_context
            event_sources = payload.get("sources") or sources
            event_evidence_pack = payload.get("evidence_pack") or evidence_pack
            event_verification = payload.get("verification") or {}
            event_evidence = payload.get("evidence") or preliminary_evidence
            event_claim_ledger = payload.get("claim_ledger") or {}
            event_trace = base_trace + list(payload.get("trace") or [])
            return {
                "type": "metadata_update",
                "stage": payload.get("stage"),
                "sources": event_sources,
                "source_summary": self._build_source_summary(
                    event_context,
                    latest_check_used,
                    question=question,
                    plan=plan,
                    evidence=event_evidence,
                    verification=event_verification,
                    timeline_events=timeline_events,
                    evidence_pack=event_evidence_pack,
                    claim_ledger=event_claim_ledger,
                ),
                "trace": event_trace,
                "verification": event_verification,
                "evidence": event_evidence,
                "claim_ledger": event_claim_ledger,
                "evidence_pack": self._public_evidence_pack(event_evidence_pack),
            }

        verification_task = asyncio.create_task(self._verify_repair_and_grade(
            question,
            full_answer,
            all_context,
            sources,
            plan,
            mode,
            broad=is_broad,
            progress_callback=progress_callback,
        ))
        while not verification_task.done():
            try:
                payload = await asyncio.wait_for(progress_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            yield metadata_update_event(payload)
        while not progress_queue.empty():
            yield metadata_update_event(progress_queue.get_nowait())

        repaired_answer, verification, evidence, verify_trace, claim_ledger, evidence_pack, sources, all_context = await verification_task
        if repaired_answer != full_answer:
            full_answer = repaired_answer
            yield {"type": "answer_replace", "content": full_answer}
        trace.extend(verify_trace)
        for step in verify_trace:
            yield {"type": "trace", "step": step}

        confidence = self._blend_confidence(first_pass.get("confidence", 0.7), evidence, verification)
        source_summary = self._build_source_summary(
            all_context,
            latest_check_used,
            question=question,
            plan=plan,
            evidence=evidence,
            verification=verification,
            timeline_events=timeline_events,
            evidence_pack=evidence_pack,
            claim_ledger=claim_ledger,
        )

        result = {
            "question": question,
            "answer": full_answer,
            "confidence": confidence,
            "sources": sources,
            "source_summary": source_summary,
            "entities_found": [
                {"name": e} if isinstance(e, str) else e
                for e in entities_found[:30]
            ],
            "graph_nodes_used": len(all_context.get("graph_nodes", [])),
            "follow_up_queries_used": planned_queries_used + gap_follow_ups,
            "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
            "iterations": 1 + len(planned_queries_used) + len(gap_follow_ups),
            "mode": mode,
            "query_plan": plan,
            "trace": trace,
            "verification": verification,
            "evidence": evidence,
            "claim_ledger": claim_ledger,
            "evidence_pack": self._public_evidence_pack(evidence_pack),
            "current_state": current_state_summary(plan, sources),
            "timeline_events": timeline_events,
            "cached": False,
        }
        query_cache.set(cache_key, result)

        yield {"type": "complete", "sources": sources,
               "source_summary": source_summary,
               "entities_found": result["entities_found"],
               "confidence": result.get("confidence", 0.7),
               "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
               "query_plan": plan,
               "trace": trace,
               "verification": verification,
               "evidence": evidence,
               "claim_ledger": claim_ledger,
               "evidence_pack": self._public_evidence_pack(evidence_pack),
               "current_state": result["current_state"],
               "timeline_events": timeline_events,
               "answer": full_answer,
               "cached": False}

    # ── Retrieval (TUNED: wider net) ────────────────────────────────

    async def _retrieve(self, query_text: str) -> dict:
        """Hybrid retrieval: vector + keyword + entity search + graph."""
        # Wider retrieval: 20 vector, 15 keyword, 8 entity
        vector_results = await self._cached_vector_search(query_text, limit=20)
        keyword_results = await embeddings_store.keyword_search(query_text, limit=15)
        entity_results = await embeddings_store.entity_vector_search(query_text, limit=8)
        entity_kw_results = await embeddings_store.entity_keyword_search(query_text, limit=8)
        entity_names = await self._extract_entities_from_query(query_text)

        graph_nodes = []
        seen_uuids = set()
        for name in entity_names:
            cache_key = f"gs:{hashlib.md5(name.encode()).hexdigest()}"
            cached = graph_cache.get(cache_key)
            if cached is not None:
                results = cached
            else:
                results = await graph_store.search_nodes(name, limit=8)
                graph_cache.set(cache_key, results)
            for r in results:
                uid = r.get("properties", {}).get("uuid", "")
                if uid and uid not in seen_uuids:
                    seen_uuids.add(uid)
                    graph_nodes.append(r)

        # Expand entity connections from top 8 vector results (was 5)
        doc_entity_uuids = set()
        for r in vector_results[:8]:
            doc_id = r.get("document_id")
            if doc_id:
                try:
                    neighbors = await graph_store.get_document_entities(doc_id)
                    for n in neighbors:
                        uid = n.get("uuid", "")
                        if uid:
                            doc_entity_uuids.add(uid)
                            if uid not in seen_uuids:
                                seen_uuids.add(uid)
                                graph_nodes.append({"labels": n.get("labels", []), "properties": n})
                except Exception:
                    pass

        # Wider subgraph: top 15 UUIDs, depth 3
        all_uuids = list(seen_uuids | doc_entity_uuids)
        subgraph = {}
        if all_uuids:
            try:
                sg_key = f"sg:{hashlib.md5(':'.join(sorted(all_uuids[:15])).encode()).hexdigest()}"
                cached = graph_cache.get(sg_key)
                if cached is not None:
                    subgraph = cached
                else:
                    subgraph = await graph_store.get_subgraph(all_uuids[:15], depth=3)
                    graph_cache.set(sg_key, subgraph)
            except Exception as e:
                logger.warning(f"Subgraph traversal failed: {e}")

        return {
            "vector_results": vector_results,
            "keyword_results": keyword_results,
            "entity_results": entity_results,
            "entity_kw_results": entity_kw_results,
            "graph_nodes": graph_nodes[:25],
            "subgraph": subgraph,
            "entity_names": entity_names,
        }

    async def _retrieve_light(self, query_text: str) -> dict:
        """Fast retrieval: vector + keyword only, no LLM entity extraction."""
        vector_results = await self._cached_vector_search(query_text, limit=15)
        keyword_results = await embeddings_store.keyword_search(query_text, limit=10)
        entity_results = await embeddings_store.entity_vector_search(query_text, limit=5)
        entity_kw_results = await embeddings_store.entity_keyword_search(query_text, limit=5)
        return {
            "vector_results": vector_results,
            "keyword_results": keyword_results,
            "entity_results": entity_results,
            "entity_kw_results": entity_kw_results,
            "graph_nodes": [],
            "subgraph": {},
            "entity_names": [],
        }

    async def _retrieve_graph_documents(self, question: str) -> dict:
        """Retrieve graph-linked documents for broad coverage."""
        entity_types = await self._get_relevant_entity_types(question)

        tasks = [self._graph_retrieve_by_org(entity_types=entity_types)]
        if entity_types:
            tasks.append(self._graph_retrieve_by_entity_type(entity_types))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_doc_ids = set()
        org_count = 0
        for r in results:
            if isinstance(r, dict):
                for did in r.get("doc_ids", []):
                    all_doc_ids.add(did)
                org_count = max(org_count, r.get("org_count", 0))

        if not all_doc_ids:
            return {}

        doc_ids_list = list(all_doc_ids)
        try:
            chunks = await embeddings_store.vector_search_by_doc_ids(
                query=question, doc_ids=doc_ids_list, limit=len(doc_ids_list)
            )
        except Exception as e:
            logger.warning(f"Graph-scoped vector search failed, falling back to batch retrieval: {e}")
            try:
                chunks = await embeddings_store.get_chunks_for_documents(doc_ids_list, chunks_per_doc=1)
            except Exception as e2:
                logger.warning(f"Batch chunk retrieval also failed: {e2}")
                chunks = []

        for chunk in chunks:
            chunk["_source"] = "graph_driven"

        logger.info(
            f"Graph-driven retrieval: {org_count} orgs + {len(entity_types)} entity types -> "
            f"{len(all_doc_ids)} unique docs -> {len(chunks)} chunks"
        )
        return {
            "vector_results": chunks,
            "keyword_results": [],
            "entity_results": [],
            "entity_kw_results": [],
            "graph_nodes": [],
            "subgraph": {},
            "entity_names": [],
        }

    async def _graph_retrieve_by_org(self, entity_types: list[str] = None) -> dict:
        """Get the most recent documents for each relevant Organization in the graph."""
        try:
            if entity_types:
                org_docs = await graph_store.get_recent_docs_per_organization_filtered(
                    entity_types=entity_types, limit_per_org=3
                )
            else:
                org_docs = await graph_store.get_recent_docs_per_organization(limit_per_org=3)

            org_seen = {}
            ranked_docs = []
            for d in org_docs:
                org = d["org_name"]
                org_seen[org] = org_seen.get(org, 0) + 1
                ranked_docs.append({
                    "doc_id": d["doc_id"],
                    "org_name": org,
                    "rank": org_seen[org],
                })

            doc_ids = [d["doc_id"] for d in ranked_docs]
            orgs = {d["org_name"] for d in ranked_docs}
            logger.info(f"Org-centric retrieval: {len(orgs)} organizations -> {len(doc_ids)} docs")
            return {"doc_ids": doc_ids, "org_count": len(orgs), "org_docs": ranked_docs}
        except Exception as e:
            logger.warning(f"Org-centric graph retrieval failed: {e}")
            return {"doc_ids": [], "org_count": 0, "org_docs": []}

    async def _graph_retrieve_by_entity_type(self, entity_types: list[str]) -> dict:
        """Get recent docs linked to specific entity types."""
        try:
            doc_ids = await graph_store.get_documents_by_entity_types(entity_types, limit=40)
            return {"doc_ids": doc_ids, "org_count": 0}
        except Exception as e:
            logger.warning(f"Entity-type graph retrieval failed: {e}")
            return {"doc_ids": [], "org_count": 0}

    async def _get_relevant_entity_types(self, question: str) -> list[str]:
        """Determine which entity types to query from the graph based on the question."""
        q_lower = question.lower()
        types = []

        financial_signals = [
            "bill", "payment", "financial", "obligation", "recurring", "monthly", "expense",
            "mortgage", "loan", "subscription", "utility", "owe", "pay", "cost", "fee",
            "charge", "balance", "statement", "account", "bank", "credit",
        ]
        if any(s in q_lower for s in financial_signals):
            types.extend(["FinancialItem", "Contract"])

        insurance_signals = ["insurance", "policy", "coverage", "premium", "deductible", "claim", "insured", "underwriter", "liability"]
        if any(s in q_lower for s in insurance_signals):
            types.extend(["InsurancePolicy"])

        medical_signals = [
            "medical", "health", "diagnosis", "condition", "disability", "medication",
            "treatment", "doctor", "hospital", "va ", "veteran", "rating", "vaccine",
            "immunization", "lab", "blood", "test result", "prescription", "surgery",
            "dental", "vision", "therapy", "physical",
        ]
        if any(s in q_lower for s in medical_signals):
            types.extend(["MedicalResult", "Condition"])

        equipment_signals = [
            "mower", "vehicle", "car", "truck", "device", "appliance", "manual",
            "instructions", "oil change", "maintenance", "repair", "equipment",
            "tool", "machine", "model", "serial number", "warranty",
        ]
        if any(s in q_lower for s in equipment_signals):
            types.extend(["Product", "System"])

        legal_signals = ["contract", "agreement", "lease", "terms", "deed", "legal", "court", "attorney", "settlement", "notarized", "signed"]
        if any(s in q_lower for s in legal_signals):
            types.extend(["Contract", "DocumentRef"])

        event_signals = ["when did", "date", "timeline", "history", "deployment", "service record", "stationed", "assigned", "milestone", "ceremony", "graduation"]
        if any(s in q_lower for s in event_signals):
            types.extend(["DateEvent", "Event"])

        location_signals = ["where", "location", "address", "stationed", "deployed", "lived", "moved", "residence", "city", "state", "base"]
        if any(s in q_lower for s in location_signals):
            types.extend(["Location", "Address"])

        property_signals = ["property", "house", "home", "real estate", "mortgage", "escrow", "hoa", "homeowner"]
        if any(s in q_lower for s in property_signals):
            types.extend(["Address", "Contract", "FinancialItem"])

        people_signals = ["who", "person", "people", "family", "contact", "employee", "spouse", "dependent", "beneficiary"]
        if any(s in q_lower for s in people_signals):
            types.extend(["Person"])

        org_signals = ["company", "employer", "provider", "vendor", "agency", "organization"]
        if any(s in q_lower for s in org_signals):
            types.extend(["Organization"])

        is_broad = any(w in q_lower for w in ["all", "every", "everything", "comprehensive", "complete"])
        if is_broad and len(set(types)) < 3:
            types.extend(["FinancialItem", "InsurancePolicy", "Contract", "MedicalResult"])

        return list(set(types))

    # ── Gap analysis (TUNED: smarter follow-ups) ────────────────────

    async def _synthesize_with_gaps(self, question: str, context: dict, conversation_history: list = None) -> dict:
        doc_context = self._format_doc_context(context, question=question)
        graph_text = self._format_graph_context(context)

        conv_context = ""
        if conversation_history:
            conv_lines = []
            for msg in conversation_history[-6:]:  # Last 6 messages (3 Q&A pairs)
                role = "User" if msg.get("role") == "user" else "Assistant"
                conv_lines.append(f"{role}: {msg['content'][:500]}")
            newline = "\n"
            conv_context = f"""\n\nPrevious conversation context:\n{newline.join(conv_lines)}\n"""

        prompt = f"""You are a knowledge assistant analyzing personal documents belonging to {_owner_name()}.
{conv_context}
Current question: {question}

Document context:
{doc_context}
{graph_text}

Analyze the context and provide:
1. A draft answer — be specific, cite document titles, and stay scoped to the user's question. Include all relevant answer details you can find, but do not add administrative/source metadata or adjacent facts merely because they are present in retrieved evidence. If this is a follow-up question, use the conversation context to understand what "it", "that", "more details", etc. refer to.
2. A confidence score (0-1) for completeness.
3. What information is MISSING or could be more complete? Generate exactly 5 targeted follow-up SEARCH queries to fill gaps. BE SPECIFIC — each query should target a SPECIFIC document type or known entity:
   - For each category mentioned in the question but not yet well-covered, generate a specific search like "GM Financial vehicle loan statement 2026" or "Progressive auto insurance declaration page"
   - Search for MORE RECENT versions of documents already found (e.g., "most recent GM Financial statement" or "2026 insurance policy renewal")
   - Search for specific account numbers, policy numbers, or entity names found in the context
   - Cover categories that are entirely missing from the retrieved context
4. List ALL entity names (people, organizations, places, conditions, etc.) mentioned.
5. Suggest 3-4 natural follow-up questions the user might want to ask next, based on what you found. Make them specific and interesting, not generic.

CRITICAL TEMPORAL AWARENESS:
- When dealing with ratings, statuses, or values that change over time, ALWAYS note you need to find the MOST RECENT/FINAL version. Generate a follow-up query specifically for "most recent" or "latest" or "final" version.
- Pay attention to document dates, policy effective periods, and statement periods. If a document has a date or effective period, use it to determine currency.
- Explicitly flag documents that appear EXPIRED or SUPERSEDED by newer ones (e.g., an old insurance policy replaced by a newer one, an old address that's no longer current, a payment amount that has since changed).
- When multiple documents cover the same topic (e.g., multiple mortgage statements), prefer the MOST RECENT and note if amounts or terms have changed.
- If a policy, contract, or service has an end date that has already passed, mark it as EXPIRED or PREVIOUS — do not list it as a current obligation.
- For addresses: note if a document references a previous address vs. the current primary residence.

Important: The user is {_owner_name()}. "my" or "I" = {_owner_name()}.

Respond in JSON: {{"draft_answer": "...", "confidence": 0.8, "follow_up_queries": ["search query 1", "search query 2", "search query 3", "search query 4", "search query 5"], "entities_found": ["Name1", "Name2"], "follow_up_suggestions": ["What is my current VA disability rating?", "Tell me about my military deployments"]}}"""

        try:
            result = await self._llm_json(prompt)
            return {
                "draft_answer": result.get("draft_answer", ""),
                "confidence": float(result.get("confidence", 0.5)),
                "follow_up_queries": result.get("follow_up_queries", []),
                "entities_found": result.get("entities_found", []),
                "follow_up_suggestions": result.get("follow_up_suggestions", []),
            }
        except Exception as e:
            logger.warning(f"Gap analysis failed: {e}")
            return {"draft_answer": "", "confidence": 0.0, "follow_up_queries": [], "entities_found": []}

    # ── Graph expansion (TUNED: wider, deeper) ──────────────────────

    async def _expand_graph(self, entity_names: list) -> dict:
        graph_nodes = []
        seen_uuids = set()

        # Expand up to 12 entities (was 8), get 5 results each (was 3)
        for name in entity_names[:12]:
            if not isinstance(name, str):
                name = str(name)
            try:
                results = await graph_store.search_nodes(name, limit=5)
                for r in results:
                    uid = r.get("properties", {}).get("uuid", "")
                    if uid and uid not in seen_uuids:
                        seen_uuids.add(uid)
                        graph_nodes.append(r)
            except Exception:
                pass

        subgraph = {}
        if seen_uuids:
            try:
                # Deeper traversal: 15 seeds, depth 3
                subgraph = await graph_store.get_subgraph(list(seen_uuids)[:15], depth=3)
            except Exception as e:
                logger.warning(f"Graph expansion failed: {e}")

        return {
            "vector_results": [], "keyword_results": [],
            "entity_results": [], "entity_kw_results": [],
            "graph_nodes": graph_nodes, "subgraph": subgraph, "entity_names": [],
        }

    # ── Final synthesis (TUNED: exhaustive prompting) ───────────────

    def _build_final_prompt(
        self,
        question: str,
        mode: str,
        doc_context: str,
        graph_text: str,
        draft_answer: str,
        conversation_history: list = None,
        plan: dict | None = None,
        timeline_events: list[dict] | None = None,
        evidence_pack: dict | None = None,
    ) -> str:
        draft_section = ""
        if draft_answer:
            draft_section = f"\n\nDraft answer from initial analysis:\n{draft_answer}\n"

        conv_section = ""
        if conversation_history:
            conv_lines = []
            for msg in conversation_history[-6:]:
                role = "User" if msg.get("role") == "user" else "Assistant"
                conv_lines.append(f"{role}: {msg['content'][:800]}")
            newline = "\n"
            conv_section = f"\n\nPrevious conversation:\n{newline.join(conv_lines)}\n\nUse the conversation above to understand context for follow-up questions.\n"

        mode_instruction = {
            "quick": "Answer concisely. Use the strongest available sources and avoid unnecessary expansion.",
            "deep": "Answer comprehensively. Use all relevant details and cite the strongest sources.",
            "timeline": "Answer chronologically. Compare dates, revisions, current vs superseded records, and explain how the facts changed over time.",
            "strict": "Answer only from source-backed evidence. Prefer exact excerpts, call out uncertainty, and do not include unsupported precise claims.",
        }[mode]

        plan_section = ""
        if plan:
            plan_section = f"""\n\nStructured query plan:
{json.dumps(plan, indent=2, default=str)[:4000]}
"""

        timeline_section = ""
        if timeline_events:
            timeline_section = f"""\n\nDeterministically sorted timeline events:
{json.dumps(timeline_events[:30], indent=2, default=str)[:7000]}
"""

        evidence_section = ""
        if evidence_pack:
            evidence_section = f"""\n\nCanonical evidence pack used for this answer:
{format_evidence_pack_for_llm(evidence_pack, max_items=50, max_chars=28000)}
"""

        return f"""You are a knowledge assistant with access to {_owner_name()}'s personal document archive and knowledge graph. You have been given context from multiple retrieval passes across hundreds of personal documents.

CONTEXT ABOUT THE USER:
- The user is {_owner_name()}
- Documents include: medical records, VA disability ratings, military service records, financial documents, mortgage statements, legal contracts, vehicle records, pet/veterinary records, insurance policies, tax documents, employment records, and more
- When the user says "my", "I", "me" — they mean {_owner_name()}
- {("Additional context: " + _owner_context()) if _owner_context() else ""}

INSTRUCTIONS:
- Query mode: {mode}. {mode_instruction}
- Build the answer from the canonical evidence pack first. Use the document context only as backup.
- Be complete within the user's requested scope. Use every relevant answer detail from the context, but do not expand into adjacent facts, administrative metadata, account/client identifiers, providers, or source logistics unless the user asked for them or they are needed to disambiguate the answer.
- Every precise fact should be traceable to a specific source document/excerpt. If exact evidence is missing, say that instead of guessing.
- Distinguish document dates, generation dates, statement periods, service/specimen dates, effective dates, and expiration dates.
- For questions about identity ("who am I"), cover ALL life domains: personal info, military service, education, medical/health, disability status, financial overview, property, family, employment, vehicles, pets — whatever the documents reveal.
- For ratings/statuses that change over time (VA disability, credit scores, balances, etc.), always identify and clearly state the MOST RECENT / FINAL / CURRENT value. If multiple values exist across documents, show the progression chronologically and highlight the latest.
- For "latest/current/last" answers, state the newest source-backed value found in the evidence. Do not claim that a newer document does not contain the requested fact unless the evidence explicitly shows that absence; use retrieval-limited phrasing when needed.

TEMPORAL AWARENESS — CRITICAL:
- Every document has a date or effective period. USE THESE to determine what is CURRENT vs. EXPIRED.
- If an insurance policy has an effective period that ended before today, mark it as EXPIRED/PREVIOUS and clearly indicate the replacement policy if one exists.
- If a contract, lease, or subscription has expired, say so explicitly — do not present it as active.
- When payment amounts change over time (e.g., mortgage escrow adjustments), always report the CURRENT amount and note the progression.
- For addresses: distinguish between current residence and previous addresses. Do not list bills from a previous address as current obligations unless there's evidence of ongoing service.
- When two policies/services of the same type overlap, determine which is the ACTIVE one based on effective dates and mark the other as superseded.
- Do not make negative absence claims (for example, "document X has no newer result") unless the source context explicitly proves that absence. Prefer "I did not find a newer source-backed value in the retrieved evidence."
- Today's date for reference: use the most recent document dates as a proxy for "now".
- Cite sources using document TITLES: (Source: "Document Title")
- If no title available, use: (Document 305)
- Include the specific dates, amounts, percentages, names, terms, identifiers, and statuses needed to answer the question. Do not include unrelated precise details just because they are source-backed.
- Format monetary values ($1,234.56), dates (January 15, 2024), and percentages (100%) clearly
- If information conflicts between documents, note BOTH, explain which is more current based on dates, and clearly label the outdated one as PREVIOUS/EXPIRED/SUPERSEDED
- Reference knowledge graph relationships when they add context
- Structure complex answers with clear headers and bullet points
- State what you could NOT find or what's missing from the archive
- When multiple documents corroborate the same fact, cite all of them for completeness

	Question: {question}
	{conv_section}{draft_section}{plan_section}{timeline_section}{evidence_section}
	Document context (from {len(doc_context.split(chr(10)+chr(10)))} retrieval passes):
	{doc_context}
	{graph_text}

	Provide your comprehensive, exhaustive answer with inline citations:"""

    async def _final_synthesis(
        self,
        question: str,
        context: dict,
        draft_answer: str,
        conversation_history: list = None,
        mode: str = "deep",
        broad: bool = False,
        plan: dict | None = None,
        timeline_events: list[dict] | None = None,
        evidence_pack: dict | None = None,
    ) -> dict:
        doc_context = self._format_doc_context(context, question=question, broad=broad)
        graph_text = self._format_graph_context(context)
        prompt = self._build_final_prompt(
            question,
            self._normalize_mode(mode),
            doc_context,
            graph_text,
            draft_answer,
            conversation_history,
            plan=plan,
            timeline_events=timeline_events,
            evidence_pack=evidence_pack,
        )

        try:
            answer = await self._llm_generate(prompt)
        except Exception as e:
            logger.error(f"Final synthesis failed: {e}")
            answer = draft_answer if draft_answer else f"Error generating answer: {e}"

        confidence = 0.7
        try:
            conf_prompt = f"""Rate your confidence (0-1) in how completely the following answer addresses the question.
Question: {question}
Answer: {answer[:2000]}
Respond with just a JSON object: {{"confidence": 0.8}}"""
            conf_result = await self._llm_json(conf_prompt)
            confidence = float(conf_result.get("confidence", 0.7))
        except Exception:
            pass

        return {"answer": answer, "confidence": confidence}

    def _normalize_mode(self, mode: str) -> str:
        return normalize_mode(mode)

    # ── Context merging ─────────────────────────────────────────────

    def _merge_context(self, ctx1: dict, ctx2: dict) -> dict:
        merged = {}

        def stable_key(value: Any) -> Any:
            if isinstance(value, (dict, list, set, tuple)):
                return json.dumps(value, sort_keys=True, default=str)
            return value

        def collect_entity_name(value: Any) -> list[str]:
            if isinstance(value, str):
                return [value] if value.strip() else []
            if isinstance(value, dict):
                name = value.get("name") or value.get("label")
                if not name and isinstance(value.get("properties"), dict):
                    name = value["properties"].get("name")
                return [str(name)] if name else []
            if isinstance(value, list):
                names = []
                for item in value:
                    names.extend(collect_entity_name(item))
                return names
            return [str(value)] if value else []

        for key in ("vector_results", "keyword_results"):
            seen = set()
            combined = []
            for r in ctx1.get(key, []) + ctx2.get(key, []):
                k = stable_key((r.get("document_id"), r.get("chunk_index", 0)))
                if k not in seen:
                    seen.add(k)
                    combined.append(r)
            merged[key] = combined

        for key in ("entity_results", "entity_kw_results"):
            seen = set()
            combined = []
            for r in ctx1.get(key, []) + ctx2.get(key, []):
                k = stable_key(r.get("entity_uuid", id(r)))
                if k not in seen:
                    seen.add(k)
                    combined.append(r)
            merged[key] = combined

        seen_uuids = set()
        merged_nodes = []
        for n in ctx1.get("graph_nodes", []) + ctx2.get("graph_nodes", []):
            uid = n.get("properties", {}).get("uuid", "")
            if uid and uid not in seen_uuids:
                seen_uuids.add(uid)
                merged_nodes.append(n)
        merged["graph_nodes"] = merged_nodes

        sg1 = ctx1.get("subgraph", {})
        sg2 = ctx2.get("subgraph", {})
        merged["subgraph"] = sg2 if (sg2 and (not sg1 or len(str(sg2)) > len(str(sg1)))) else sg1

        entity_names = []
        seen_names = set()
        for item in ctx1.get("entity_names", []) + ctx2.get("entity_names", []):
            for name in collect_entity_name(item):
                if name not in seen_names:
                    seen_names.add(name)
                    entity_names.append(name)
        merged["entity_names"] = entity_names
        return merged

    # ── Formatting (TUNED: more context to LLM) ────────────────────

    async def _build_evidence_pack(
        self,
        question: str,
        context: dict,
        sources: list[dict],
        plan: dict,
        mode: str,
        broad: bool = False,
    ) -> dict:
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", []),
            question=question,
        )
        high_accuracy = normalize_mode(mode) == "strict" or is_high_stakes_query(question, plan)
        base_limit = 85 if broad else 55 if high_accuracy else 36
        selected = self._diversify_chunks(
            combined,
            limit=base_limit,
            max_per_doc=4 if high_accuracy else 3,
        )

        # Pull neighboring chunks from cited/high-rank docs so synthesis and
        # verification see enough local context around exact values.
        doc_ids = []
        for source in sources[:10 if high_accuracy else 6]:
            if source.get("document_id") is not None:
                doc_ids.append(int(source["document_id"]))
        for chunk in selected[:14 if high_accuracy else 8]:
            if chunk.get("document_id") is not None:
                doc_ids.append(int(chunk["document_id"]))
        doc_ids = list(dict.fromkeys(doc_ids))[:14 if high_accuracy else 8]
        if doc_ids:
            try:
                neighbor_chunks = await embeddings_store.get_chunks_for_documents(
                    doc_ids,
                    chunks_per_doc=8 if high_accuracy else 4,
                )
                full_doc_chunks = await self._expand_source_documents(
                    question,
                    doc_ids[:5 if high_accuracy else 3],
                    high_accuracy=high_accuracy,
                )
                selected_for_merge = [
                    {**chunk, "similarity": chunk.get("combined_score", chunk.get("similarity", 0))}
                    for chunk in selected
                ]
                selected = self._merge_and_rank(
                    selected_for_merge + neighbor_chunks + full_doc_chunks,
                    [],
                    question=question,
                )
            except Exception as e:
                logger.warning("Evidence neighbor chunk expansion failed: %s", e)
        selected = self._rank_evidence_chunks(selected, question, sources)

        return build_evidence_pack(
            question=question,
            plan=plan,
            chunks=selected,
            sources=sources,
            max_items=90 if broad or high_accuracy else 60,
        )

    async def _expand_source_documents(
        self,
        question: str,
        doc_ids: list[int],
        high_accuracy: bool = False,
    ) -> list[dict]:
        """Fetch full Paperless text for top source docs in strict/high-stakes mode.

        Retrieval chunks are intentionally narrow. For exact-answer workflows,
        especially medical/lab, tax, insurance, and legal questions, the top
        source document often contains neighboring tables or values that did not
        win vector ranking. This expansion promotes bounded full-document chunks
        into the evidence pack without changing the indexed graph.
        """
        if not high_accuracy or not doc_ids:
            return []

        expanded = []
        seen = set()
        for doc_id in doc_ids[:5]:
            if doc_id in seen:
                continue
            seen.add(doc_id)
            try:
                doc = await paperless_client.get_document(int(doc_id))
            except Exception as e:
                logger.warning("Full source fetch failed for Paperless doc %s: %s", doc_id, e)
                continue

            content = str(doc.get("content") or "")
            if not content.strip():
                continue
            title = str(doc.get("title") or f"Document {doc_id}")
            doc_type = str(doc.get("document_type") or "")
            if isinstance(doc.get("document_type"), dict):
                doc_type = str(doc["document_type"].get("name") or "")
            date = doc.get("created") or doc.get("modified") or doc.get("added")

            prefix = f"Title: {title}\nDocument ID: {doc_id}\nDate: {date or 'unknown'}\n\n"
            chunks = chunk_text(content, chunk_size=3600, overlap=500)
            ranked_chunks = self._rank_full_document_chunks(question, chunks)
            for rank, text in ranked_chunks[:10]:
                expanded.append({
                    "document_id": int(doc_id),
                    "chunk_index": 100000 + rank,
                    "content": prefix + text,
                    "title": title,
                    "doc_type": doc_type,
                    "similarity": 0.78,
                    "rank_score": 1.0,
                    "_source": "paperless_full_document",
                })
        if expanded:
            logger.info("Expanded %s full-document evidence chunks from %s source docs", len(expanded), len(seen))
        return expanded

    def _rank_full_document_chunks(self, question: str, chunks: list[str]) -> list[tuple[int, str]]:
        query_terms = self._query_terms_with_concept_expansion(question)

        def score(index_and_text: tuple[int, str]) -> float:
            idx, text = index_and_text
            lower = text.lower()
            hits = sum(1 for term in query_terms if term in lower)
            structured = min(1.0, structured_fact_count(text[:4000]) / 3)
            table = 1 if "|" in text[:2500] and text.count("|") >= 4 else 0
            date = 1 if re.search(r"\b(19|20)\d{2}\b", lower) else 0
            return hits + 0.75 * structured + 0.5 * table + 0.25 * date - (idx * 0.01)

        indexed = list(enumerate(chunks))
        return sorted(indexed, key=score, reverse=True)

    def _query_terms_with_concept_expansion(self, question: str) -> set[str]:
        terms = {
            term
            for term in re.findall(r"[a-z0-9$]+", question.lower())
            if len(term) >= 3
        }
        expanded = set(terms)
        for term in list(terms):
            if term.endswith("s") and len(term) > 3:
                expanded.add(term[:-1])
            else:
                expanded.add(f"{term}s")

        # Domain-neutral concept groups. If the user asks for a result, amount,
        # status, date, identifier, obligation, or governing document, nearby
        # source language often uses a sibling word instead of the query token.
        concept_groups = [
            {"level", "levels", "result", "results", "value", "values", "amount", "amounts", "balance", "total", "score", "range", "status"},
            {"latest", "last", "recent", "current", "newest", "final", "updated", "revised", "effective", "expiration", "expires"},
            {"payment", "payments", "installment", "installments", "due", "premium", "charge", "fee", "invoice", "receipt", "statement"},
            {"coverage", "policy", "contract", "agreement", "declaration", "terms", "benefit", "benefits"},
            {"identifier", "number", "id", "account", "policy", "claim", "case", "reference", "serial", "vin", "ein", "ssn", "sku"},
            {"income", "tax", "return", "w2", "1099", "k1", "schedule"},
            {"record", "report", "summary", "document", "source", "form", "notice"},
        ]
        for group in concept_groups:
            if expanded & group:
                expanded.update(group)
        return expanded

    def _rank_evidence_chunks(self, chunks: list[dict], question: str, sources: list[dict]) -> list[dict]:
        source_rank = {
            int(source["document_id"]): idx
            for idx, source in enumerate(sources)
            if source.get("document_id") is not None
        }

        def score(chunk: dict) -> float:
            doc_id = chunk.get("document_id")
            base = float(chunk.get("rerank_score", chunk.get("combined_score", chunk.get("similarity", 0))) or 0)
            title = str(chunk.get("title") or "")
            content = str(chunk.get("content") or "")
            title_hits = evidence_exact_term_hits(question, title)
            content_hits = evidence_exact_term_hits(question, content[:2500])
            source_boost = 0.0
            if doc_id is not None and int(doc_id) in source_rank:
                source_boost = max(0.0, 0.35 - 0.025 * source_rank[int(doc_id)])
            full_doc_boost = 0.22 if chunk.get("_source") == "paperless_full_document" else 0.0
            return base + 0.18 * title_hits + 0.05 * content_hits + source_boost + full_doc_boost

        return sorted(chunks, key=score, reverse=True)

    def _format_doc_context(self, context: dict, question: str = "", broad: bool = False) -> str:
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", []),
            question=question,
        )
        # Broad queries get more chunks for category coverage.
        chunk_limit = 75 if broad else 25
        if broad:
            top = self._diversify_chunks(combined, limit=chunk_limit, max_per_doc=2)
        else:
            top = combined[:chunk_limit]

        parts = []
        for r in top:
            title = r.get("title", "")
            doc_type = r.get("doc_type", "")
            if title:
                header = f'[Source: "{title}"'
                if doc_type:
                    header += f" ({doc_type})"
                header += f" — Document {r['document_id']}, chunk {r.get('chunk_index', 0)}]"
            else:
                header = f"[Document {r['document_id']}"
                if doc_type:
                    header += f" ({doc_type})"
                header += f", chunk {r.get('chunk_index', 0)}]"
            parts.append(f"{header}:\n{r['content'][:3000]}")

        # More entity context: 12 entities (was 8)
        entity_parts = []
        seen_entities = set()
        for r in context.get("entity_results", []) + context.get("entity_kw_results", []):
            eid = r.get("entity_uuid", "")
            if eid in seen_entities:
                continue
            seen_entities.add(eid)
            entity_parts.append(
                f"[Entity: {r.get('entity_name', '')} ({r.get('entity_type', '')})] {r.get('content', '')[:500]}"
            )

        unique_doc_ids = {r.get("document_id") for r in top}
        logger.info(f"Synthesis context: {len(top)} chunks from {len(unique_doc_ids)} unique documents (broad={broad})")

        result = "\n\n".join(parts)
        if entity_parts:
            result += "\n\nEntity matches:\n" + "\n".join(entity_parts[:12])
        return result

    def _format_graph_context(self, context: dict) -> str:
        graph_nodes = context.get("graph_nodes", [])
        subgraph = context.get("subgraph", {})
        if not graph_nodes and not subgraph:
            return ""
        # More graph context: 25 nodes, 10k chars (was 15 nodes, 6k)
        graph_data = {"nodes": graph_nodes[:25]}
        if subgraph:
            graph_data["subgraph"] = subgraph
        return "\n\nKnowledge Graph context:\n" + json.dumps(graph_data, indent=2, default=str)[:10000]

    def _build_sources(self, context: dict, question: str = "") -> list[dict]:
        """Build deduplicated source citations grouped by document."""
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", []),
            question=question,
        )

        doc_groups = defaultdict(list)
        # Consider top 25 results for source grouping (was 15)
        for r in combined[:25]:
            doc_groups[r["document_id"]].append(r)

        sources = []
        for doc_id, chunks in doc_groups.items():
            chunks.sort(key=lambda x: x.get("combined_score", x.get("similarity", 0)), reverse=True)
            best = chunks[0]

            source = {
                "document_id": doc_id,
                "chunk_index": best.get("chunk_index", 0),
                "similarity": float(best.get("combined_score", best.get("similarity", 0))),
            }
            if best.get("title"):
                source["title"] = best["title"]
            if best.get("doc_type"):
                source["doc_type"] = best["doc_type"]
            source_date = self._extract_source_date(best)
            if source_date:
                source["date"] = source_date
            date_signals = extract_date_signals(
                best.get("content", ""),
                title=best.get("title", ""),
                fallback_date=source_date,
            )
            if date_signals:
                source["date_signals"] = date_signals
            source["source_quality"] = infer_source_quality(
                title=best.get("title", ""),
                doc_type=best.get("doc_type", ""),
                content=best.get("content", ""),
            )
            if len(chunks) > 1:
                source["excerpt_count"] = len(chunks)
            source["excerpt"] = best.get("content", "")[:300]
            sources.append(source)

        sources.sort(key=lambda x: x["similarity"], reverse=True)
        return sources[:15]  # Return up to 15 sources (was 10)

    def _build_source_summary(
        self,
        context: dict,
        latest_check_used: bool,
        question: str = "",
        plan: dict | None = None,
        evidence: dict | None = None,
        verification: dict | None = None,
        timeline_events: list[dict] | None = None,
        evidence_pack: dict | None = None,
        claim_ledger: dict | None = None,
    ) -> dict:
        sources = self._build_sources(context, question=question)
        dates = [s["date"] for s in sources if s.get("date")]
        latest_retrieved_date = max(dates) if dates else None
        plan = plan or {}
        evidence = evidence or {}
        verification = verification or {}
        evidence_pack = evidence_pack or {}
        claim_ledger = claim_ledger or {}
        supporting_dates = self._supporting_evidence_dates(evidence_pack, question)
        latest_supporting_date = max(supporting_dates) if supporting_dates else None
        current = current_state_summary(plan, sources)
        return {
            "latest_source_date": latest_supporting_date or latest_retrieved_date,
            "latest_retrieved_source_date": latest_retrieved_date,
            "latest_supporting_source_date": latest_supporting_date,
            "latest_check_used": latest_check_used,
            "source_count": len(sources),
            "newer_docs_may_exist": not latest_check_used,
            "trust_score": evidence.get("score"),
            "trust_level": evidence.get("level"),
            "trust_reasons": evidence.get("reasons", []),
            "trust_penalties": evidence.get("penalties", []),
            "trust_dimensions": evidence.get("dimensions", {}),
            "audit_status": evidence.get("audit_status"),
            "claim_summary": claim_ledger.get("summary", {}),
            "evidence_coverage": (evidence_pack.get("coverage") or {}),
            "verification_status": verification.get("status"),
            "unsupported_claim_count": len(verification.get("unsupported_claims") or []),
            "stale_or_conflicting_claim_count": len(verification.get("stale_or_conflicting_claims") or []),
            "current_state": current,
            "timeline_event_count": len(timeline_events or []),
        }

    def _supporting_evidence_dates(self, evidence_pack: dict, question: str) -> list[str]:
        focus_terms = self._supporting_focus_terms(evidence_pack, question)
        dates = []
        for item in evidence_pack.get("items") or []:
            if not item.get("exact_term_hits"):
                continue
            text = f"{item.get('title') or ''} {item.get('doc_type') or ''} {item.get('excerpt') or ''}".lower()
            if focus_terms and not (evidence_exact_term_matches(question, text) & focus_terms):
                continue
            signals = item.get("date_signals") or {}
            for key in ("reported_date", "document_date", "specimen_date", "service_date"):
                for value in signals.get(key) or []:
                    normalized = self._normalize_summary_date(str(value))
                    if normalized:
                        dates.append(normalized)
        return list(dict.fromkeys(dates))

    def _supporting_focus_terms(self, evidence_pack: dict, question: str) -> set[str]:
        query_terms = evidence_query_terms(question)
        if not query_terms:
            return set()

        counts: Counter[str] = Counter()
        for item in evidence_pack.get("items") or []:
            text = f"{item.get('title') or ''} {item.get('doc_type') or ''} {item.get('excerpt') or ''}"
            counts.update(evidence_exact_term_matches(question, text) & query_terms)
        if not counts:
            return set()

        # Use the rarest matched query terms as the date-support focus. This is
        # intentionally domain-neutral: exact short codes, IDs, form names, lab
        # markers, vehicle VIN terms, tax form terms, and other specific tokens
        # naturally beat broad words that appear across many retrieved docs.
        rarest_count = min(counts.values())
        max_count = max(rarest_count, min(3, len(evidence_pack.get("items") or [])))
        return {term for term, count in counts.items() if count <= max_count}

    def _normalize_summary_date(self, value: str) -> str | None:
        iso_match = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", value)
        if iso_match:
            return "-".join(iso_match.groups())
        slash_match = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", value)
        if slash_match:
            month, day, year = slash_match.groups()
            return f"{year}-{int(month):02d}-{int(day):02d}"
        long_match = re.search(r"\b([A-Z][a-z]+) (\d{1,2}), (20\d{2})\b", value)
        if long_match:
            month_name, day, year = long_match.groups()
            try:
                month = datetime.strptime(month_name, "%B").month
            except ValueError:
                return None
            return f"{year}-{month:02d}-{int(day):02d}"
        return None

    def _public_evidence_pack(self, evidence_pack: dict | None) -> dict:
        """Return UI-safe evidence metadata without sending full chunk bodies."""
        evidence_pack = evidence_pack or {}
        items = []
        for item in (evidence_pack.get("items") or [])[:30]:
            items.append({
                "id": item.get("id"),
                "document_id": item.get("document_id"),
                "chunk_index": item.get("chunk_index"),
                "title": item.get("title"),
                "doc_type": item.get("doc_type"),
                "source_quality": item.get("source_quality"),
                "date_signals": item.get("date_signals"),
                "structured_fact_count": item.get("structured_fact_count"),
                "exact_term_hits": item.get("exact_term_hits"),
                "excerpt": item.get("excerpt"),
            })
        return {
            "coverage": evidence_pack.get("coverage") or {},
            "source_documents": evidence_pack.get("source_documents") or [],
            "items": items,
        }

    def _requires_latest_check(self, question: str) -> bool:
        sensitive_terms = (
            "insurance", "policy", "coverage", "premium", "deductible",
            "medical", "health", "doctor", "diagnosis", "medication", "prescription",
            "tax", "irs", "return", "w2", "1099", "mortgage", "loan", "balance",
            "finance", "financial", "bank", "account", "contract", "legal", "lease",
            "rating", "disability", "va",
        )
        q = question.lower()
        return any(term in q for term in sensitive_terms)

    def _latest_check_query(self, question: str) -> str:
        return f"{question} latest current final most recent updated revised declaration statement policy"

    def _extract_source_date(self, result: dict) -> str | None:
        content = result.get("content", "") or ""
        match = re.search(r"^Date:\s*([^\n]+)", content, flags=re.MULTILINE)
        if match:
            value = match.group(1).strip()
            return value if value and value.lower() != "unknown" else None
        return None

    # ── Existing helpers ────────────────────────────────────────────

    async def _cached_vector_search(self, query: str, limit: int = 20) -> list[dict]:
        cache_key = f"vs:{hashlib.md5(query.encode()).hexdigest()}:{limit}"
        cached = vector_cache.get(cache_key)
        if cached is not None:
            return cached
        results = await embeddings_store.vector_search(query, limit=limit)
        vector_cache.set(cache_key, results)
        return results

    def _diversify_chunks(self, ranked_chunks: list[dict], limit: int = 40, max_per_doc: int = 2) -> list[dict]:
        """Select chunks with document diversity while preserving high-scoring matches."""
        vector_chunks = [c for c in ranked_chunks if c.get("_source") != "graph_driven"]
        graph_chunks = [c for c in ranked_chunks if c.get("_source") == "graph_driven"]

        logger.info(f"Chunk selection: {len(ranked_chunks)} total ({len(vector_chunks)} vector, {len(graph_chunks)} graph), limit={limit}")

        selected = []
        seen_doc_ids = set()
        doc_counts = defaultdict(int)

        tier1_limit = int(limit * 0.5)
        for chunk in vector_chunks:
            doc_id = chunk.get("document_id")
            if doc_counts[doc_id] < max_per_doc:
                selected.append(chunk)
                doc_counts[doc_id] += 1
                seen_doc_ids.add(doc_id)
            if len(selected) >= tier1_limit:
                break

        graph_sorted = sorted(graph_chunks, key=lambda c: c.get("similarity", 0), reverse=True)
        for chunk in graph_sorted:
            doc_id = chunk.get("document_id")
            if doc_id not in seen_doc_ids and doc_counts[doc_id] < max_per_doc:
                selected.append(chunk)
                doc_counts[doc_id] += 1
                seen_doc_ids.add(doc_id)
            if len(selected) >= limit:
                break

        if len(selected) < limit:
            remaining = [c for c in ranked_chunks if c not in selected]
            for chunk in remaining:
                doc_id = chunk.get("document_id")
                if doc_counts[doc_id] < max_per_doc:
                    selected.append(chunk)
                    doc_counts[doc_id] += 1
                    seen_doc_ids.add(doc_id)
                if len(selected) >= limit:
                    break

        logger.info(f"Final selection: {len(selected)} chunks from {len(seen_doc_ids)} unique documents")
        return selected

    def _merge_and_rank(self, vector_results: list[dict], keyword_results: list[dict], question: str = "") -> list[dict]:
        scored = {}
        for r in vector_results:
            key = (r["document_id"], r.get("chunk_index", 0))
            if key not in scored:
                scored[key] = {**r, "vector_score": float(r.get("similarity", 0)), "keyword_score": 0.0}
            else:
                scored[key]["vector_score"] = max(scored[key].get("vector_score", 0), float(r.get("similarity", 0)))
                if r.get("_source") == "graph_driven":
                    scored[key]["_source"] = "graph_driven"

        for r in keyword_results:
            key = (r["document_id"], r.get("chunk_index", 0))
            if key not in scored:
                scored[key] = {**r, "vector_score": 0.0, "keyword_score": float(r.get("rank_score", 0.5))}
            else:
                scored[key]["keyword_score"] = max(scored[key].get("keyword_score", 0), float(r.get("rank_score", 0.5)))

        for key, r in scored.items():
            r["combined_score"] = 0.7 * r.get("vector_score", 0) + 0.3 * r.get("keyword_score", 0)
            r["rerank_score"] = self._rerank_score(r, question)

        return sorted(scored.values(), key=lambda x: x["rerank_score"], reverse=True)

    def _rerank_score(self, result: dict, question: str) -> float:
        base = float(result.get("combined_score", 0))
        query = question.lower()
        title = (result.get("title") or "").lower()
        doc_type = (result.get("doc_type") or "").lower()
        content = (result.get("content") or "").lower()

        score = base + self._doc_type_boost(query, doc_type) + self._recency_boost(result)
        exact_hits = evidence_exact_term_hits(question, f"{title} {doc_type} {content[:5000]}")
        score += 0.18 * exact_hits
        if self._looks_superseded(content):
            score -= 0.18
        return score

    def _doc_type_boost(self, query: str, doc_type: str) -> float:
        mappings = [
            (("insurance", "policy", "coverage", "premium"), ("insurance", "policy"), 0.12),
            (("tax", "irs", "return", "w-2", "1099"), ("tax", "financial"), 0.12),
            (("medical", "doctor", "diagnosis", "medication", "lab", "labs", "blood", "bloodwork"), ("medical",), 0.12),
            (("mortgage", "loan", "escrow", "home"), ("mortgage", "statement", "contract"), 0.10),
            (("vehicle", "auto", "car", "truck", "vin"), ("vehicle", "registration", "insurance"), 0.10),
        ]
        for terms, types, boost in mappings:
            if any(term in query for term in terms) and any(t in doc_type for t in types):
                return boost
        return 0.0

    def _recency_boost(self, result: dict) -> float:
        date_value = self._extract_indexed_date(result.get("content", ""))
        if not date_value:
            return 0.0
        try:
            dt = datetime.fromisoformat(date_value.replace("Z", "+00:00"))
        except ValueError:
            return 0.0
        age_days = max((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).days, 0)
        if age_days <= 90:
            return 0.12
        if age_days <= 365:
            return 0.08
        if age_days <= 730:
            return 0.04
        return 0.0

    def _extract_indexed_date(self, content: str) -> str | None:
        match = re.search(r"^Date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2}[^\n]*)", content, flags=re.MULTILINE)
        return match.group(1).strip() if match else None

    def _looks_superseded(self, content: str) -> bool:
        terms = ("superseded", "replaced by", "cancelled", "canceled", "expired", "void", "prior version")
        return any(term in content for term in terms)

    async def _extract_entities_from_query(self, question: str) -> list[str]:
        try:
            prompt = f"""Extract person names, organization names, and key concepts/topics from this question.
Return a JSON object with an "entities" key containing an array of strings — just the names and key terms, nothing else.
If there are no specific entities, return the 2-3 most important search terms.
For broad questions like "who am I" or "tell me about myself", return: {json.dumps(_owner_name().split() + [_owner_name()]) if _owner_name() != "the document owner" else '["owner"]'}

Question: {question}"""
            result = await self._llm_json(prompt)
            entities = result.get("entities", [])
            if isinstance(entities, list):
                return [str(e) for e in entities if e][:10]
        except Exception as e:
            logger.warning(f"Entity extraction from query failed: {e}")
        words = question.split()
        return [w for w in words if len(w) > 3][:5]


query_engine = QueryEngine()
