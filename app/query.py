from app.source_text import certifying_text, bind_document_context
from app.history_coverage import history_requested, subject_terms, choose_documents, choose_recent_documents, MAX_CANDIDATES, MAX_DOCUMENTS
import json
import logging
import hashlib
import re
import asyncio
import contextlib
from contextvars import ContextVar
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
from app.cache import (query_cache, vector_cache, graph_cache,
    cache_get, cache_set, get_corpus_generation_async)
from app.answer_finalization import AnswerFinalizer, POLICY_VERSION, parse_date, evidence_spans, select_spans, empty_ledger
from app.timeline import validate_timeline
from app.query_quality import (
    current_state_summary,
    heuristic_plan,
    merge_agent_plan,
    normalize_mode,
    retrieval_queries,
    trace_step,
)
from app.evidence import (
    build_evidence_pack,
    extract_date_signals,
    exact_term_matches as evidence_exact_term_matches,
    exact_term_hits as evidence_exact_term_hits,
    format_evidence_pack_for_llm,
    infer_source_quality,
    is_high_stakes_query,
    query_terms as evidence_query_terms,
    structured_fact_count,
)
from app.strands_orchestrator import strands_orchestrator

logger = logging.getLogger(__name__)
QUERY_CACHE_VERSION = f"{POLICY_VERSION}:bounded-context-v1"
CONVERSATION_CONTEXT_MAX_CHARS = 12_000
_REQUEST_MODEL = ContextVar("query_model", default=None)
_REQUEST_GENERATION = ContextVar("query_generation", default="initial")


class QueryEngine:
    def __init__(self):
        self.client = AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
        self.model = settings.gemini_model

    async def close(self):
        await self.client.close()

    def _active_model(self, model_override=None):
        return model_override or _REQUEST_MODEL.get() or self.model

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
        """Keep recent follow-up context within a fixed model-input budget."""
        remaining = CONVERSATION_CONTEXT_MAX_CHARS
        lines = []
        for msg in reversed((conversation_history or [])[-10:]):
            role = "User" if msg.get("role") == "user" else "Assistant"
            prefix = f"{role}: "
            content = msg.get("content", "")
            if len(prefix) + len(content) > remaining:
                prefix += "[earlier content omitted] "
                available = remaining - len(prefix)
                if available <= 0:
                    break
                content = content[-available:]
            line = prefix + content
            lines.append(line)
            remaining -= len(line) + 1
            if remaining <= 0:
                break
        return "\n".join(reversed(lines))

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

        # All retrieval children belong to this request. Failure/cancellation
        # drains them before request cleanup or application shutdown closes I/O.
        async with asyncio.TaskGroup() as group:
            broad_decompose_task = group.create_task(self._decompose_query(question)) if is_broad else None
            graph_docs_task = group.create_task(self._retrieve_graph_documents(question)) if is_broad else None
            history_task = group.create_task(self._retrieve_history(
                question, include_history=history_requested(question, plan, mode),
                include_recent=bool(plan.get("requires_current")) or latest_check_used)) if (
                    history_requested(question, plan, mode) or plan.get("requires_current") or latest_check_used) else None
            retrieval_tasks = [group.create_task(_retrieve_one(item)) for item in queries]
        retrieved = [task.result() for task in retrieval_tasks]
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
            broad_queries = broad_decompose_task.result()
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
            graph_doc_context = graph_docs_task.result()
            if graph_doc_context:
                all_context = self._merge_context(all_context, graph_doc_context)
                trace.append(trace_step(
                    "graph_document_coverage",
                    "ok",
                    "Added graph-linked documents for broad coverage",
                    {"vector_results": len(graph_doc_context.get("vector_results", []))},
                ))

        if history_task:
            history = history_task.result()
            all_context = self._merge_context(all_context, history)
            if history.get("recent_document_ids"):
                trace.append(trace_step("recent_coverage", "limited" if history["recent_coverage"].get("truncated") else "ok",
                                        "Reserved distinct recent indexed records", history["recent_coverage"]))
            trace.append(trace_step("historical_coverage", "limited" if history["history_coverage"].get("truncated") else "ok",
                                    "Reserved indexed sources across recorded periods", history["history_coverage"]))
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

    async def _extract_timeline_events(self, question, context, sources, mode, broad=False, evidence_pack=None):
        if normalize_mode(mode) != "timeline":
            return [], []
        pack = evidence_pack or {}
        spans = select_spans(question, [], evidence_spans(pack), serialized=True)
        try:
            async with asyncio.timeout(settings.answer_audit_timeout_seconds):
                events = await strands_orchestrator.extract_timeline(question, json.dumps(spans, ensure_ascii=False))
                rejections = {}
                accepted = await validate_timeline(events, pack, strands_orchestrator, question, manifest=spans,
                                                   date_order=settings.source_date_order, diagnostics=rejections)
            return accepted, [trace_step("timeline", "ok" if accepted else "needs_review",
                                        f"{len(accepted)} source-validated events; {sum(rejections.values())} rejected",
                                        {"accepted": len(accepted), "rejection_reasons": rejections})]
        except (TimeoutError, Exception):
            return [], [trace_step("timeline", "needs_review", "Timeline audit unavailable; no unverified events published")]

    async def _verify_repair_and_grade(
        self, question, answer, context, sources, plan, mode, broad=False, progress_callback=None,
        evidence_pack=None, timeline_events=None,
    ):
        if evidence_pack is None:
            evidence_pack = await self._build_evidence_pack(question, context, sources, plan, mode, broad=broad)
        doc_ids = list({i["document_id"] for i in evidence_pack.get("items", []) if type(i.get("document_id")) is int})
        flagged = await embeddings_store.get_open_feedback_document_ids(doc_ids)
        for item in evidence_pack.get("items", []):
            item["feedback_open"] = item.get("document_id") in flagged
        if timeline_events is not None:
            timeline_events[:] = [event for event in timeline_events if event.get("document_id") not in flagged]
        final = await AnswerFinalizer(strands_orchestrator, strands_orchestrator,
                                      timeout_seconds=settings.answer_audit_timeout_seconds,
                                      concurrency=settings.strands_max_concurrent_calls,
                                      date_order=settings.source_date_order).finalize(
            question, answer, evidence_pack, plan=plan, mode=mode, evaluated_at=plan.get("evaluated_at"))
        verification = final["verification"]
        verification["finalization"] = final["finalization"]
        verification["current_state"] = final["current_state"]
        references = [r for c in final["claim_ledger"]["claims"] for r in c["references"]]
        references.extend(r for event in (timeline_events or []) for r in event.get("references", []))
        for item in evidence_pack.get("items", []):
            refs = [r for r in references if r["evidence_id"] == item["id"]]
            item["support_spans"] = refs
            if refs:
                item["excerpt"] = "\n…\n".join(dict.fromkeys(r["quote"] for r in refs))
        # Keep source titles and excerpts tied to validated source membership.
        cited = list(dict.fromkeys(final["finalization"]["cited_document_ids"] +
                                  [event["document_id"] for event in (timeline_events or [])]))
        if cited:
            sources = [{"document_id": doc_id, "title": next(r["source_title"] for r in references if r["document_id"] == doc_id),
                        "excerpt": "\n…\n".join(dict.fromkeys(r["quote"] for r in references if r["document_id"] == doc_id))}
                       for doc_id in cited]
        trace = [trace_step("source_audit", "ok" if final["finalization"]["complete"] else "needs_review",
                            final["finalization"]["disposition"], final["claim_ledger"]["summary"])]
        return final["answer"], verification, final["evidence"], trace, final["claim_ledger"], evidence_pack, sources, context

    def _blend_confidence(self, llm_confidence, evidence, verification):
        # Provider self-confidence cannot raise a failed source-audit verdict.
        return float(evidence.get("score", 0.0))

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

    async def query(self, question: str, conversation_history: list = None, model_override: str = None, mode: str = "strict") -> dict:
        token = _REQUEST_MODEL.set(model_override or self.model)
        generation = await get_corpus_generation_async()
        generation_token = _REQUEST_GENERATION.set(generation)
        try:
            result = await self._query(question, conversation_history, mode)
            await self._check_delivery_snapshot(result)
            return result
        finally:
            _REQUEST_MODEL.reset(token)
            _REQUEST_GENERATION.reset(generation_token)

    async def _query(self, question, conversation_history, mode):
        mode = self._normalize_mode(mode)
        evaluated_at = datetime.now(timezone.utc).date().isoformat()
        identity = {"policy": QUERY_CACHE_VERSION, "mode": mode, "question": question,
                    "history": conversation_history or [], "model": self._active_model(),
                    "strands_model": settings.strands_model or settings.gemini_model,
                    "generation": _REQUEST_GENERATION.get(), "evaluated_at": evaluated_at,
                    "source_date_order": settings.source_date_order}
        cache_key = hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        cached = await cache_get(query_cache, cache_key)
        if self._cacheable_answer(cached, mode):
            cached["cached"] = True
            return cached

        is_broad = mode != "quick" and self._is_broad_query(question)
        plan, trace = await self._build_query_plan(question, mode, conversation_history)
        plan["evaluated_at"] = evaluated_at
        plan["conversation_context"] = self._conversation_context(conversation_history)
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
        timeline_events, timeline_trace = await self._extract_timeline_events(question, all_context, sources, mode, broad=is_broad, evidence_pack=evidence_pack)
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
            evidence_pack=evidence_pack,
            timeline_events=timeline_events,
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
            "query_plan": {key: value for key, value in plan.items() if key != "conversation_context"},
            "trace": trace,
            "verification": verification,
            "evidence": evidence,
            "claim_ledger": claim_ledger,
            "evidence_pack": self._public_evidence_pack(evidence_pack),
            "current_state": verification["current_state"],
            "finalization": verification["finalization"],
            "timeline_events": timeline_events,
            "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
            "cached": False,
        }

        if await self._check_delivery_snapshot(result) and self._cacheable_answer(result, mode):
            await cache_set(query_cache, cache_key, result)
        return result

    @staticmethod
    def _cacheable_answer(result, mode):
        if not isinstance(result, dict):
            return False
        final = result.get("finalization")
        if not isinstance(final, dict):
            return False
        return (final.get("complete") is True and final.get("disposition") in {"supported", "qualified"}) or (
            mode == "quick" and final.get("disposition") == "unaudited")

    async def _check_delivery_snapshot(self, result: dict) -> bool:
        """One snapshot check for cached and newly audited public results."""
        source_ids = [i["document_id"] for i in result.get("evidence_pack", {}).get("items", [])
                      if type(i.get("document_id")) is int]
        incomplete = await embeddings_store.get_incomplete_document_ids(source_ids)
        # Check generation after the datastore read, which may itself overlap a
        # mutation. No await separates this last observation and the verdict.
        generation_changed = await get_corpus_generation_async() != _REQUEST_GENERATION.get()
        if generation_changed or incomplete:
            # A completed audit of a changing/partially replaced snapshot is not
            # a completed request. Rejected candidate text must not be published.
            result["answer"] = "The document index changed or is awaiting repair. Please retry after indexing finishes so the answer can be checked against a consistent set of sources."
            result["confidence"] = 0.0
            result["timeline_events"] = []
            result["sources"] = []
            result["claim_ledger"] = empty_ledger("")
            claim_summary = result["claim_ledger"]["summary"]
            missing = ["Stable completed source index required."]
            current = {**result.get("current_state", {}), "status": "needs_review",
                       "active_documented_interval_ids": [], "note": missing[0]}
            result["current_state"] = current
            for item in result.get("evidence_pack", {}).get("items", []):
                item["support_spans"] = []
            result["finalization"].update(disposition="corpus_changed", complete=False, answer_verified=False, cited_document_ids=[],
                answer_digest=hashlib.sha256(result["answer"].encode()).hexdigest())
            result["verification"].pop("partial", None)
            current.pop("comparison_scope", None)
            current.pop("comparison_document_ids", None)
            result["verification"].update(status="corpus_changed", missing_evidence=missing,
                supported_claims=[], unsupported_claims=[], stale_or_conflicting_claims=[],
                current_state=current, finalization=result["finalization"])
            result["evidence"].update(score=0.0, level="low", audit_status="corpus_changed",
                claim_summary=claim_summary, source_count=0, reasons=[], penalties=missing,
                dimensions={"claim_support": 0, "audit_coverage": 0},
                coverage={"answer_complete": False, "selected_span_count": 0, "available_span_count": 0})
            result["source_summary"].update(trust_score=0.0, trust_level="low",
                verification_status="corpus_changed", audit_status="corpus_changed",
                claim_summary=claim_summary, trust_reasons=[], trust_penalties=missing,
                trust_dimensions=result["evidence"]["dimensions"], current_state=current,
                source_count=0, timeline_event_count=0, unsupported_claim_count=0,
                stale_or_conflicting_claim_count=0)
            return False
        return True

    # ── Streaming query (SSE) ───────────────────────────────────────

    async def query_stream(self, question: str, conversation_history: list = None, model_override: str = None, mode: str = "strict"):
        """Progress may stream; factual prose is delivered only at the acceptance boundary."""
        yield {"type": "status", "message": "Retrieving source evidence and auditing the complete answer…"}
        task = asyncio.create_task(self.query(question, conversation_history, model_override, mode))
        try:
            while not task.done():
                done, _ = await asyncio.wait({task}, timeout=10)
                if not done:
                    yield {"type": "status", "message": "Source checks are still running…"}
            result = await task
            yield {"type": "complete", **result}
        finally:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

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
            cache_key = f"{_REQUEST_GENERATION.get()}:gs:{hashlib.md5(name.encode()).hexdigest()}"
            cached = await cache_get(graph_cache, cache_key)
            if cached is not None:
                results = cached
            else:
                results = await graph_store.search_nodes(name, limit=8)
                await cache_set(graph_cache, cache_key, results)
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
                sg_key = f"{_REQUEST_GENERATION.get()}:sg:{hashlib.md5(':'.join(sorted(all_uuids[:15])).encode()).hexdigest()}"
                cached = await cache_get(graph_cache, sg_key)
                if cached is not None:
                    subgraph = cached
                else:
                    subgraph = await graph_store.get_subgraph(all_uuids[:15], depth=3)
                    await cache_set(graph_cache, sg_key, subgraph)
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

    async def _retrieve_history(self, question: str, *, include_history=True, include_recent=False) -> dict:
        terms = subject_terms(question)
        try:
            candidates = await embeddings_store.historical_document_candidates(terms, MAX_CANDIDATES)
            try:
                dates = await graph_store.get_document_dates([row["document_id"] for row in candidates["documents"]])
            except Exception:
                dates = {}  # OCR/title periods still provide a deterministic fallback.
            candidates["documents"] = [{**row, "indexed_date": dates.get(row["document_id"])} for row in candidates["documents"]]
            coverage = {}
            chosen = choose_documents(candidates["documents"], question, date_order=settings.source_date_order, diagnostics=coverage) if include_history else []
            recent_coverage = {}
            recent = choose_recent_documents(candidates["documents"], question, date_order=settings.source_date_order, diagnostics=recent_coverage) if include_recent else []
            recent_ids = [row["document_id"] for row in recent]
            ids = [row["document_id"] for row in chosen]
            chunks = await embeddings_store.get_chunks_for_documents(list(dict.fromkeys(ids + recent_ids)), chunks_per_doc=3, relevance_terms=terms, include_opening=True)
            # Reservation metadata survives duplicate chunk merging separately.
            return {"vector_results": chunks, "history_chunks": [c for c in chunks if c["document_id"] in ids], "history_document_ids": ids,
                    "recent_chunks": [c for c in chunks if c["document_id"] in recent_ids], "recent_document_ids": recent_ids,
                    "recent_coverage": {**recent_coverage, "candidate_limit": MAX_CANDIDATES, "document_limit": MAX_DOCUMENTS,
                        "reserved_document_ids": recent_ids, "truncated": candidates["truncated"] or len(candidates["documents"]) > len(recent_ids),
                        "retrieval_is_exhaustive": False},
                    "history_coverage": {**coverage, "candidate_count": candidates["candidate_count"],
                        "candidate_limit": MAX_CANDIDATES, "document_limit": MAX_DOCUMENTS,
                        "reserved_document_ids": ids, "truncated": candidates["truncated"] or len(candidates["documents"]) > len(ids),
                        "retrieval_is_exhaustive": False}}
        except Exception as exc:
            logger.warning("Historical index discovery unavailable: %s", type(exc).__name__)
            return {"history_document_ids": [], "history_coverage": {"status": "unavailable", "truncated": True,
                                                                     "retrieval_is_exhaustive": False}}

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

        history_context = self._conversation_context(conversation_history)
        conv_context = f"\n\nPrevious conversation context:\n{history_context}\n" if history_context else ""

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

        history_context = self._conversation_context(conversation_history)
        conv_section = (
            f"\n\nPrevious conversation:\n{history_context}\n\n"
            "Use the conversation above to understand context for follow-up questions.\n"
        ) if history_context else ""

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
- Answer the direct question first and stay within its scope. For a request to identify or list records, give the identifying names and documented terms needed to distinguish them; do not expand into every coverage, amount, administrative field, or historical record unless asked.
- Every precise fact should be traceable to a specific source document/excerpt. If exact evidence is missing, say that instead of guessing.
- Distinguish document dates, generation dates, statement periods, service/specimen dates, effective dates, and expiration dates.
- For questions about identity ("who am I"), cover ALL life domains: personal info, military service, education, medical/health, disability status, financial overview, property, family, employment, vehicles, pets — whatever the documents reveal.
- For changing ratings, statuses and balances, report the value documented by each relevant dated source. A more recent source can provide a newer observation without proving an exhaustive or currently active state.
- For "latest/current/last" answers, state the newest source-backed value found in the evidence. Do not claim that a newer document does not contain the requested fact unless the evidence explicitly shows that absence; use retrieval-limited phrasing when needed.

TEMPORAL AWARENESS — CRITICAL:
- A document date or effective period establishes what that record states, not whether the real-world policy or obligation remains active. Report documented terms with their dates.
- If a documented policy term ended before the evaluation date, say that the recorded term ended. Do not infer cancellation, renewal or replacement from dates alone.
- Distinguish an ended recorded contract term from evidence of cancellation, renewal or continuing service.
- When payment amounts change, give the amount and effective date explicitly documented by the relevant notice.
- For addresses: distinguish between current residence and previous addresses. Do not list bills from a previous address as current obligations unless there's evidence of ongoing service.
- Overlapping dates do not prove that one policy replaced another. Only label a policy cancelled or superseded when the source explicitly supports that relationship.
- Do not make negative absence claims (for example, "document X has no newer result") unless the source context explicitly proves that absence. Prefer "I did not find a newer source-backed value in the retrieved evidence."
- Evaluation date (UTC): {plan.get("evaluated_at")}. A recent document date does not establish current status.
- For a current-status question without explicit resolution evidence, answer with dated document observations only. Do not add a generic current-status disclaimer to the candidate; the final acceptance layer appends its own limitation to accepted documented facts.
- Keep every changing-state statement tied to its source and recorded date or term. Identify the relevant subject with only the fields needed to distinguish it. Detached value or status labels can imply current validity even when a separate sentence is dated.
- For a history question, compare the earliest relevant records, material intermediate changes, and latest documented observations for each relevant subject. Reserved historical sources are context opportunities, not proof of facts or archive completeness. Use their original text; report a coverage gap if the requested history cannot be established.
- Write facts without inline citations or links; the source audit attaches authoritative citations after validation.
- Include the specific dates, amounts, percentages, names, terms, identifiers, and statuses needed to answer the question. Do not include unrelated precise details just because they are source-backed.
- Format monetary values ($1,234.56), dates (January 15, 2024), and percentages (100%) clearly
- If relevant records conflict, describe the dated observations and the unresolved conflict. Do not choose an active record or invent supersession from chronology alone.
- Reference knowledge graph relationships when they add context
- Structure complex answers with unnumbered headings and bullet points. Do not add numeric section labels; reserve numbers for source-backed facts.
- Describe a material limitation as a limitation of the retrieved evidence; do not claim that an item is absent from the entire archive.
- When multiple documents corroborate the same fact, preserve its meaning; the source audit supplies the validated references.

	Question: {question}
	{conv_section}{draft_section}{plan_section}{timeline_section}{evidence_section}
	Document context (from {len(doc_context.split(chr(10)+chr(10)))} retrieval passes):
	{doc_context}
	{graph_text}

	Provide a focused answer grounded in the supplied source text:"""

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
        for scope in ("history", "recent"):
            merged[f"{scope}_document_ids"] = list(dict.fromkeys(ctx1.get(f"{scope}_document_ids", []) + ctx2.get(f"{scope}_document_ids", [])))[:MAX_DOCUMENTS]
            merged[f"{scope}_coverage"] = ctx1.get(f"{scope}_coverage") or ctx2.get(f"{scope}_coverage") or {}
            chunks = {(chunk["document_id"], chunk.get("chunk_index", 0)): chunk
                      for chunk in ctx1.get(f"{scope}_chunks", []) + ctx2.get(f"{scope}_chunks", [])}
            merged[f"{scope}_chunks"] = list(chunks.values())[:MAX_DOCUMENTS * 3]
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
        full_doc_chunks = []
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
        # Expansion can contribute many chunks from one document. Preserve a
        # diverse base again before applying the explicit source reservations.
        selected = self._diversify_chunks(selected, limit=90 if broad or high_accuracy else 60,
                                          max_per_doc=4 if high_accuracy else 3)
        reserved = {}
        for scope in ("recent", "history"):
            for doc_id in context.get(f"{scope}_document_ids", []):
                choices = [chunk for chunk in (context.get(f"{scope}_chunks") or combined)
                           if chunk.get("document_id") == doc_id and certifying_text(chunk)]
                for chunk in sorted(choices, key=lambda chunk: chunk.get("chunk_index", 0))[:3]:
                    key = (doc_id, chunk.get("chunk_index", 0))
                    reserved.setdefault(key, dict(chunk))[f"{scope}_reserved"] = True
        selected = list(reserved.values()) + selected

        pack = build_evidence_pack(
            question=question,
            plan=plan,
            chunks=selected,
            sources=sources,
            max_items=90 if broad or high_accuracy else 60,
        )
        # Full OCR is already available from bounded source expansion. Keep it
        # private and bind every retained chunk of those documents to its context.
        document_contexts = {c['document_id']: c['_source_document_content'] for c in full_doc_chunks
                             if isinstance(c.get('_source_document_content'), str)}
        for item in pack['items']:
            if item['document_id'] in document_contexts:
                bind_document_context(item, document_contexts[item['document_id']])
        pack["coverage"]["history"] = context.get("history_coverage", {})
        pack["coverage"]["recent"] = context.get("recent_coverage", {})
        flagged = await embeddings_store.get_open_feedback_document_ids(
            list({item["document_id"] for item in pack["items"] if type(item.get("document_id")) is int}))
        for item in pack["items"]:
            item["feedback_open"] = item.get("document_id") in flagged
        return pack

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

            chunks = chunk_text(content, chunk_size=3600, overlap=500)
            ranked_chunks = self._rank_full_document_chunks(question, chunks)
            for rank, text in ranked_chunks[:10]:
                expanded.append({
                    "document_id": int(doc_id),
                    "chunk_index": 100000 + rank,
                    "content": text,
                    "source_kind": "ocr",
                    "source_content": text,
                    "title": title,
                    "date": date,
                    "doc_type": doc_type,
                    "similarity": 0.78,
                    "rank_score": 1.0,
                    "_source": "paperless_full_document",
                    "_source_document_content": content,
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
        current = verification.get("current_state") or current_state_summary(plan, sources)
        return {
            "latest_source_date": latest_supporting_date or latest_retrieved_date,
            "latest_retrieved_source_date": latest_retrieved_date,
            "latest_supporting_source_date": latest_supporting_date,
            "latest_check_used": latest_check_used,
            "source_count": len(sources),
            "newer_docs_may_exist": True,
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
        parsed = parse_date(value)
        if parsed:
            return parsed[0]
        for pattern in ("%m/%d/%Y", "%B %d, %Y"):
            try:
                return datetime.strptime(value, pattern).date().isoformat()
            except ValueError:
                continue
        return None

    def _public_evidence_pack(self, evidence_pack: dict | None) -> dict:
        """Return UI-safe evidence metadata without sending full chunk bodies."""
        evidence_pack = evidence_pack or {}
        items = []
        for item in (evidence_pack.get("items") or []):
            items.append({
                "id": item.get("id"),
                "document_id": item.get("document_id"),
                "chunk_index": item.get("chunk_index"),
                "title": item.get("title"),
                "doc_type": item.get("doc_type"),
                "source_kind": item.get("source_kind"),
                "source_context": item.get("source_context"),
                "history_reserved": item.get("history_reserved") is True,
                "recent_reserved": item.get("recent_reserved") is True,
                "source_quality": item.get("source_quality"),
                "date_signals": item.get("date_signals"),
                "structured_fact_count": item.get("structured_fact_count"),
                "exact_term_hits": item.get("exact_term_hits"),
                "excerpt": item.get("excerpt"),
                "support_spans": item.get("support_spans", []),
                "feedback_open": item.get("feedback_open", False),
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
        cache_key = f"{_REQUEST_GENERATION.get()}:vs:{hashlib.md5(query.encode()).hexdigest()}:{limit}"
        cached = await cache_get(vector_cache, cache_key)
        if cached is not None:
            return cached
        results = await embeddings_store.vector_search(query, limit=limit)
        await cache_set(vector_cache, cache_key, results)
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
