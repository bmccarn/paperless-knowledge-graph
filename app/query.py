import json
import logging
import hashlib
import re
import asyncio
from datetime import datetime, timezone
from collections import defaultdict

from openai import AsyncOpenAI

from app.config import settings

def _owner_name():
    return settings.owner_name or "the document owner"

def _owner_context():
    return settings.owner_context or ""
from app.retry import retry_with_backoff
from app.embeddings import embeddings_store
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
from app.strands_orchestrator import strands_orchestrator

logger = logging.getLogger(__name__)


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
        response = await self.client.chat.completions.create(
            model=self._active_model(),
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content

    async def _llm_json(self, prompt: str) -> any:
        response = await self.client.chat.completions.create(
            model=self._active_model(),
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)

    async def _llm_generate_stream(self, prompt: str):
        """Yield answer chunks via streaming."""
        stream = await self.client.chat.completions.create(
            model=self._active_model(),
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        )
        async for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

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
        queries = retrieval_queries(plan, max_queries=1 if mode == "quick" else 6)
        latest_check_used = any(q.get("role") == "current_state" for q in queries)

        async def _retrieve_one(item: dict) -> tuple[dict, dict]:
            ctx = await self._retrieve(item["query"])
            return item, ctx

        if mode == "quick":
            item = queries[0] if queries else {"role": "primary", "query": question}
            all_context = await self._retrieve(item["query"])
            trace.append(trace_step("retrieval", "ok", "Single-pass hybrid retrieval completed", {"queries": [item]}))
            return all_context, [], latest_check_used, trace

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
        return all_context, [q["query"] for q in queries[1:]], latest_check_used, trace

    async def _gap_review(self, question: str, context: dict, conversation_history: list, mode: str) -> tuple[dict, dict, list[str], list[dict]]:
        trace = []
        if normalize_mode(mode) == "quick":
            entities = context.get("entity_names", [])
            return {"draft_answer": "", "confidence": 0.55, "entities_found": entities, "follow_up_suggestions": []}, context, [], trace

        first_pass = await self._synthesize_with_gaps(question, context, conversation_history)
        follow_ups_used = []
        for follow_up in first_pass.get("follow_up_queries", [])[:2]:
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
            trace.append(trace_step("gap_review", "ok", "No additional gap follow-up retrieval was needed"))
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

    async def _extract_timeline_events(self, question: str, context: dict, sources: list[dict], mode: str) -> tuple[list[dict], list[dict]]:
        if normalize_mode(mode) != "timeline":
            return [], []
        doc_context = self._format_doc_context(context, question=question)
        graph_text = self._format_graph_context(context)
        events = await strands_orchestrator.extract_timeline(question, f"{doc_context}\n{graph_text}")
        if events:
            events = sort_timeline_events(events)
            return events[:30], [trace_step("timeline", "ok", f"Extracted and sorted {len(events)} timeline events with Strands")]
        fallback = timeline_fallback_events(sources)
        return fallback, [trace_step("timeline", "fallback", f"Used source-date fallback for {len(fallback)} timeline events")]

    async def _verify_and_grade(
        self,
        question: str,
        answer: str,
        context: dict,
        sources: list[dict],
        plan: dict,
        mode: str,
    ) -> tuple[dict, dict, list[dict]]:
        verification = None
        trace = []
        if normalize_mode(mode) != "quick":
            verification = await strands_orchestrator.verify_answer(
                question,
                answer,
                sources,
                self._format_doc_context(context, question=question),
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
            else:
                verification = {
                    "status": "not_run",
                    "supported_claims": [],
                    "unsupported_claims": [],
                    "stale_or_conflicting_claims": [],
                    "missing_evidence": [],
                    "notes": ["Verifier unavailable; confidence is computed from retrieval evidence only."],
                    "confidence_adjustment": 0,
                }
                trace.append(trace_step("verifier", "fallback", "Verifier unavailable; used computed evidence score only"))

        evidence = compute_evidence_grade(question, plan, sources, context, verification)
        trace.append(trace_step(
            "evidence_grade",
            "ok",
            f"{evidence['level']} trust score ({evidence['score']:.2f})",
            {"reasons": evidence.get("reasons", []), "penalties": evidence.get("penalties", [])},
        ))
        return verification or {}, evidence, trace

    def _blend_confidence(self, llm_confidence: float, evidence: dict, verification: dict) -> float:
        evidence_score = float(evidence.get("score", 0.5))
        adjustment = 0.0
        try:
            adjustment = float(verification.get("confidence_adjustment") or 0)
        except Exception:
            adjustment = 0.0
        blended = (0.35 * float(llm_confidence or 0.5)) + (0.65 * evidence_score) + adjustment
        return round(max(0.0, min(1.0, blended)), 3)

    # ── Main query (non-streaming, backward compat) ─────────────────

    async def query(self, question: str, conversation_history: list = None, model_override: str = None, mode: str = "deep") -> dict:
        """Answer a question using mode-specific retrieval + synthesis."""
        mode = self._normalize_mode(mode)
        self._model_override = model_override
        # Include conversation context in cache key for uniqueness
        conv_suffix = ""
        if conversation_history:
            conv_text = " ".join(m.get("content", "")[:50] for m in conversation_history[-4:])
            conv_suffix = hashlib.md5(conv_text.encode()).hexdigest()[:8]
        cache_key = normalize_query_key(f"{mode}:{question}{conv_suffix}")
        cached = query_cache.get(cache_key)
        if cached is not None:
            cached["cached"] = True
            return cached

        plan, trace = await self._build_query_plan(question, mode, conversation_history)
        all_context, planned_queries_used, latest_check_used, retrieval_trace = await self._execute_retrieval_plan(question, plan, mode)
        trace.extend(retrieval_trace)

        first_pass, all_context, gap_follow_ups, gap_trace = await self._gap_review(question, all_context, conversation_history, mode)
        trace.extend(gap_trace)

        entities_found = first_pass.get("entities_found", []) or all_context.get("entity_names", [])
        all_context, graph_trace = await self._expand_planned_graph(all_context, entities_found)
        trace.extend(graph_trace)

        sources = self._build_sources(all_context, question=question)
        timeline_events, timeline_trace = await self._extract_timeline_events(question, all_context, sources, mode)
        trace.extend(timeline_trace)
        final = await self._final_synthesis(
            question,
            all_context,
            first_pass.get("draft_answer", ""),
            conversation_history,
            mode=mode,
            plan=plan,
            timeline_events=timeline_events,
        )
        verification, evidence, verify_trace = await self._verify_and_grade(
            question,
            final.get("answer", ""),
            all_context,
            sources,
            plan,
            mode,
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
        )

        result = {
            "question": question,
            "answer": final.get("answer", ""),
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
        cache_key = normalize_query_key(f"{mode}:{question}{conv_suffix}")
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
                   "current_state": cached.get("current_state", {}),
                   "timeline_events": cached.get("timeline_events", []),
                   "cached": True}
            return

        yield {"type": "status", "message": "Planning query workflow..."}
        plan, trace = await self._build_query_plan(question, mode, conversation_history)
        yield {"type": "trace", "step": trace[-1]}

        strategy_label = "single-pass search" if mode == "quick" else "parallel planned searches"
        yield {"type": "status", "message": f"Running {strategy_label}..."}
        all_context, planned_queries_used, latest_check_used, retrieval_trace = await self._execute_retrieval_plan(question, plan, mode)
        trace.extend(retrieval_trace)
        for step in retrieval_trace:
            yield {"type": "trace", "step": step}

        yield {"type": "status", "message": "Reviewing evidence gaps..."}
        first_pass, all_context, gap_follow_ups, gap_trace = await self._gap_review(question, all_context, conversation_history, mode)
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
        if mode == "timeline":
            yield {"type": "status", "message": "Extracting and sorting timeline events..."}
        timeline_events, timeline_trace = await self._extract_timeline_events(question, all_context, sources, mode)
        trace.extend(timeline_trace)
        for step in timeline_trace:
            yield {"type": "trace", "step": step}

        yield {"type": "status", "message": "Synthesizing answer from ranked evidence..."}

        prompt = self._build_final_prompt(
            question,
            mode,
            self._format_doc_context(all_context, question=question),
            self._format_graph_context(all_context),
            first_pass.get("draft_answer", ""),
            conversation_history,
            plan=plan,
            timeline_events=timeline_events,
        )

        answer_chunks = []
        async for chunk in self._llm_generate_stream(prompt):
            answer_chunks.append(chunk)
            yield {"type": "answer_chunk", "content": chunk}

        full_answer = "".join(answer_chunks)

        yield {"type": "status", "message": "Verifying source support and trust score..."}
        verification, evidence, verify_trace = await self._verify_and_grade(
            question,
            full_answer,
            all_context,
            sources,
            plan,
            mode,
        )
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
               "current_state": result["current_state"],
               "timeline_events": timeline_events,
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
1. A draft answer — be specific, cite document titles. Include ALL relevant details you can find. If this is a follow-up question, use the conversation context to understand what "it", "that", "more details", etc. refer to.
2. A confidence score (0-1) for completeness.
3. What information is MISSING or could be more complete? Generate exactly 3 targeted follow-up SEARCH queries to fill gaps:
   - Are there MORE RECENT documents that might update/supersede what you found?
   - Are there related topics not yet covered?
   - Are there specific terms, dates, or reference numbers you could search for?
4. List ALL entity names (people, organizations, places, conditions, etc.) mentioned.
5. Suggest 3-4 natural follow-up questions the user might want to ask next, based on what you found. Make them specific and interesting, not generic.

CRITICAL: When dealing with ratings, statuses, or values that change over time, ALWAYS note you need to find the MOST RECENT/FINAL version. Generate a follow-up query specifically for "most recent" or "latest" or "final" version.

Important: The user is {_owner_name()}. "my" or "I" = {_owner_name()}.

Respond in JSON: {{"draft_answer": "...", "confidence": 0.8, "follow_up_queries": ["search query 1", "search query 2", "search query 3"], "entities_found": ["Name1", "Name2"], "follow_up_suggestions": ["What is my current VA disability rating?", "Tell me about my military deployments"]}}"""

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

        return f"""You are a knowledge assistant with access to {_owner_name()}'s personal document archive and knowledge graph. You have been given context from multiple retrieval passes across hundreds of personal documents.

CONTEXT ABOUT THE USER:
- The user is {_owner_name()}
- Documents include: medical records, VA disability ratings, military service records, financial documents, mortgage statements, legal contracts, vehicle records, pet/veterinary records, insurance policies, tax documents, employment records, and more
- When the user says "my", "I", "me" — they mean {_owner_name()}
- {("Additional context: " + _owner_context()) if _owner_context() else ""}

INSTRUCTIONS:
- Query mode: {mode}. {mode_instruction}
- Be EXHAUSTIVE — use every piece of relevant information from the context. Do not summarize away details.
- For questions about identity ("who am I"), cover ALL life domains: personal info, military service, education, medical/health, disability status, financial overview, property, family, employment, vehicles, pets — whatever the documents reveal.
- For ratings/statuses that change over time (VA disability, credit scores, balances, etc.), always identify and clearly state the MOST RECENT / FINAL / CURRENT value. If multiple values exist across documents, show the progression chronologically and highlight the latest.
- Cite sources using document TITLES: (Source: "Document Title")
- If no title available, use: (Document 305)
- Include ALL specific details: dates, amounts, percentages, names, medical terms, account numbers
- Format monetary values ($1,234.56), dates (January 15, 2024), and percentages (100%) clearly
- If information conflicts between documents, note BOTH and explain which is likely more current
- Reference knowledge graph relationships when they add context
- Structure complex answers with clear headers and bullet points
- State what you could NOT find or what's missing from the archive
- When multiple documents corroborate the same fact, cite all of them for completeness

	Question: {question}
	{conv_section}{draft_section}{plan_section}{timeline_section}
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
        plan: dict | None = None,
        timeline_events: list[dict] | None = None,
    ) -> dict:
        doc_context = self._format_doc_context(context, question=question)
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

        for key in ("vector_results", "keyword_results"):
            seen = set()
            combined = []
            for r in ctx1.get(key, []) + ctx2.get(key, []):
                k = (r.get("document_id"), r.get("chunk_index", 0))
                if k not in seen:
                    seen.add(k)
                    combined.append(r)
            merged[key] = combined

        for key in ("entity_results", "entity_kw_results"):
            seen = set()
            combined = []
            for r in ctx1.get(key, []) + ctx2.get(key, []):
                k = r.get("entity_uuid", id(r))
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

        merged["entity_names"] = list(set(
            ctx1.get("entity_names", []) + ctx2.get("entity_names", [])
        ))
        return merged

    # ── Formatting (TUNED: more context to LLM) ────────────────────

    def _format_doc_context(self, context: dict, question: str = "") -> str:
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", []),
            question=question,
        )
        # Feed top 20 chunks to LLM (was 12)
        top = combined[:20]

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
    ) -> dict:
        sources = self._build_sources(context, question=question)
        dates = [s["date"] for s in sources if s.get("date")]
        latest_date = max(dates) if dates else None
        plan = plan or {}
        evidence = evidence or {}
        verification = verification or {}
        current = current_state_summary(plan, sources)
        return {
            "latest_source_date": latest_date,
            "latest_check_used": latest_check_used,
            "source_count": len(sources),
            "newer_docs_may_exist": not latest_check_used,
            "trust_score": evidence.get("score"),
            "trust_level": evidence.get("level"),
            "trust_reasons": evidence.get("reasons", []),
            "trust_penalties": evidence.get("penalties", []),
            "verification_status": verification.get("status"),
            "unsupported_claim_count": len(verification.get("unsupported_claims") or []),
            "stale_or_conflicting_claim_count": len(verification.get("stale_or_conflicting_claims") or []),
            "current_state": current,
            "timeline_event_count": len(timeline_events or []),
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

    def _merge_and_rank(self, vector_results: list[dict], keyword_results: list[dict], question: str = "") -> list[dict]:
        scored = {}
        for r in vector_results:
            key = (r["document_id"], r.get("chunk_index", 0))
            if key not in scored:
                scored[key] = {**r, "vector_score": float(r.get("similarity", 0)), "keyword_score": 0.0}
            else:
                scored[key]["vector_score"] = max(scored[key].get("vector_score", 0), float(r.get("similarity", 0)))

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
        doc_type = (result.get("doc_type") or "").lower()
        content = (result.get("content") or "").lower()

        score = base + self._doc_type_boost(query, doc_type) + self._recency_boost(result)
        if self._looks_superseded(content):
            score -= 0.18
        return score

    def _doc_type_boost(self, query: str, doc_type: str) -> float:
        mappings = [
            (("insurance", "policy", "coverage", "premium"), ("insurance", "policy"), 0.12),
            (("tax", "irs", "return", "w-2", "1099"), ("tax", "financial"), 0.12),
            (("medical", "doctor", "diagnosis", "medication", "lab"), ("medical",), 0.12),
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
