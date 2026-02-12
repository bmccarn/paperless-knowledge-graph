import json
import logging
import hashlib
from collections import defaultdict

from openai import AsyncOpenAI

from app.config import settings
from app.retry import retry_with_backoff
from app.embeddings import embeddings_store
from app.graph import graph_store
from app.cache import query_cache, vector_cache, graph_cache, normalize_query_key

logger = logging.getLogger(__name__)


class QueryEngine:
    def __init__(self):
        self.client = AsyncOpenAI(
            base_url=settings.litellm_url,
            api_key=settings.litellm_api_key,
        )
        self.model = settings.gemini_model

    async def _llm_generate(self, prompt: str) -> str:
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content

    async def _llm_json(self, prompt: str) -> any:
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)

    async def _llm_generate_stream(self, prompt: str):
        """Yield answer chunks via streaming."""
        stream = await self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        )
        async for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content

    # ── Main query (non-streaming, backward compat) ─────────────────

    async def query(self, question: str, conversation_history: list = None) -> dict:
        """Answer a question using iterative retrieval + synthesis."""
        # Include conversation context in cache key for uniqueness
        conv_suffix = ""
        if conversation_history:
            conv_text = " ".join(m.get("content", "")[:50] for m in conversation_history[-4:])
            conv_suffix = hashlib.md5(conv_text.encode()).hexdigest()[:8]
        cache_key = normalize_query_key(question + conv_suffix)
        cached = query_cache.get(cache_key)
        if cached is not None:
            cached["cached"] = True
            return cached

        initial_context = await self._retrieve(question)
        first_pass = await self._synthesize_with_gaps(question, initial_context, conversation_history)
        all_context = initial_context

        # Up to 3 follow-up iterations for deeper retrieval
        follow_ups_used = []
        for follow_up in first_pass.get("follow_up_queries", [])[:3]:
            extra_context = await self._retrieve(follow_up)
            all_context = self._merge_context(all_context, extra_context)
            follow_ups_used.append(follow_up)

        entities_found = first_pass.get("entities_found", [])
        graph_context = await self._expand_graph(entities_found)
        all_context = self._merge_context(all_context, graph_context)

        final = await self._final_synthesis(
            question, all_context, first_pass.get("draft_answer", ""), conversation_history
        )

        result = {
            "question": question,
            "answer": final.get("answer", ""),
            "confidence": final.get("confidence", first_pass.get("confidence", 0.5)),
            "sources": self._build_sources(all_context),
            "entities_found": [
                {"name": e} if isinstance(e, str) else e
                for e in entities_found[:30]
            ],
            "graph_nodes_used": len(all_context.get("graph_nodes", [])),
            "follow_up_queries_used": follow_ups_used,
            "iterations": 1 + len(follow_ups_used),
            "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
            "cached": False,
        }

        query_cache.set(cache_key, result)
        return result

    # ── Streaming query (SSE) ───────────────────────────────────────

    async def query_stream(self, question: str, conversation_history: list = None):
        """Stream query response via SSE events."""
        conv_suffix = ""
        if conversation_history:
            conv_text = " ".join(m.get("content", "")[:50] for m in conversation_history[-4:])
            conv_suffix = hashlib.md5(conv_text.encode()).hexdigest()[:8]
        cache_key = normalize_query_key(question + conv_suffix)
        cached = query_cache.get(cache_key)
        if cached is not None:
            yield {"type": "answer_chunk", "content": cached["answer"]}
            yield {"type": "complete", "sources": cached["sources"],
                   "entities_found": cached.get("entities_found", []),
                   "confidence": cached.get("confidence", 0.7),
                   "follow_up_suggestions": cached.get("follow_up_suggestions", []),
                   "cached": True}
            return

        yield {"type": "status", "message": "Searching documents and knowledge graph..."}
        initial_context = await self._retrieve(question)

        yield {"type": "status", "message": "Analyzing results and identifying gaps..."}
        first_pass = await self._synthesize_with_gaps(question, initial_context, conversation_history)
        all_context = initial_context

        # Up to 3 follow-up iterations
        follow_ups_used = []
        for i, follow_up in enumerate(first_pass.get("follow_up_queries", [])[:3]):
            yield {"type": "status", "message": f"Deep dive {i+1}/3: {follow_up[:60]}..."}
            extra_context = await self._retrieve(follow_up)
            all_context = self._merge_context(all_context, extra_context)
            follow_ups_used.append(follow_up)

        entities_found = first_pass.get("entities_found", [])
        if entities_found:
            yield {"type": "status", "message": f"Expanding knowledge graph ({len(entities_found)} entities)..."}
            graph_context = await self._expand_graph(entities_found)
            all_context = self._merge_context(all_context, graph_context)

        yield {"type": "status", "message": "Synthesizing comprehensive answer..."}

        prompt = self._build_final_prompt(
            question,
            self._format_doc_context(all_context),
            self._format_graph_context(all_context),
            first_pass.get("draft_answer", ""),
            conversation_history,
        )

        answer_chunks = []
        async for chunk in self._llm_generate_stream(prompt):
            answer_chunks.append(chunk)
            yield {"type": "answer_chunk", "content": chunk}

        full_answer = "".join(answer_chunks)
        sources = self._build_sources(all_context)

        result = {
            "question": question,
            "answer": full_answer,
            "confidence": first_pass.get("confidence", 0.7),
            "sources": sources,
            "entities_found": [
                {"name": e} if isinstance(e, str) else e
                for e in entities_found[:30]
            ],
            "graph_nodes_used": len(all_context.get("graph_nodes", [])),
            "follow_up_queries_used": follow_ups_used,
            "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
            "iterations": 1 + len(follow_ups_used),
            "cached": False,
        }
        query_cache.set(cache_key, result)

        yield {"type": "complete", "sources": sources,
               "entities_found": result["entities_found"],
               "confidence": result.get("confidence", 0.7),
               "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
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
        doc_context = self._format_doc_context(context)
        graph_text = self._format_graph_context(context)

        conv_context = ""
        if conversation_history:
            conv_lines = []
            for msg in conversation_history[-6:]:  # Last 6 messages (3 Q&A pairs)
                role = "User" if msg.get("role") == "user" else "Assistant"
                conv_lines.append(f"{role}: {msg['content'][:500]}")
            newline = "\n"
            conv_context = f"""\n\nPrevious conversation context:\n{newline.join(conv_lines)}\n"""

        prompt = f"""You are a knowledge assistant analyzing personal documents belonging to John Doe.
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

Important: The user is John Doe. "my" or "I" = John Doe.

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

    def _build_final_prompt(self, question: str, doc_context: str, graph_text: str, draft_answer: str, conversation_history: list = None) -> str:
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

        return f"""You are a knowledge assistant with access to John Doe's personal document archive and knowledge graph. You have been given context from multiple retrieval passes across hundreds of personal documents.

CONTEXT ABOUT THE USER:
- The user is John Doe (DOB: February 14, 1996)
- Documents include: medical records, VA disability ratings, military service records (USAF), financial documents, mortgage statements, legal contracts, vehicle records, pet/veterinary records, insurance policies, tax documents, employment records, and more
- When the user says "my", "I", "me" — they mean John Doe
- Additional owner context is provided via environment variables

INSTRUCTIONS:
- Be EXHAUSTIVE — use every piece of relevant information from the context. Do not summarize away details.
- For questions about identity ("who am I"), cover ALL life domains: personal info, military service, education, medical/health, disability status, financial overview, property, family, employment, vehicles, pets — whatever the documents reveal.
- For ratings/statuses that change over time (VA disability, credit scores, balances, etc.), always identify and clearly state the MOST RECENT / FINAL / CURRENT value. If multiple values exist across documents, show the progression chronologically and highlight the latest.
- Cite sources using document TITLES: (Source: "Medical Record for John Doe")
- If no title available, use: (Document 305)
- Include ALL specific details: dates, amounts, percentages, names, medical terms, account numbers
- Format monetary values ($1,234.56), dates (January 15, 2024), and percentages (100%) clearly
- If information conflicts between documents, note BOTH and explain which is likely more current
- Reference knowledge graph relationships when they add context
- Structure complex answers with clear headers and bullet points
- State what you could NOT find or what's missing from the archive
- When multiple documents corroborate the same fact, cite all of them for completeness

Question: {question}
{conv_section}{draft_section}
Document context (from {len(doc_context.split(chr(10)+chr(10)))} retrieval passes):
{doc_context}
{graph_text}

Provide your comprehensive, exhaustive answer with inline citations:"""

    async def _final_synthesis(self, question: str, context: dict, draft_answer: str, conversation_history: list = None) -> dict:
        doc_context = self._format_doc_context(context)
        graph_text = self._format_graph_context(context)
        prompt = self._build_final_prompt(question, doc_context, graph_text, draft_answer, conversation_history)

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

    def _format_doc_context(self, context: dict) -> str:
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", [])
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

    def _build_sources(self, context: dict) -> list[dict]:
        """Build deduplicated source citations grouped by document."""
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", [])
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
            if len(chunks) > 1:
                source["excerpt_count"] = len(chunks)
            source["excerpt"] = best.get("content", "")[:300]
            sources.append(source)

        sources.sort(key=lambda x: x["similarity"], reverse=True)
        return sources[:15]  # Return up to 15 sources (was 10)

    # ── Existing helpers ────────────────────────────────────────────

    async def _cached_vector_search(self, query: str, limit: int = 20) -> list[dict]:
        cache_key = f"vs:{hashlib.md5(query.encode()).hexdigest()}:{limit}"
        cached = vector_cache.get(cache_key)
        if cached is not None:
            return cached
        results = await embeddings_store.vector_search(query, limit=limit)
        vector_cache.set(cache_key, results)
        return results

    def _merge_and_rank(self, vector_results: list[dict], keyword_results: list[dict]) -> list[dict]:
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

        return sorted(scored.values(), key=lambda x: x["combined_score"], reverse=True)

    async def _extract_entities_from_query(self, question: str) -> list[str]:
        try:
            prompt = f"""Extract person names, organization names, and key concepts/topics from this question.
Return a JSON object with an "entities" key containing an array of strings — just the names and key terms, nothing else.
If there are no specific entities, return the 2-3 most important search terms.
For broad questions like "who am I" or "tell me about myself", return: ["John Doe", "John Doe"]

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
