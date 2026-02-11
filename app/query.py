import json
import logging
import hashlib

from openai import AsyncOpenAI

from app.config import settings
from app.retry import retry_with_backoff
from app.embeddings import embeddings_store
from app.graph import graph_store
from app.cache import query_cache, vector_cache, graph_cache

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

    # ── Main query entry point (iterative multi-step) ───────────────

    async def query(self, question: str) -> dict:
        """Answer a question using iterative retrieval + synthesis."""
        # Check cache
        cache_key = f"q:{hashlib.md5(question.encode()).hexdigest()}"
        cached = query_cache.get(cache_key)
        if cached is not None:
            cached["cached"] = True
            return cached

        # Step 1: Initial retrieval
        initial_context = await self._retrieve(question)

        # Step 2: First synthesis + gap analysis
        first_pass = await self._synthesize_with_gaps(question, initial_context)

        all_context = initial_context

        # Step 3: Follow-up iterations (max 2)
        follow_ups_used = []
        for follow_up in first_pass.get("follow_up_queries", [])[:2]:
            extra_context = await self._retrieve(follow_up)
            all_context = self._merge_context(all_context, extra_context)
            follow_ups_used.append(follow_up)

        # Graph expansion from found entities
        entities_found = first_pass.get("entities_found", [])
        graph_context = await self._expand_graph(entities_found)
        all_context = self._merge_context(all_context, graph_context)

        # Step 4: Final synthesis
        final = await self._final_synthesis(
            question, all_context, first_pass.get("draft_answer", "")
        )

        result = {
            "question": question,
            "answer": final.get("answer", ""),
            "confidence": final.get("confidence", first_pass.get("confidence", 0.5)),
            "sources": self._build_sources(all_context),
            "entities_found": [
                {"name": e} if isinstance(e, str) else e
                for e in entities_found[:20]
            ],
            "graph_nodes_used": len(all_context.get("graph_nodes", [])),
            "follow_up_queries_used": follow_ups_used,
            "iterations": 1 + len(follow_ups_used),
            "cached": False,
        }

        query_cache.set(cache_key, result)
        return result

    # ── Retrieval ───────────────────────────────────────────────────

    async def _retrieve(self, query_text: str) -> dict:
        """Hybrid retrieval: vector + keyword + entity search + graph."""
        # Vector search (documents)
        vector_results = await self._cached_vector_search(query_text, limit=10)

        # Keyword search (documents)
        keyword_results = await embeddings_store.keyword_search(query_text, limit=8)

        # Entity vector search
        entity_results = await embeddings_store.entity_vector_search(query_text, limit=5)

        # Entity keyword search
        entity_kw_results = await embeddings_store.entity_keyword_search(query_text, limit=5)

        # LLM entity extraction from query
        entity_names = await self._extract_entities_from_query(query_text)

        # Graph search for named entities
        graph_nodes = []
        seen_uuids = set()
        for name in entity_names:
            cache_key = f"gs:{hashlib.md5(name.encode()).hexdigest()}"
            cached = graph_cache.get(cache_key)
            if cached is not None:
                results = cached
            else:
                results = await graph_store.search_nodes(name, limit=5)
                graph_cache.set(cache_key, results)
            for r in results:
                uid = r.get("properties", {}).get("uuid", "")
                if uid and uid not in seen_uuids:
                    seen_uuids.add(uid)
                    graph_nodes.append(r)

        # Get document-entity connections for top vector results
        doc_entity_uuids = set()
        for r in vector_results[:5]:
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

        # Multi-hop subgraph
        all_uuids = list(seen_uuids | doc_entity_uuids)
        subgraph = {}
        if all_uuids:
            try:
                sg_key = f"sg:{hashlib.md5(':'.join(sorted(all_uuids[:10])).encode()).hexdigest()}"
                cached = graph_cache.get(sg_key)
                if cached is not None:
                    subgraph = cached
                else:
                    subgraph = await graph_store.get_subgraph(all_uuids[:10], depth=2)
                    graph_cache.set(sg_key, subgraph)
            except Exception as e:
                logger.warning(f"Subgraph traversal failed: {e}")

        return {
            "vector_results": vector_results,
            "keyword_results": keyword_results,
            "entity_results": entity_results,
            "entity_kw_results": entity_kw_results,
            "graph_nodes": graph_nodes[:15],
            "subgraph": subgraph,
            "entity_names": entity_names,
        }

    # ── Gap analysis synthesis ──────────────────────────────────────

    async def _synthesize_with_gaps(self, question: str, context: dict) -> dict:
        """First-pass synthesis that identifies gaps."""
        doc_context = self._format_doc_context(context)
        graph_text = self._format_graph_context(context)

        prompt = f"""You are a knowledge assistant analyzing retrieved documents and a knowledge graph.

Question: {question}

Document context:
{doc_context}
{graph_text}

Based on this context, provide:
1. A draft answer to the question (be specific, cite details from documents)
2. A confidence score (0-1) for how complete your answer is
3. What information is MISSING that would improve the answer? Generate 1-3 specific follow-up search queries.
4. List all entity names (people, organizations, places, etc.) mentioned in the context.

Respond in JSON: {{"draft_answer": "...", "confidence": 0.8, "follow_up_queries": ["query1", "query2"], "entities_found": ["Entity Name 1", "Entity Name 2"]}}"""

        try:
            result = await self._llm_json(prompt)
            return {
                "draft_answer": result.get("draft_answer", ""),
                "confidence": float(result.get("confidence", 0.5)),
                "follow_up_queries": result.get("follow_up_queries", []),
                "entities_found": result.get("entities_found", []),
            }
        except Exception as e:
            logger.warning(f"Gap analysis failed: {e}")
            return {
                "draft_answer": "",
                "confidence": 0.0,
                "follow_up_queries": [],
                "entities_found": [],
            }

    # ── Graph expansion ─────────────────────────────────────────────

    async def _expand_graph(self, entity_names: list) -> dict:
        """Expand graph from found entity names (2-3 hops)."""
        graph_nodes = []
        seen_uuids = set()

        for name in entity_names[:8]:
            if not isinstance(name, str):
                name = str(name)
            try:
                results = await graph_store.search_nodes(name, limit=3)
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
                subgraph = await graph_store.get_subgraph(list(seen_uuids)[:10], depth=3)
            except Exception as e:
                logger.warning(f"Graph expansion failed: {e}")

        return {
            "vector_results": [],
            "keyword_results": [],
            "entity_results": [],
            "entity_kw_results": [],
            "graph_nodes": graph_nodes,
            "subgraph": subgraph,
            "entity_names": [],
        }

    # ── Final synthesis ─────────────────────────────────────────────

    async def _final_synthesis(self, question: str, context: dict, draft_answer: str) -> dict:
        """Final comprehensive synthesis with all accumulated context."""
        doc_context = self._format_doc_context(context)
        graph_text = self._format_graph_context(context)

        draft_section = ""
        if draft_answer:
            draft_section = f"\n\nDraft answer from initial analysis:\n{draft_answer}\n"

        prompt = f"""You are a knowledge assistant with access to a personal document archive and knowledge graph.
Answer the following question using ALL provided context.

Guidelines:
- Be comprehensive and detailed
- Cite specific documents by title when available, otherwise by document ID
- Include dates, amounts, names — be specific
- If information conflicts between documents, note it
- Structure the answer with headers if it's complex
- State what you DON'T know / couldn't find
- Reference knowledge graph relationships when relevant

Question: {question}
{draft_section}
Document context (from multiple retrieval passes):
{doc_context}
{graph_text}

Provide your answer. Be thorough and cite your sources."""

        try:
            answer = await self._llm_generate(prompt)
        except Exception as e:
            logger.error(f"Final synthesis failed: {e}")
            answer = draft_answer if draft_answer else f"Error generating answer: {e}"

        # Try to get a confidence score
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
        """Merge two context dicts, deduplicating by document_id+chunk_index."""
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

        # Graph nodes
        seen_uuids = set()
        merged_nodes = []
        for n in ctx1.get("graph_nodes", []) + ctx2.get("graph_nodes", []):
            uid = n.get("properties", {}).get("uuid", "")
            if uid and uid not in seen_uuids:
                seen_uuids.add(uid)
                merged_nodes.append(n)
        merged["graph_nodes"] = merged_nodes

        # Merge subgraphs (take the larger one or combine)
        sg1 = ctx1.get("subgraph", {})
        sg2 = ctx2.get("subgraph", {})
        if sg2 and (not sg1 or len(str(sg2)) > len(str(sg1))):
            merged["subgraph"] = sg2
        else:
            merged["subgraph"] = sg1

        # Entity names
        merged["entity_names"] = list(set(
            ctx1.get("entity_names", []) + ctx2.get("entity_names", [])
        ))

        return merged

    # ── Formatting helpers ──────────────────────────────────────────

    def _format_doc_context(self, context: dict) -> str:
        """Format document results for LLM consumption."""
        # Combine and rank
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", [])
        )
        top = combined[:12]

        parts = []
        for r in top:
            title = r.get("title", "")
            doc_type = r.get("doc_type", "")
            header = f"[Document {r['document_id']}"
            if title:
                header += f" - {title}"
            if doc_type:
                header += f" ({doc_type})"
            header += f" chunk {r.get('chunk_index', 0)} (score={r.get('combined_score', r.get('similarity', 0)):.3f})]"
            parts.append(f"{header}:\n{r['content'][:3000]}")

        # Entity results
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
            result += "\n\nEntity matches:\n" + "\n".join(entity_parts[:8])
        return result

    def _format_graph_context(self, context: dict) -> str:
        """Format graph context for LLM consumption."""
        graph_nodes = context.get("graph_nodes", [])
        subgraph = context.get("subgraph", {})
        if not graph_nodes and not subgraph:
            return ""
        graph_data = {"nodes": graph_nodes[:15]}
        if subgraph:
            graph_data["subgraph"] = subgraph
        return "\n\nKnowledge Graph context:\n" + json.dumps(graph_data, indent=2, default=str)[:6000]

    def _build_sources(self, context: dict) -> list[dict]:
        """Build source citations from context."""
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", [])
        )
        sources = []
        for r in combined[:10]:
            source = {
                "document_id": r["document_id"],
                "chunk_index": r.get("chunk_index", 0),
                "similarity": float(r.get("combined_score", r.get("similarity", 0))),
            }
            if r.get("title"):
                source["title"] = r["title"]
            if r.get("doc_type"):
                source["doc_type"] = r["doc_type"]
            # Include a snippet
            source["excerpt"] = r.get("content", "")[:300]
            sources.append(source)
        return sources

    # ── Existing helpers ────────────────────────────────────────────

    async def _cached_vector_search(self, query: str, limit: int = 10) -> list[dict]:
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

Question: {question}"""
            result = await self._llm_json(prompt)
            entities = result.get("entities", [])
            if isinstance(entities, list):
                return [str(e) for e in entities if e][:8]
        except Exception as e:
            logger.warning(f"Entity extraction from query failed: {e}")
        words = question.split()
        return [w for w in words if len(w) > 3][:5]


query_engine = QueryEngine()
