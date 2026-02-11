import json
import logging
import hashlib

from openai import AsyncOpenAI

from app.config import settings
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
        """Generate text via LiteLLM OpenAI-compatible API."""
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content

    async def _llm_json(self, prompt: str) -> any:
        """Generate JSON via LiteLLM OpenAI-compatible API."""
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)

    async def query(self, question: str) -> dict:
        """Answer a natural language question using hybrid search + graph context."""
        # Check query cache
        cache_key = f"q:{hashlib.md5(question.encode()).hexdigest()}"
        cached = query_cache.get(cache_key)
        if cached is not None:
            cached["cached"] = True
            return cached

        # 1. Determine if query needs decomposition
        sub_queries = await self._maybe_decompose(question)

        # 2. Hybrid search (vector + keyword) for each sub-query
        all_vector_results = []
        all_keyword_results = []
        seen_doc_chunks = set()

        for q in sub_queries:
            vr = await self._cached_vector_search(q, limit=8)
            for r in vr:
                key = (r["document_id"], r["chunk_index"])
                if key not in seen_doc_chunks:
                    seen_doc_chunks.add(key)
                    all_vector_results.append(r)

            kr = await embeddings_store.keyword_search(q, limit=5)
            for r in kr:
                key = (r["document_id"], r["chunk_index"])
                if key not in seen_doc_chunks:
                    seen_doc_chunks.add(key)
                    all_keyword_results.append(r)

        # 3. Combine and rank results
        combined = self._merge_and_rank(all_vector_results, all_keyword_results)
        top_results = combined[:10]

        # 4. Graph-aware retrieval: extract entities from top docs, traverse graph
        graph_context = await self._get_graph_context(question, top_results)

        # 5. Build context for LLM
        doc_context = "\n\n".join([
            f"[Document {r['document_id']} chunk {r.get('chunk_index', 0)} (score={r.get('combined_score', r.get('similarity', 0)):.3f})]:\n{r['content'][:1500]}"
            for r in top_results
        ])

        graph_text = ""
        if graph_context and (graph_context.get("nodes") or graph_context.get("subgraph")):
            graph_text = "\n\nKnowledge Graph context:\n" + json.dumps(graph_context, indent=2, default=str)[:4000]

        # 6. Build source info for citations
        source_docs = []
        for r in top_results[:8]:
            source_docs.append({
                "document_id": r["document_id"],
                "similarity": float(r.get("combined_score", r.get("similarity", 0))),
            })

        # 7. LLM synthesis with citation instructions
        prompt = f"""You are a knowledge assistant with access to a personal document archive and knowledge graph.
Answer the following question based on the provided context from documents and a knowledge graph.

Be specific and thorough. When referencing information, mention the document ID or entity names.
If information comes from the knowledge graph (relationships between entities), explain those connections.
Say "I don't have enough information" if the context doesn't contain the answer.

Question: {question}

Document context:
{doc_context}
{graph_text}

Answer (include specific references to documents and entities where possible):"""

        try:
            answer = await self._llm_generate(prompt)
        except Exception as e:
            logger.error(f"Query LLM failed: {e}")
            answer = f"Error generating answer: {e}"

        # Build entity list from graph context
        entities_found = []
        if isinstance(graph_context, dict):
            for node in graph_context.get("nodes", []):
                props = node.get("properties", node.get("props", {}))
                name = props.get("name") or props.get("title", "")
                if name:
                    entities_found.append({
                        "name": name,
                        "type": (node.get("labels", ["Unknown"])[0] if node.get("labels") else "Unknown"),
                    })

        result = {
            "question": question,
            "answer": answer,
            "sources": source_docs,
            "entities_found": entities_found[:20],
            "graph_nodes_used": len(graph_context.get("nodes", [])) if isinstance(graph_context, dict) else 0,
            "sub_queries": sub_queries if len(sub_queries) > 1 else [],
            "cached": False,
        }

        query_cache.set(cache_key, result)
        return result

    async def _maybe_decompose(self, question: str) -> list[str]:
        """Decompose complex queries into sub-queries."""
        words = question.split()
        if len(words) <= 8:
            return [question]

        try:
            prompt = f"""Analyze this question and determine if it should be broken into simpler sub-queries for document retrieval.

If the question is simple or asks about one thing, return the original question only.
If it's complex or asks about multiple topics, break it into 2-4 focused sub-queries.

Return a JSON object with a "queries" key containing an array of strings. Each string is a search query.

Question: {question}"""

            result = await self._llm_json(prompt)
            sub_queries = result.get("queries", [question])
            if isinstance(sub_queries, list) and len(sub_queries) >= 1:
                return sub_queries[:4]
        except Exception as e:
            logger.warning(f"Query decomposition failed: {e}")

        return [question]

    async def _cached_vector_search(self, query: str, limit: int = 8) -> list[dict]:
        """Vector search with caching."""
        cache_key = f"vs:{hashlib.md5(query.encode()).hexdigest()}:{limit}"
        cached = vector_cache.get(cache_key)
        if cached is not None:
            return cached
        results = await embeddings_store.vector_search(query, limit=limit)
        vector_cache.set(cache_key, results)
        return results

    def _merge_and_rank(self, vector_results: list[dict], keyword_results: list[dict]) -> list[dict]:
        """Merge vector and keyword results, deduplicate, and rank by combined score."""
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

        # Combined score: weight vector higher but keyword provides boost
        for key, r in scored.items():
            r["combined_score"] = 0.7 * r.get("vector_score", 0) + 0.3 * r.get("keyword_score", 0)

        results = sorted(scored.values(), key=lambda x: x["combined_score"], reverse=True)
        return results

    async def _get_graph_context(self, question: str, doc_results: list[dict]) -> dict:
        """Extract entities from query using LLM, search graph, and traverse relationships."""
        # Step 1: Use LLM to extract entity names from the query
        entity_names = await self._extract_entities_from_query(question)

        # Step 2: Search graph for those entities
        all_nodes = []
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
                    all_nodes.append(r)

        # Step 3: Extract entity UUIDs from doc results (via graph search for doc IDs)
        doc_entity_uuids = set()
        for r in doc_results[:5]:
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
                                all_nodes.append({"labels": n.get("labels", []), "properties": n})
                except Exception:
                    pass

        # Step 4: Multi-hop traversal from found entities
        all_entity_uuids = list(seen_uuids | doc_entity_uuids)
        subgraph = {}
        if all_entity_uuids:
            try:
                cache_key = f"sg:{hashlib.md5(':'.join(sorted(all_entity_uuids[:10])).encode()).hexdigest()}"
                cached = graph_cache.get(cache_key)
                if cached is not None:
                    subgraph = cached
                else:
                    subgraph = await graph_store.get_subgraph(all_entity_uuids[:10], depth=2)
                    graph_cache.set(cache_key, subgraph)
            except Exception as e:
                logger.warning(f"Subgraph traversal failed: {e}")

        return {
            "nodes": all_nodes[:15],
            "subgraph": subgraph,
            "entity_uuids_from_docs": list(doc_entity_uuids)[:20],
        }

    async def _extract_entities_from_query(self, question: str) -> list[str]:
        """Use LLM to extract entity names from a query."""
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

        # Fallback: return significant words
        words = question.split()
        return [w for w in words if len(w) > 3][:5]


query_engine = QueryEngine()
