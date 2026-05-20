import asyncio
import json
import logging
import hashlib
import time
from collections import defaultdict
from openai import RateLimitError

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

    # ── Main query ──────────────────────────────────────────────────

    async def query(self, question: str, conversation_history: list = None, model_override: str = None) -> dict:
        """Answer a question using iterative retrieval + synthesis."""
        prev_override = self._model_override
        if model_override:
            self._model_override = model_override
        conv_suffix = ""
        if conversation_history:
            conv_text = " ".join(m.get("content", "")[:50] for m in conversation_history[-4:])
            conv_suffix = hashlib.md5(conv_text.encode()).hexdigest()[:8]
        cache_key = normalize_query_key(question + conv_suffix)
        cached = query_cache.get(cache_key)
        if cached is not None:
            cached["cached"] = True
            return cached

        # Phase 1: Initial retrieval + broad decomposition in parallel
        is_broad = self._is_broad_query(question)
        if is_broad:
            # Decompose, graph retrieve, and vector retrieve concurrently
            decompose_task = asyncio.create_task(self._decompose_query(question))
            graph_docs_task = asyncio.create_task(self._retrieve_graph_documents(question))
            initial_context = await self._retrieve(question)
            sub_queries = await decompose_task
            logger.info(f"Broad query: {len(sub_queries)} sub-queries: {sub_queries}")

            # Retrieve ALL sub-queries in parallel (light — no LLM entity extraction)
            if sub_queries:
                sub_results = await asyncio.gather(
                    *[self._retrieve_light(sq) for sq in sub_queries],
                    return_exceptions=True
                )
                for sr in sub_results:
                    if isinstance(sr, dict):
                        initial_context = self._merge_context(initial_context, sr)

            # Merge graph-driven document chunks
            graph_doc_context = await graph_docs_task
            if graph_doc_context:
                initial_context = self._merge_context(initial_context, graph_doc_context)
        else:
            initial_context = await self._retrieve(question)

        # Phase 2: Gap analysis
        first_pass = await self._synthesize_with_gaps(question, initial_context, conversation_history)
        all_context = initial_context

        # Phase 3: Follow-up retrievals in parallel (light — no LLM entity extraction)
        follow_up_queries = first_pass.get("follow_up_queries", [])[:5]

        # For broad queries, inject guaranteed follow-ups for known high-value gaps.
        # Mortgage payment changes live in separate "Payment Change Notice" docs that
        # don't score well against "bills" queries — inject them at the front so they
        # always run, displacing lower-priority LLM-generated follow-ups if needed.
        if is_broad:
            q_lower = question.lower()
            injected = []
            if any(s in q_lower for s in ["mortgage", "payment", "bill", "obligation", "financial"]):
                injected.append("PHH Mortgage current monthly payment amount payment change notice 2025 2026")
            # Prepend injected queries, cap total at 7 for broad queries
            if injected:
                follow_up_queries = injected + [q for q in follow_up_queries if q not in injected]
                follow_up_queries = follow_up_queries[:7]
            logger.info(f"Follow-up queries (incl. {len(injected)} injected): {follow_up_queries}")

        if follow_up_queries:
            follow_results = await asyncio.gather(
                *[self._retrieve_light(fq) for fq in follow_up_queries],
                return_exceptions=True
            )
            for fr in follow_results:
                if isinstance(fr, dict):
                    all_context = self._merge_context(all_context, fr)

        # Phase 4: Graph expansion
        entities_found = first_pass.get("entities_found", [])
        graph_context = await self._expand_graph(entities_found)
        all_context = self._merge_context(all_context, graph_context)

        # Phase 5: Final synthesis
        final = await self._final_synthesis(
            question, all_context, first_pass.get("draft_answer", ""), conversation_history, broad=is_broad
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
            "follow_up_queries_used": follow_up_queries,
            "iterations": 1 + len(follow_up_queries) + (len(sub_queries) if is_broad else 0),
            "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
            "cached": False,
        }

        query_cache.set(cache_key, result)
        return result

    # ── Streaming query (SSE) ───────────────────────────────────────

    async def query_stream(self, question: str, conversation_history: list = None, model_override: str = None):
        """Stream query response via SSE events."""
        prev_override = self._model_override
        if model_override:
            self._model_override = model_override
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

        # Broad query decomposition + parallel retrieval
        is_broad = self._is_broad_query(question)
        if is_broad:
            yield {"type": "status", "message": "Broad query detected — decomposing into focused searches..."}
            decompose_task = asyncio.create_task(self._decompose_query(question))
            graph_docs_task = asyncio.create_task(self._retrieve_graph_documents(question))
            initial_context = await self._retrieve(question)
            sub_queries = await decompose_task
            if sub_queries:
                yield {"type": "status", "message": f"Running {len(sub_queries)} parallel sub-searches..."}
                sub_results = await asyncio.gather(
                    *[self._retrieve_light(sq) for sq in sub_queries],
                    return_exceptions=True
                )
                for sr in sub_results:
                    if isinstance(sr, dict):
                        initial_context = self._merge_context(initial_context, sr)

            # Graph-driven retrieval: get docs linked to key entity types
            graph_doc_context = await graph_docs_task
            if graph_doc_context:
                yield {"type": "status", "message": f"Adding graph-linked documents for coverage..."}
                initial_context = self._merge_context(initial_context, graph_doc_context)
        else:
            yield {"type": "status", "message": "Searching documents and knowledge graph..."}
            initial_context = await self._retrieve(question)

        yield {"type": "status", "message": "Analyzing results and identifying gaps..."}
        first_pass = await self._synthesize_with_gaps(question, initial_context, conversation_history)
        all_context = initial_context

        # Parallel follow-up retrievals (light)
        follow_up_queries = first_pass.get("follow_up_queries", [])[:5]

        # Inject guaranteed follow-ups for known high-value gaps on broad queries
        if is_broad:
            q_lower = question.lower()
            injected = []
            if any(s in q_lower for s in ["mortgage", "payment", "bill", "obligation", "financial"]):
                injected.append("PHH Mortgage current monthly payment amount payment change notice 2025 2026")
            if injected:
                follow_up_queries = injected + [q for q in follow_up_queries if q not in injected]
                follow_up_queries = follow_up_queries[:7]

        if follow_up_queries:
            yield {"type": "status", "message": f"Running {len(follow_up_queries)} follow-up searches in parallel..."}
            follow_results = await asyncio.gather(
                *[self._retrieve_light(fq) for fq in follow_up_queries],
                return_exceptions=True
            )
            for fr in follow_results:
                if isinstance(fr, dict):
                    all_context = self._merge_context(all_context, fr)

        entities_found = first_pass.get("entities_found", [])
        if entities_found:
            yield {"type": "status", "message": f"Expanding knowledge graph ({len(entities_found)} entities)..."}
            graph_context = await self._expand_graph(entities_found)
            all_context = self._merge_context(all_context, graph_context)

        yield {"type": "status", "message": "Synthesizing comprehensive answer..."}

        prompt = self._build_final_prompt(
            question,
            self._format_doc_context(all_context, broad=is_broad),
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
            "follow_up_queries_used": follow_up_queries,
            "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
            "iterations": 1 + len(follow_up_queries),
            "cached": False,
        }
        query_cache.set(cache_key, result)

        yield {"type": "complete", "sources": sources,
               "entities_found": result["entities_found"],
               "confidence": result.get("confidence", 0.7),
               "follow_up_suggestions": first_pass.get("follow_up_suggestions", []),
               "cached": False}

    # ── Retrieval (TUNED: wider net) ────────────────────────────────

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
        """Graph-driven retrieval using two complementary strategies:
        1. Organization-centric: most recent doc per Organization (ensures every payee represented)
        2. Entity-type-driven: docs linked to relevant entity types (financial, insurance, etc.)
        Both run in parallel, merge, and deduplicate.
        Chunks are tagged with _org_rank (1=first doc per org, 2=second) so tier 2
        selection prioritizes coverage (one doc per org) before depth (second docs)."""
        entity_types = await self._get_relevant_entity_types(question)

        # Run both strategies in parallel
        tasks = []
        tasks.append(self._graph_retrieve_by_org(entity_types=entity_types))
        if entity_types:
            tasks.append(self._graph_retrieve_by_entity_type(entity_types))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Merge doc IDs from both strategies, deduplicate
        all_doc_ids = set()
        org_count = 0
        for r in results:
            if isinstance(r, dict):
                for did in r.get("doc_ids", []):
                    all_doc_ids.add(did)
                org_count = max(org_count, r.get("org_count", 0))

        if not all_doc_ids:
            return {}

        # Use vector similarity to find the most relevant chunks among graph-discovered docs.
        # Instead of blindly picking by recency, we search the query embedding against ONLY
        # the graph-discovered doc IDs. This ensures Starlink invoices (semantically similar
        # to "recurring bills") beat out irrelevant recent docs.
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

        logger.info(f"Graph-driven retrieval: {org_count} orgs + {len(entity_types)} entity types → {len(all_doc_ids)} unique docs → {len(chunks)} chunks")
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
        """Get the most recent documents for each Organization in the graph.
        When entity_types is provided, only returns orgs connected to entities
        of those types (e.g., financial orgs for financial queries, medical
        providers for medical queries). Pulls 2 docs per org for temporal coverage.
        Returns org_docs with rank info so tier 2 can prioritize 1st-per-org docs."""
        try:
            if entity_types:
                org_docs = await graph_store.get_recent_docs_per_organization_filtered(
                    entity_types=entity_types, limit_per_org=3
                )
            else:
                org_docs = await graph_store.get_recent_docs_per_organization(limit_per_org=3)

            # Assign rank: for each org, first doc = rank 1, second = rank 2
            org_seen = {}
            ranked_docs = []
            for d in org_docs:
                org = d["org_name"]
                if org not in org_seen:
                    org_seen[org] = 1
                else:
                    org_seen[org] += 1
                ranked_docs.append({
                    "doc_id": d["doc_id"],
                    "org_name": org,
                    "rank": org_seen[org],
                })

            doc_ids = [d["doc_id"] for d in ranked_docs]
            orgs = set(d["org_name"] for d in ranked_docs)
            logger.info(f"Org-centric retrieval: {len(orgs)} organizations → {len(doc_ids)} docs (filtered by {entity_types})")
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
        """Determine which entity types to query from the graph based on the question.
        Query-agnostic: covers financial, medical, equipment, legal, location, and more."""
        q_lower = question.lower()
        types = []

        financial_signals = ["bill", "payment", "financial", "obligation", "recurring", "monthly", "expense",
                           "mortgage", "loan", "subscription", "utility", "owe", "pay", "cost", "fee",
                           "charge", "balance", "statement", "account", "bank", "credit"]
        if any(s in q_lower for s in financial_signals):
            types.extend(["FinancialItem", "Contract"])

        insurance_signals = ["insurance", "policy", "coverage", "premium", "deductible", "claim",
                           "insured", "underwriter", "liability"]
        if any(s in q_lower for s in insurance_signals):
            types.extend(["InsurancePolicy"])

        medical_signals = ["medical", "health", "diagnosis", "condition", "disability", "medication",
                         "treatment", "doctor", "hospital", "va ", "veteran", "rating", "vaccine",
                         "immunization", "lab", "blood", "test result", "prescription", "surgery",
                         "dental", "vision", "therapy", "physical"]
        if any(s in q_lower for s in medical_signals):
            types.extend(["MedicalResult", "Condition"])

        equipment_signals = ["mower", "vehicle", "car", "truck", "device", "appliance", "manual",
                           "instructions", "oil change", "maintenance", "repair", "equipment",
                           "tool", "machine", "model", "serial number", "warranty"]
        if any(s in q_lower for s in equipment_signals):
            types.extend(["Product", "System"])

        legal_signals = ["contract", "agreement", "lease", "terms", "deed", "legal",
                        "court", "attorney", "settlement", "notarized", "signed"]
        if any(s in q_lower for s in legal_signals):
            types.extend(["Contract", "DocumentRef"])

        event_signals = ["when did", "date", "timeline", "history", "deployment", "service record",
                        "stationed", "assigned", "milestone", "ceremony", "graduation"]
        if any(s in q_lower for s in event_signals):
            types.extend(["DateEvent", "Event"])

        location_signals = ["where", "location", "address", "stationed", "deployed", "lived",
                          "moved", "residence", "city", "state", "base"]
        if any(s in q_lower for s in location_signals):
            types.extend(["Location", "Address"])

        property_signals = ["property", "house", "home", "real estate", "mortgage", "escrow",
                          "hoa", "homeowner"]
        if any(s in q_lower for s in property_signals):
            types.extend(["Address", "Contract", "FinancialItem"])

        people_signals = ["who", "person", "people", "family", "contact", "employee",
                        "spouse", "dependent", "beneficiary"]
        if any(s in q_lower for s in people_signals):
            types.extend(["Person"])

        org_signals = ["company", "employer", "provider", "vendor", "agency", "organization"]
        if any(s in q_lower for s in org_signals):
            types.extend(["Organization"])

        # For broad queries: ensure minimum type coverage
        is_broad = any(w in q_lower for w in ["all", "every", "everything", "comprehensive", "complete"])
        if is_broad and len(set(types)) < 3:
            types.extend(["FinancialItem", "InsurancePolicy", "Contract", "MedicalResult"])

        return list(set(types))

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

        prompt = f"""You are a knowledge assistant analyzing personal documents belonging to {_owner_name()}.
{conv_context}
Current question: {question}

Document context:
{doc_context}
{graph_text}

Analyze the context and provide:
1. A draft answer — be specific, cite document titles. Include ALL relevant details you can find. If this is a follow-up question, use the conversation context to understand what "it", "that", "more details", etc. refer to.
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

        return f"""You are a knowledge assistant with access to {_owner_name()}'s personal document archive and knowledge graph. You have been given context from multiple retrieval passes across hundreds of personal documents.

CONTEXT ABOUT THE USER:
- The user is {_owner_name()}
- Documents include: medical records, VA disability ratings, military service records, financial documents, mortgage statements, legal contracts, vehicle records, pet/veterinary records, insurance policies, tax documents, employment records, and more
- When the user says "my", "I", "me" — they mean {_owner_name()}
- {("Additional context: " + _owner_context()) if _owner_context() else ""}

INSTRUCTIONS:
- Be EXHAUSTIVE — use every piece of relevant information from the context. Do not summarize away details.
- For questions about identity ("who am I"), cover ALL life domains: personal info, military service, education, medical/health, disability status, financial overview, property, family, employment, vehicles, pets — whatever the documents reveal.
- For ratings/statuses that change over time (VA disability, credit scores, balances, etc.), always identify and clearly state the MOST RECENT / FINAL / CURRENT value. If multiple values exist across documents, show the progression chronologically and highlight the latest.

TEMPORAL AWARENESS — CRITICAL:
- Every document has a date or effective period. USE THESE to determine what is CURRENT vs. EXPIRED.
- If an insurance policy has an effective period that ended before today, mark it as EXPIRED/PREVIOUS and clearly indicate the replacement policy if one exists.
- If a contract, lease, or subscription has expired, say so explicitly — do not present it as active.
- When payment amounts change over time (e.g., mortgage escrow adjustments), always report the CURRENT amount and note the progression.
- For addresses: distinguish between current residence and previous addresses. Do not list bills from a previous address as current obligations unless there's evidence of ongoing service.
- When two policies/services of the same type overlap, determine which is the ACTIVE one based on effective dates and mark the other as superseded.
- Today's date for reference: use the most recent document dates as a proxy for "now".
- Cite sources using document TITLES: (Source: "Document Title")
- If no title available, use: (Document 305)
- Include ALL specific details: dates, amounts, percentages, names, medical terms, account numbers
- Format monetary values ($1,234.56), dates (January 15, 2024), and percentages (100%) clearly
- If information conflicts between documents, note BOTH, explain which is more current based on dates, and clearly label the outdated one as PREVIOUS/EXPIRED/SUPERSEDED
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

    async def _final_synthesis(self, question: str, context: dict, draft_answer: str, conversation_history: list = None, broad: bool = False) -> dict:
        doc_context = self._format_doc_context(context, broad=broad)
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

    def _format_doc_context(self, context: dict, broad: bool = False) -> str:
        combined = self._merge_and_rank(
            context.get("vector_results", []),
            context.get("keyword_results", [])
        )
        # Broad queries get more chunks for category coverage (75 vs 25)
        chunk_limit = 75 if broad else 25
        # For broad queries, ensure document diversity: max 2 chunks per document
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

        unique_doc_ids = set(r.get("document_id") for r in top)
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

    def _diversify_chunks(self, ranked_chunks: list[dict], limit: int = 40, max_per_doc: int = 2) -> list[dict]:
        """Select chunks ensuring document diversity — max N chunks per document.
        Uses two-tier selection: vector/keyword results fill first half,
        graph-driven results fill second half, ensuring structural coverage
        doesn't get drowned out by semantic similarity scores."""
        # Separate graph-driven from vector/keyword results
        vector_chunks = [c for c in ranked_chunks if c.get("_source") != "graph_driven"]
        graph_chunks = [c for c in ranked_chunks if c.get("_source") == "graph_driven"]

        logger.info(f"Chunk selection: {len(ranked_chunks)} total ({len(vector_chunks)} vector, {len(graph_chunks)} graph), limit={limit}")

        selected = []
        seen_doc_ids = set()
        doc_counts = defaultdict(int)

        # Tier 1: Top vector/keyword results (up to 50% of limit)
        tier1_limit = int(limit * 0.5)
        for chunk in vector_chunks:
            doc_id = chunk.get("document_id")
            if doc_counts[doc_id] < max_per_doc:
                selected.append(chunk)
                doc_counts[doc_id] += 1
                seen_doc_ids.add(doc_id)
            if len(selected) >= tier1_limit:
                break

        tier1_doc_ids = set(c.get("document_id") for c in selected)
        logger.info(f"Tier 1 (vector): {len(selected)} chunks from {len(tier1_doc_ids)} unique docs — IDs: {sorted(tier1_doc_ids)}")

        # Tier 2: Graph-driven results, sorted by SEMANTIC SIMILARITY to the query.
        # Graph retrieval already ran a vector search scoped to graph-discovered docs,
        # so these chunks are pre-ranked by relevance. Just maintain that order.
        tier2_start = len(selected)
        graph_sorted = sorted(
            graph_chunks,
            key=lambda c: c.get("similarity", 0),
            reverse=True
        )
        for chunk in graph_sorted:
            doc_id = chunk.get("document_id")
            if doc_id not in seen_doc_ids and doc_counts[doc_id] < max_per_doc:
                selected.append(chunk)
                doc_counts[doc_id] += 1
                seen_doc_ids.add(doc_id)
            if len(selected) >= limit:
                break

        tier2_doc_ids = set(c.get("document_id") for c in selected[tier2_start:])
        logger.info(f"Tier 2 (graph): {len(selected) - tier2_start} chunks from {len(tier2_doc_ids)} unique docs — IDs: {sorted(tier2_doc_ids)}")

        # Tier 3: Fill remaining with any unchosen chunks (vector or graph)
        if len(selected) < limit:
            tier3_start = len(selected)
            remaining = [c for c in ranked_chunks if c not in selected]
            for chunk in remaining:
                doc_id = chunk.get("document_id")
                if doc_counts[doc_id] < max_per_doc:
                    selected.append(chunk)
                    doc_counts[doc_id] += 1
                if len(selected) >= limit:
                    break
            if len(selected) > tier3_start:
                tier3_doc_ids = set(c.get("document_id") for c in selected[tier3_start:])
                logger.info(f"Tier 3 (fill): {len(selected) - tier3_start} chunks from {len(tier3_doc_ids)} unique docs")

        logger.info(f"Final selection: {len(selected)} chunks from {len(seen_doc_ids)} unique documents")
        return selected

    def _merge_and_rank(self, vector_results: list[dict], keyword_results: list[dict]) -> list[dict]:
        scored = {}
        for r in vector_results:
            key = (r["document_id"], r.get("chunk_index", 0))
            if key not in scored:
                scored[key] = {**r, "vector_score": float(r.get("similarity", 0)), "keyword_score": 0.0}
            else:
                scored[key]["vector_score"] = max(scored[key].get("vector_score", 0), float(r.get("similarity", 0)))
                # Preserve graph_driven source tag if present
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

        return sorted(scored.values(), key=lambda x: x["combined_score"], reverse=True)

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
