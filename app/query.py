import json
import logging

from google import genai

from app.config import settings
from app.embeddings import embeddings_store
from app.graph import graph_store

logger = logging.getLogger(__name__)


class QueryEngine:
    def __init__(self):
        self.gemini = genai.Client(api_key=settings.gemini_api_key)
        self.model = settings.gemini_model

    async def query(self, question: str) -> dict:
        """Answer a natural language question using vector search + graph context."""
        # 1. Vector search for relevant documents
        vector_results = await embeddings_store.vector_search(question, limit=8)

        # 2. Extract entity names from query and find graph nodes
        graph_context = await self._get_graph_context(question)

        # 3. Build context
        doc_context = "\n\n".join([
            f"[Document chunk (similarity={r['similarity']:.3f})]:\n{r['content'][:1500]}"
            for r in vector_results
        ])

        graph_text = ""
        if graph_context:
            graph_text = "\n\nGraph context:\n" + json.dumps(graph_context, indent=2, default=str)[:3000]

        # 4. LLM synthesis
        prompt = f"""You are a knowledge assistant with access to a personal document archive. 
Answer the following question based on the provided context from documents and a knowledge graph.

Be specific, cite document details when possible, and say "I don't have enough information" if the context doesn't contain the answer.

Question: {question}

Document context:
{doc_context}
{graph_text}

Answer:"""

        try:
            response = self.gemini.models.generate_content(
                model=self.model,
                contents=prompt,
            )
            answer = response.text
        except Exception as e:
            logger.error(f"Query LLM failed: {e}")
            answer = f"Error generating answer: {e}"

        return {
            "question": question,
            "answer": answer,
            "sources": [
                {"document_id": r["document_id"], "similarity": float(r["similarity"])}
                for r in vector_results[:5]
            ],
            "graph_nodes_used": len(graph_context.get("nodes", [])) if isinstance(graph_context, dict) else 0,
        }

    async def _get_graph_context(self, question: str) -> dict:
        """Search graph for entities mentioned in the question."""
        # Simple approach: search graph for significant words
        words = question.split()
        # Try multi-word combinations and single significant words
        search_terms = []
        # Add the full question as a search
        search_terms.append(question)
        # Add 2-3 word combinations
        for i in range(len(words)):
            for j in range(i + 1, min(i + 4, len(words) + 1)):
                term = " ".join(words[i:j])
                if len(term) > 3:
                    search_terms.append(term)

        all_nodes = []
        seen_props = set()

        for term in search_terms[:10]:  # Limit searches
            results = await graph_store.search_nodes(term, limit=5)
            for r in results:
                props_key = str(r.get("properties", {}).get("uuid", ""))
                if props_key and props_key not in seen_props:
                    seen_props.add(props_key)
                    all_nodes.append(r)

        # Get neighbors for top nodes
        neighborhoods = []
        for node in all_nodes[:3]:
            uuid = node.get("properties", {}).get("uuid")
            if uuid:
                try:
                    neighbors = await graph_store.get_neighbors(uuid, depth=1)
                    neighborhoods.append(neighbors)
                except Exception:
                    pass

        return {
            "nodes": all_nodes[:10],
            "neighborhoods": neighborhoods[:3],
        }


query_engine = QueryEngine()
