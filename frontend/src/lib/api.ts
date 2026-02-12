const API_URL = process.env.NEXT_PUBLIC_API_URL || "/api";

export async function apiFetch(path: string, options?: RequestInit) {
  const res = await fetch(`${API_URL}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export async function getStatus() {
  return apiFetch("/status");
}

export async function postSync() {
  return apiFetch("/sync", { method: "POST" });
}

export async function postReindex() {
  return apiFetch("/reindex", { method: "POST" });
}

export async function postReindexDoc(docId: number) {
  return apiFetch(`/reindex/${docId}`, { method: "POST" });
}

export async function getTask(taskId: string) {
  return apiFetch(`/task/${taskId}`);
}

export async function postQuery(question: string, conversationId?: string) {
  return apiFetch("/query", {
    method: "POST",
    body: JSON.stringify({ question, conversation_id: conversationId }),
  });
}

// SSE streaming query
export async function* postQueryStream(question: string, conversationId?: string) {
  const response = await fetch(`${API_URL}/query/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, conversation_id: conversationId }),
  });

  if (!response.ok) {
    throw new Error(`Stream error: ${response.status} ${response.statusText}`);
  }

  const reader = response.body?.getReader();
  if (!reader) throw new Error("No response body reader");

  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const events = buffer.split("\n\n");
      buffer = events.pop() || "";

      for (const event of events) {
        if (!event.trim()) continue;
        const match = event.match(/^data: (.+)$/m);
        if (match) {
          try {
            yield JSON.parse(match[1]);
          } catch {
            console.warn("SSE parse error:", event);
          }
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}

// Conversation API
export async function listConversations(limit = 50) {
  return apiFetch(`/conversations?limit=${limit}`);
}

export async function createConversation(title = "New conversation") {
  return apiFetch("/conversations", {
    method: "POST",
    body: JSON.stringify({ title }),
  });
}

export async function getConversation(id: string) {
  return apiFetch(`/conversations/${id}`);
}

export async function renameConversation(id: string, title: string) {
  return apiFetch(`/conversations/${id}`, {
    method: "PATCH",
    body: JSON.stringify({ title }),
  });
}

export async function deleteConversation(id: string) {
  return apiFetch(`/conversations/${id}`, { method: "DELETE" });
}

export async function graphSearch(q: string, type?: string, limit = 20) {
  const params = new URLSearchParams({ q, limit: String(limit) });
  if (type) params.set("type", type);
  return apiFetch(`/graph/search?${params}`);
}

export async function getGraphNode(uuid: string) {
  return apiFetch(`/graph/node/${uuid}`);
}

export async function getGraphNeighbors(uuid: string, depth = 2) {
  return apiFetch(`/graph/neighbors/${uuid}?depth=${depth}`);
}

export async function getGraphInitial(limit = 300) {
  return apiFetch(`/graph/initial?limit=${limit}`);
}

export async function resolveEntities() {
  return apiFetch("/resolve-entities", { method: "POST" });
}

export async function cancelTask(taskId: string) {
  return apiFetch(`/task/${taskId}/cancel`, { method: "POST" });
}
