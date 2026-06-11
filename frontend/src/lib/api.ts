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

export async function getFreshness() {
  return apiFetch("/freshness");
}

export async function postSync() {
  return apiFetch("/sync", { method: "POST" });
}

export async function postReindex() {
  return apiFetch("/reindex", { method: "POST" });
}

export async function postReindexDoc(docId: number, waitForCompletion = true) {
  const response = await apiFetch(`/reindex/${docId}`, { method: "POST" });
  if (waitForCompletion && response.task_id) {
    await waitForTask(response.task_id);
  }
  return response;
}

export async function postRepairDrift() {
  return apiFetch("/freshness/repair", { method: "POST" });
}

export async function getDocumentDetail(docId: number) {
  return apiFetch(`/document/${docId}/detail`);
}

export async function postDocumentFeedback(docId: number, reason: string, note = "") {
  return apiFetch(`/document/${docId}/feedback`, {
    method: "POST",
    body: JSON.stringify({ reason, note }),
  });
}

export async function getTask(taskId: string) {
  return apiFetch(`/task/${taskId}`);
}

export async function waitForTask(taskId: string, intervalMs = 2000, timeoutMs = 15 * 60 * 1000) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const task = await getTask(taskId);
    if (task.status === "completed") return task;
    if (task.status === "failed" || task.status === "cancelled") {
      throw new Error(task.error || `Task ${task.status}`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error("Timed out waiting for reindex task to finish");
}

export async function postQuery(question: string, conversationId?: string, model?: string, mode = "deep") {
  return apiFetch("/query", {
    method: "POST",
    body: JSON.stringify({ question, conversation_id: conversationId, model, mode }),
  });
}

// SSE streaming query
export async function* postQueryStream(question: string, conversationId?: string, model?: string, mode = "deep") {
  const response = await fetch(`${API_URL}/query/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, conversation_id: conversationId, model, mode }),
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

export async function generateTitle(message: string): Promise<string> {
  const resp = await apiFetch("/generate-title", {
    method: "POST",
    body: JSON.stringify({ message }),
  });
  return resp.title;
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

export async function getEntityReviewCandidates(limit = 50) {
  return apiFetch(`/entity-review/candidates?limit=${limit}`);
}

export async function ignoreEntityCandidate(left_uuid: string, right_uuid: string, note = "") {
  return apiFetch("/entity-review/ignore", {
    method: "POST",
    body: JSON.stringify({ left_uuid, right_uuid, note }),
  });
}

export async function splitEntityCandidate(left_uuid: string, right_uuid: string, note = "") {
  return apiFetch("/entity-review/split", {
    method: "POST",
    body: JSON.stringify({ left_uuid, right_uuid, note }),
  });
}

export async function mergeEntityCandidate(primary_uuid: string, duplicate_uuid: string) {
  return apiFetch("/entity-review/merge", {
    method: "POST",
    body: JSON.stringify({ primary_uuid, duplicate_uuid }),
  });
}

export async function runEntitySteward(limit = 75) {
  return apiFetch(`/entity-review/steward/task?limit=${limit}`, { method: "POST" });
}

export async function cancelTask(taskId: string) {
  return apiFetch(`/task/${taskId}/cancel`, { method: "POST" });
}




// Models API
export interface ModelInfo {
  id: string;
  name: string;
}

export async function getModels(): Promise<{ models: ModelInfo[]; default: string }> {
  return apiFetch("/models");
}

// Config (paperless URL, etc.)
let _configCache: { paperless_url: string } | null = null;

export async function getConfig(): Promise<{ paperless_url: string }> {
  if (_configCache) return _configCache;
  _configCache = await apiFetch("/config");
  return _configCache!;
}

export function getPaperlessDocUrl(docId: number, paperlessUrl: string): string {
  return `${paperlessUrl.replace(/\/$/, "")}/documents/${docId}/`;
}
