const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://your-server-host:8484";

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

export async function postQuery(question: string) {
  return apiFetch("/query", {
    method: "POST",
    body: JSON.stringify({ question }),
  });
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
