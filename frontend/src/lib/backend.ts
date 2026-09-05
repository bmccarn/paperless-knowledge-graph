/** Shared runtime destination for ordinary API requests and SSE. */
export function backendUrl(path: string): string {
  const base = (process.env.BACKEND_URL || "http://app:8000").replace(/\/$/, "");
  return `${base}/${path.replace(/^\//, "")}`;
}
