import { backendUrl } from "./backend";

/** Reject browser mutations from other origins before adding the server credential. */
export function mutationAllowed(req: Request): boolean {
  if (["GET", "HEAD"].includes(req.method)) return true;
  const site = req.headers.get("sec-fetch-site");
  if (site && site !== "same-origin" && site !== "none") return false;
  const origin = req.headers.get("origin");
  // Browser mutations must supply an exact origin. Non-browser consumers use the backend.
  if (!origin) return false;
  try {
    const parsed = new URL(origin);
    // Next standalone uses its internal listen address in req.url.
    // The ingress preserves Host; never trust an arbitrary forwarded host.
    const host = req.headers.get("host") || new URL(req.url).host;
    return ["http:", "https:"].includes(parsed.protocol) && parsed.origin === origin && parsed.host === host;
  } catch {
    return false;
  }
}

export async function proxyBackend(req: Request, path: string, streaming = false): Promise<Response> {
  // With no credential, preserve the legacy off-mode proxy behavior.
  if (process.env.KG_API_KEY && !mutationAllowed(req)) {
    return Response.json({ detail: "Same-origin request required" }, { status: 403 });
  }
  const timeoutSignal = streaming ? AbortSignal.timeout(300000) : undefined;
  const signal = timeoutSignal ? AbortSignal.any([req.signal, timeoutSignal]) : req.signal;
  try {
    const response = await fetch(backendUrl(path), {
      method: req.method,
      headers: {
        "Content-Type": req.headers.get("content-type") || "application/json",
        "X-KG-API-Key": process.env.KG_API_KEY || "",
      },
      body: ["GET", "HEAD"].includes(req.method) ? undefined : await req.arrayBuffer(),
      signal,
      cache: "no-store",
      redirect: "manual",
    });
    const headers: Record<string, string> = {
      "Content-Type": response.headers.get("content-type") || "application/json",
      "Cache-Control": "no-store",
    };
    if (headers["Content-Type"].includes("text/event-stream")) {
      headers["Cache-Control"] = "no-cache, no-transform";
      headers["X-Accel-Buffering"] = "no";
    }
    return new Response(response.body, { status: response.status, headers });
  } catch {
    const status = req.signal.aborted ? 499 : timeoutSignal?.aborted ? 504 : 502;
    return Response.json({ detail: status === 504 ? "Query timed out" : "Backend request failed" }, { status });
  }
}
