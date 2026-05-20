import { NextRequest } from "next/server";

// Allow up to 5 minutes for long-running queries
export const maxDuration = 300;
export const dynamic = "force-dynamic";

const BACKEND_URL = process.env.BACKEND_URL || "http://app:8000";

export async function POST(req: NextRequest) {
  const body = await req.text();

  const controller = new AbortController();
  // 5 minute timeout for the backend fetch
  const timeout = setTimeout(() => controller.abort(), 300000);

  try {
    const backendRes = await fetch(`${BACKEND_URL}/query/stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
      signal: controller.signal,
    });

    clearTimeout(timeout);

    if (!backendRes.ok) {
      return new Response(backendRes.statusText, { status: backendRes.status });
    }

    // Pipe the SSE stream straight through
    return new Response(backendRes.body, {
      status: 200,
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
      },
    });
  } catch (e: any) {
    clearTimeout(timeout);
    if (e.name === "AbortError") {
      return new Response("Query timed out", { status: 504 });
    }
    return new Response(e.message || "Internal error", { status: 500 });
  }
}
