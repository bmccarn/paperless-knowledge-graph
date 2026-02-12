import { NextRequest } from "next/server";

const BACKEND_URL = process.env.BACKEND_URL || "http://app:8000";

export async function POST(req: NextRequest) {
  const body = await req.text();

  const backendRes = await fetch(`${BACKEND_URL}/query/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });

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
}
