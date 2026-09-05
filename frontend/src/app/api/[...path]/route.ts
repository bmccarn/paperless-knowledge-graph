import { NextRequest } from "next/server";
import { backendUrl } from "@/lib/backend";

export const dynamic = "force-dynamic";

async function proxy(req: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  try {
    const response = await fetch(backendUrl(path.map(encodeURIComponent).join("/") + req.nextUrl.search), {
      method: req.method,
      headers: { "Content-Type": req.headers.get("content-type") || "application/json" },
      body: ["GET", "HEAD"].includes(req.method) ? undefined : await req.arrayBuffer(),
      signal: req.signal,
      cache: "no-store",
    });
    return new Response(response.body, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("content-type") || "application/json",
        "Cache-Control": "no-store",
      },
    });
  } catch {
    return Response.json({ detail: "Backend request failed" }, { status: 502 });
  }
}

export { proxy as GET, proxy as POST, proxy as PUT, proxy as PATCH, proxy as DELETE, proxy as HEAD };
