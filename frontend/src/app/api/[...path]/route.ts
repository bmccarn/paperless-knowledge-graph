import { NextRequest } from "next/server";
import { proxyBackend } from "@/lib/api-proxy";

export const dynamic = "force-dynamic";

async function proxy(req: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return proxyBackend(req, path.map(encodeURIComponent).join("/") + req.nextUrl.search);
}

export { proxy as GET, proxy as POST, proxy as PUT, proxy as PATCH, proxy as DELETE, proxy as HEAD };
