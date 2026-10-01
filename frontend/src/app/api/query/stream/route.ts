import { NextRequest } from "next/server";
import { proxyBackend } from "@/lib/api-proxy";

export const maxDuration = 300;
export const dynamic = "force-dynamic";

export async function POST(req: NextRequest) {
  return proxyBackend(req, "query/stream", true);
}
