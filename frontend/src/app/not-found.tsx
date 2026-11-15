import Link from "next/link";
import { Button } from "@/components/ui/button";

export default function NotFound() {
  return (
    <div className="h-full overflow-y-auto p-4 md:px-12 md:py-10">
      <h1 className="page-title">Page not found</h1>
      <p className="copy-14 mt-3 text-fg-muted">This page does not exist or has moved.</p>
      <div className="mt-6 flex gap-3">
        <Button asChild><Link href="/query">Ask</Link></Button>
        <Button asChild variant="outline"><Link href="/documents">Documents</Link></Button>
      </div>
    </div>
  );
}
