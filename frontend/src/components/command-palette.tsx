"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { Search } from "lucide-react";
import { apiFetch, getDocuments, graphSearch } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle, DialogDescription } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Kbd } from "@/components/ui/kbd";

const pages = [
  { title: "Ask", href: "/query" },
  { title: "Documents", href: "/documents" },
  { title: "Graph", href: "/graph" },
  { title: "Entities", href: "/entities/review" },
  { title: "Status", href: "/" },
  { title: "Hubs", href: "/hubs" },
  { title: "Logs", href: "/debug" },
];
type Row = { title: string; href: string };
type Group = { title: string; rows: Row[]; failed?: boolean };
type SearchNode = { labels: string[]; properties: { uuid: string; name: string } };

async function findEntities(query: string): Promise<Row[]> {
  const rows: Row[] = [];
  for (let offset = 0; rows.length < 5; offset += 50) {
    const data = await graphSearch(query, undefined, 50, offset);
    rows.push(...data.results.filter((node: SearchNode) => !node.labels.includes("Document") && !node.labels.includes("DocumentRef"))
      .map((node: SearchNode) => ({ title: node.properties.name, href: `/graph?q=${encodeURIComponent(node.properties.name)}` })));
    if (!data.has_more) break;
  }
  return rows.slice(0, 5);
}

async function findConversations(query: string): Promise<Row[]> {
  const rows: Row[] = [];
  for (let offset = 0; rows.length < 5; offset += 50) {
    const conversations: { id: string; title: string }[] = await apiFetch(`/conversations?limit=50&offset=${offset}`);
    rows.push(...conversations.filter(row => row.title.toLocaleLowerCase().includes(query.toLocaleLowerCase()))
      .map(row => ({ title: row.title, href: `/query?conversation=${encodeURIComponent(row.id)}` })));
    if (conversations.length < 50) break;
  }
  return rows.slice(0, 5);
}

export function CommandPalette({ open, onOpenChange, returnFocus }: {
  open: boolean; onOpenChange: (open: boolean) => void; returnFocus: () => void;
}) {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [groups, setGroups] = useState<Group[]>([]);
  const [loading, setLoading] = useState(false);
  const [active, setActive] = useState(0);
  const [retry, setRetry] = useState(0);
  const list = useRef<HTMLDivElement>(null);
  const rows = groups.flatMap(group => group.rows);
  const search = query.trim();

  useEffect(() => {
    if (!open || search.length < 2) return;
    let current = true;
    const timer = setTimeout(async () => {
      const results = await Promise.allSettled([
        getDocuments(search, "", 0, 5).then(data => data.results.map((node: { properties: { title: string; paperless_id: number } }) => ({
          title: node.properties.title, href: `/documents/${node.properties.paperless_id}`,
        }))),
        findEntities(search),
        findConversations(search),
      ]);
      if (!current) return;
      setGroups([
        { title: "Pages", rows: pages.filter(page => page.title.toLocaleLowerCase().includes(search.toLocaleLowerCase())) },
        ...results.map((result, index) => ({ title: ["Documents", "Entities", "Conversations"][index],
          rows: result.status === "fulfilled" ? result.value : [], failed: result.status === "rejected" })),
      ]);
      setLoading(false);
    }, 150);
    return () => { current = false; clearTimeout(timer); };
  }, [open, search, retry]);

  useEffect(() => { list.current?.querySelector('[aria-selected="true"]')?.scrollIntoView({ block: "nearest" }); }, [active]);

  function choose(row: Row) {
    onOpenChange(false);
    router.push(row.href);
  }

  return <Dialog open={open} onOpenChange={onOpenChange}>
    <DialogContent data-palette className="gap-0 p-0 md:max-w-xl md:p-0" onCloseAutoFocus={event => { event.preventDefault(); returnFocus(); }}>
      <div className="space-y-2 border-b p-4 pr-14">
        <DialogTitle>Search pages and archive</DialogTitle>
        <DialogDescription>Find a page, document, entity or saved conversation.</DialogDescription>
      </div>
      <div className="p-4">
        <label htmlFor="palette-query" className="sr-only">Search pages and archive</label>
        <Input id="palette-query" role="combobox" aria-autocomplete="list" aria-expanded="true" aria-controls="palette-results"
          aria-activedescendant={rows[active] ? `palette-option-${active}` : undefined}
          placeholder="Type at least 2 characters…" value={query}
          onChange={event => { setQuery(event.target.value); setGroups([]); setActive(0); setLoading(event.target.value.trim().length >= 2); }}
          onKeyDown={event => {
            if (event.nativeEvent.isComposing) return;
            if (["ArrowDown", "ArrowUp"].includes(event.key)) {
              event.preventDefault();
              if (rows.length) setActive(index => (index + (event.key === "ArrowDown" ? 1 : -1) + rows.length) % rows.length);
            } else if (event.key === "Enter") {
              event.preventDefault();
              if (rows[active]) choose(rows[active]);
            }
          }} />
      </div>
      <p role="status" className="label-12 px-4 pb-3 text-fg-muted">
        {search.length < 2 ? "Type at least 2 characters to search." : loading ? "Searching…" : `${rows.length} results`}
      </p>
      <div ref={list} id="palette-results" role="listbox" aria-label="Search results" aria-busy={loading}
        className="max-h-[45dvh] overflow-y-auto px-2 pb-2">
        {groups.map(group => <div key={group.title} role="group" aria-label={group.title} className="mb-2">
          <p aria-hidden="true" className="label-12 px-2 py-2 text-fg-muted">{group.title}</p>
          {group.rows.map(row => {
            const index = rows.indexOf(row);
            return <div key={row.href} id={`palette-option-${index}`} role="option" aria-selected={active === index}
              className={`label-14 min-h-11 cursor-pointer rounded-md px-2 py-3 md:min-h-9 md:py-2 ${active === index ? "bg-raised" : "hover:bg-raised"}`}
              onMouseDown={event => event.preventDefault()} onMouseMove={() => setActive(index)} onClick={() => choose(row)}>
              <span className={group.title === "Documents" ? "doc-title" : undefined}>{row.title}</span>
            </div>;
          })}
          {!group.rows.length && <p className="label-13 px-2 py-1 text-fg-muted">{group.failed ? "Search unavailable." : "No matches."}</p>}
        </div>)}
      </div>
      {groups.some(group => group.failed) && <div role="alert" className="flex items-center justify-between gap-2 border-t p-4">
        <p className="label-13">Some searches failed.</p>
        <Button variant="outline" onClick={() => { setLoading(true); setRetry(value => value + 1); }}>Retry</Button>
      </div>}
      <div className="label-12 flex flex-wrap gap-4 border-t p-4 text-fg-muted">
        <span><Kbd>↑</Kbd> <Kbd>↓</Kbd> Move</span><span><Kbd>Enter</Kbd> Open</span><span><Kbd>Esc</Kbd> Close</span>
      </div>
    </DialogContent>
  </Dialog>;
}

export function PaletteSearchButton({ onClick, className }: { onClick: () => void; className?: string }) {
  return <Button variant="outline" className={className} onClick={onClick} aria-label="Search pages and archive">
    <Search className="size-4" strokeWidth={1.75} aria-hidden="true" />
    Search <Kbd className="ml-auto hidden md:inline-flex">Ctrl K</Kbd>
  </Button>;
}
