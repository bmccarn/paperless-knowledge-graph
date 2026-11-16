"use client";

import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { CircleX, Search } from "lucide-react";
import { KindShape } from "@/components/kind-shape";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Kbd } from "@/components/ui/kbd";
import { ApiError, getGraphNodeSummary, searchEntities } from "@/lib/api";
import { nodeLabelText } from "@/lib/categories";
import {
  addRecent, defaultFocus, entityName, entityRow, exactMatch, groupResults, highlight, mostConnected, parseRecent, RECENT_KEY,
  type EntityRow, type RecentEntity,
} from "@/lib/graph-landing";
import type { NodeSummary } from "@/lib/graph-layout";
import { graphNavigation } from "@/lib/graph-url-state";
import { cn, errMsg } from "@/lib/utils";

const slug = (title: string) => title.toLowerCase().replace(/\s+/g, "-");
const documentsText = (count: number) => `${count} ${count === 1 ? "document" : "documents"}`;
const rowsOf = (data: { results?: unknown[] }) =>
  (data.results ?? []).map(entityRow).filter((row): row is EntityRow => row !== null);

// Browser storage can be missing or blocked; Recent is then empty.
const readRecent = () => { try { return localStorage.getItem(RECENT_KEY); } catch { return null; } };
const writeRecent = (list: RecentEntity[]) => {
  try { localStorage.setItem(RECENT_KEY, JSON.stringify(list)); } catch { /* Recent is a convenience */ }
};
const subscribeRecent = (change: () => void) => {
  window.addEventListener("storage", change);
  return () => window.removeEventListener("storage", change);
};

/** Put a focused entity first in Recent. */
export const rememberRecent = (entity: RecentEntity) => writeRecent(addRecent(parseRecent(readRecent()), entity));
const forgetRecent = (id: string) => writeRecent(parseRecent(readRecent()).filter(item => item.id !== id));

// The summary `/graph` fetched to check that the last Recent entity still exists; the view uses it once.
let opened: { id: string; summary: Promise<NodeSummary> } | null = null;
export function focusSummary(id: string) {
  const summary = opened?.id === id ? opened.summary : getGraphNodeSummary(id);
  opened = null;
  return summary;
}

function Marked({ text, query }: { text: string; query: string }) {
  return highlight(text, query).map((part, index) => part.match
    ? <mark key={index} className="rounded-[2px] bg-mark px-px text-on-mark">{part.text}</mark>
    : part.text);
}

/** One entity: kind shape, name, type label (not on phone), an evidence bar and evidence documents. */
function EntityLine({ entity, query = "", alias = null, bar }: { entity: RecentEntity; query?: string; alias?: string | null; bar?: number }) {
  return (
    <>
      <KindShape label={entity.label} bare />
      <span className="truncate copy-14 text-fg">
        <Marked text={entityName(entity)} query={query} />
        {alias && <span className="ml-2 label-12 text-fg-subtle">Also <Marked text={alias} query={query} /></span>}
      </span>
      <span className="w-[104px] truncate label-12 text-fg-subtle max-md:sr-only">{nodeLabelText(entity.label)}</span>
      {bar !== undefined && (
        <span aria-hidden className="hidden h-1 w-14 overflow-hidden rounded-[2px] bg-border md:block">
          <span className="block h-full rounded-r-[2px] bg-fg-subtle" style={{ width: `${bar}%` }} />
        </span>
      )}
      <span className="min-w-[84px] whitespace-nowrap text-right label-12 tabular-nums text-fg-muted">{documentsText(entity.documents)}</span>
    </>
  );
}

const lineGrid = (bars = false) => `grid items-center gap-3 grid-cols-[16px_minmax(0,1fr)_auto] ${bars ? "md:grid-cols-[16px_minmax(0,1fr)_auto_56px_auto]" : "md:grid-cols-[16px_minmax(0,1fr)_auto_auto]"}`;

type Group = { title: string; rows: (RecentEntity & { alias?: string | null })[]; bars?: boolean };

/**
 * The graph header's entity search: grouped typeahead after 2 characters; with focus and no text, Recent and
 * Most connected. `/` focuses it. A choice opens that entity in the current view.
 */
export function GraphSearch({ className }: { className?: string }) {
  const router = useRouter();
  const params = useSearchParams();
  const initialQuery = params.get("q") ?? "";
  const input = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState(initialQuery);
  // Back and Forward can change `q` without a new focus; the box follows the address.
  const [shownQuery, setShownQuery] = useState(initialQuery);
  if (shownQuery !== initialQuery) {
    setShownQuery(initialQuery);
    setQuery(initialQuery);
  }
  const [results, setResults] = useState<{ query: string; rows: EntityRow[] } | null>(null);
  const [searchError, setSearchError] = useState<{ query: string; message: string } | null>(null);
  const [closed, setClosed] = useState(true);
  const [active, setActive] = useState(0);
  const [retry, setRetry] = useState(0);
  const [top, setTop] = useState<EntityRow[] | null>(null);
  const [topError, setTopError] = useState<string | null>(null);
  const [topAttempt, setTopAttempt] = useState(0);
  const recentText = useSyncExternalStore(subscribeRecent, readRecent, () => null);
  const recent = useMemo(() => parseRecent(recentText), [recentText]);
  const search = query.trim();
  const browsing = !closed && !query;

  useEffect(() => {
    if (!browsing || top) return;
    let current = true;
    // shortcut: 20 rows leave room for the owner and DocumentRef rows; page further if 10 entities are ever short.
    searchEntities("", 20, "documents")
      .then(data => { if (current) setTop(mostConnected(rowsOf(data))); })
      .catch(error => { if (current) setTopError(errMsg(error, "Request failed.")); });
    return () => { current = false; };
  }, [browsing, top, topAttempt]);

  useEffect(() => {
    if (search.length < 2) return;
    let current = true;
    const timer = setTimeout(() => {
      searchEntities(search, 20)
        .then(data => { if (current) setResults({ query: search, rows: rowsOf(data) }); })
        .catch(error => { if (current) setSearchError({ query: search, message: errMsg(error, "Request failed.") }); });
    }, 150);
    return () => { current = false; clearTimeout(timer); };
  }, [search, retry]);

  // `/graph?q=` opens with its results showing.
  useEffect(() => { if (initialQuery.trim()) input.current?.focus(); }, [initialQuery]);
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement;
      if (event.key !== "/" || event.ctrlKey || event.metaKey || event.altKey || event.defaultPrevented
        || target.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName)) return;
      event.preventDefault();
      input.current?.focus();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  const shown = results?.query === search ? results.rows : null;
  const failed = searchError?.query === search && !shown ? searchError.message : null;
  const groups: Group[] = useMemo(() => browsing
    ? [{ title: "Recent", rows: recent }, { title: "Most connected", rows: top ?? [], bars: true }].filter(group => group.rows.length)
    : groupResults(shown ?? []), [browsing, recent, top, shown]);
  const options = groups.flatMap(group => group.rows);
  const typing = !closed && search.length >= 2 && (shown !== null || failed !== null);
  const open = browsing || typing;
  const choose = (row: RecentEntity) => {
    const current = new URLSearchParams(params);
    current.delete("q");
    // In Paths the choice becomes the start of the path, as "Path from here" does.
    if (current.get("view") === "path") current.set("a", row.id);
    router.push(graphNavigation(current, { focus: row.id }).href, { scroll: false });
    setClosed(true);
    setQuery("");
    input.current?.blur();
  };
  const most = Math.max(1, ...(top ?? []).map(entity => entity.documents));

  return (
    <div className={cn("md:relative", className)} onFocus={() => setClosed(false)}
      onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget)) setClosed(true); }}>
      <label className="flex h-11 items-center gap-2.5 rounded-(--r-ctl) border border-border-strong bg-surface pl-3 pr-1.5 text-fg-muted transition-colors focus-within:border-fg-subtle focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-ring md:h-9">
        <Search aria-hidden className="size-4 shrink-0" strokeWidth={1.75} />
        <span className="sr-only">Search entities</span>
        <input ref={input} type="search" value={query} autoComplete="off" spellCheck={false} placeholder="Search entities"
          role="combobox" aria-autocomplete="list" aria-expanded={open} aria-controls={open ? "graph-results" : undefined}
          aria-activedescendant={open && options[active] ? `graph-option-${active}` : undefined}
          className="min-w-0 flex-1 bg-transparent text-base text-fg outline-none focus-visible:outline-none! placeholder:text-fg-subtle md:text-[13px] [&::-webkit-search-cancel-button]:hidden"
          onChange={event => { setQuery(event.target.value); setActive(0); setClosed(false); }}
          onKeyDown={event => {
            if (event.nativeEvent.isComposing) return;
            if (event.key === "Escape") {
              // Escape closes the list here; it does not reach the graph's own Escape handling.
              if (open) event.stopPropagation();
              setClosed(true);
            } else if (["ArrowDown", "ArrowUp"].includes(event.key)) {
              event.preventDefault();
              setClosed(false);
              if (options.length && open) setActive((active + (event.key === "ArrowDown" ? 1 : -1) + options.length) % options.length);
            } else if (event.key === "Enter" && open && options[active]) {
              event.preventDefault();
              choose(options[active]);
            }
          }} />
        <Kbd aria-hidden className="hidden md:inline-flex">/</Kbd>
      </label>
      {open && (
        <div id="graph-results" role={options.length ? "listbox" : undefined}
          aria-label={options.length ? (browsing ? "Recent and most connected entities" : `${options.length} results`) : undefined}
          className="absolute inset-x-2 top-[calc(100%+4px)] z-40 max-h-[min(560px,calc(100dvh-140px))] overflow-y-auto rounded-(--r-panel) border border-border bg-overlay px-2 pb-2 pt-1 shadow-(--shadow-overlay) md:inset-x-auto md:left-0 md:top-[calc(100%+6px)] md:w-[520px]">
          {failed ? (
            <Alert variant="destructive" className="my-1">
              <CircleX aria-hidden strokeWidth={1.75} />
              <AlertTitle>The search failed.</AlertTitle>
              <AlertDescription>
                <p>{failed}</p>
                <Button size="sm" variant="outline" className="mt-1" onMouseDown={event => event.preventDefault()}
                  onClick={() => { setSearchError(null); setRetry(value => value + 1); }}>Retry</Button>
              </AlertDescription>
            </Alert>
          ) : options.length || browsing ? groups.map(group => (
            <div key={group.title} role="group" aria-labelledby={`graph-group-${slug(group.title)}`}>
              <p id={`graph-group-${slug(group.title)}`} aria-hidden className="px-2 pb-1 pt-3 label-12 text-fg-subtle">{group.title}</p>
              {group.rows.map(row => {
                const index = options.indexOf(row);
                return (
                  <div key={row.id} id={`graph-option-${index}`} role="option" aria-selected={index === active}
                    className={`${lineGrid(group.bars)} h-12 cursor-pointer rounded-(--r-ctl) px-2 md:h-10 ${index === active ? "bg-raised shadow-[inset_0_0_0_1px_var(--border-strong)] [:root:not(.dark)_&]:bg-accent-bg" : ""}`}
                    onMouseDown={event => event.preventDefault()} onMouseMove={() => setActive(index)} onClick={() => choose(row)}>
                    <EntityLine entity={row} query={browsing ? "" : search} alias={row.alias}
                      bar={group.bars ? Math.round((row.documents / most) * 100) : undefined} />
                  </div>
                );
              })}
            </div>
          )) : (
            <div role="status" className="px-2 py-3 copy-14 text-fg-muted">
              <span>No match for &lsquo;{search}&rsquo;.</span>{" "}
              <Link href={`/documents?q=${encodeURIComponent(search)}`} onMouseDown={event => event.preventDefault()}
                className="text-fg underline decoration-border-strong underline-offset-4 hover:decoration-fg">
                Search documents instead
              </Link>
            </div>
          )}
          {browsing && (topError ? (
            <Alert variant="destructive" className="mt-2">
              <CircleX aria-hidden strokeWidth={1.75} />
              <AlertTitle>The most connected entities could not be loaded.</AlertTitle>
              <AlertDescription>
                <p>{topError}</p>
                <Button size="sm" variant="outline" className="mt-1" onMouseDown={event => event.preventDefault()}
                  onClick={() => { setTopError(null); setTopAttempt(value => value + 1); }}>Retry</Button>
              </AlertDescription>
            </Alert>
          ) : !top ? <p role="status" className="px-2 pb-1 pt-3 copy-14 text-fg-muted">Loading the most connected entities…</p>
            : top.length ? <p className="px-2 pt-2 label-12 text-fg-subtle">Ranked by evidence documents. Your own entity is left out.</p>
            : !recent.length && <p className="px-2 pt-3 copy-14 text-fg-muted">No entities yet.</p>)}
        </div>
      )}
    </div>
  );
}

/** The graph page header: title, entity search, then the view controls on the right. */
export function GraphHeader({ controls, children }: { controls?: React.ReactNode; children?: React.ReactNode }) {
  return (
    <header className="relative border-b px-4 py-3 md:px-5">
      <div className="flex items-center gap-3 md:gap-6">
        <h1 className="page-title">Graph</h1>
        <GraphSearch className="min-w-0 flex-1 md:max-w-[300px]" />
        {controls && <div className="ml-auto flex shrink-0 items-center gap-3 max-md:hidden">{controls}</div>}
      </div>
      {children}
    </header>
  );
}

const focusUrl = (id: string, query: string) =>
  `/graph?${new URLSearchParams(query ? { focus: id, q: query } : { focus: id })}`;

/**
 * `/graph` without a focus opens a graph at once: the last entity in Recent if it still exists, else the owner,
 * else the first Most connected. The chosen focus replaces the address, so Back leaves the Graph page.
 * `?q=` with exactly one equal name opens that entity; other text stays in the address and shows its results.
 */
export function GraphOpener({ query }: { query: string }) {
  const router = useRouter();
  const [state, setState] = useState<{ error: string } | "empty" | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let current = true;
    const text = query.trim();
    const open = (id: string, summary?: Promise<NodeSummary>, keepQuery = true) => {
      if (!current) return;
      if (summary) opened = { id, summary };
      router.replace(focusUrl(id, keepQuery ? text : ""), { scroll: false });
    };
    (async () => {
      if (text.length >= 2) {
        const match = exactMatch(rowsOf(await searchEntities(text, 20)), text);
        if (match) return open(match.id, undefined, false);
      }
      const last = parseRecent(readRecent())[0];
      if (last) {
        const summary = getGraphNodeSummary(last.id);
        try {
          await summary;
          return open(last.id, summary);
        } catch (error) {
          // Another failure is the view's to show, with its Retry.
          if (!(error instanceof ApiError && error.status === 404)) return open(last.id, summary);
          forgetRecent(last.id);
        }
      }
      const data = await searchEntities("", 20, "documents");
      const id = defaultFocus(data.owner, rowsOf(data));
      if (id) open(id);
      else if (current) setState("empty");
    })().catch(error => { if (current) setState({ error: errMsg(error, "Request failed.") }); });
    return () => { current = false; };
  }, [query, attempt, router]);

  return (
    <div className="flex h-full min-h-0 flex-col bg-background">
      <GraphHeader />
      <div className="flex flex-1 items-center justify-center px-4 pb-16">
        {state === "empty" ? (
          <div className="max-w-[44ch] text-center">
            <p className="heading-16 text-fg">No entities yet.</p>
            <p className="mt-1.5 copy-14 text-fg-muted">The graph fills in as documents are read. Run a sync, then come back.</p>
          </div>
        ) : state ? (
          <Alert variant="destructive" className="max-w-lg">
            <CircleX aria-hidden strokeWidth={1.75} />
            <AlertTitle>The graph could not be opened.</AlertTitle>
            <AlertDescription>
              <p>{state.error}</p>
              <Button size="sm" variant="outline" className="mt-1" onClick={() => { setState(null); setAttempt(value => value + 1); }}>Retry</Button>
            </AlertDescription>
          </Alert>
        ) : <p role="status" className="copy-14 text-fg-muted">Opening the graph…</p>}
      </div>
    </div>
  );
}
