"use client";

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { CircleX, Search } from "lucide-react";
import { KindShape } from "@/components/kind-shape";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Kbd } from "@/components/ui/kbd";
import { searchEntities } from "@/lib/api";
import { nodeLabelText } from "@/lib/categories";
import {
  addRecent, entityName, entityRow, exactMatch, groupResults, highlight, mostConnected, parseRecent, RECENT_KEY,
  type EntityRow, type RecentEntity,
} from "@/lib/graph-landing";
import { errMsg } from "@/lib/utils";

const slug = (title: string) => title.toLowerCase().replace(/\s+/g, "-");
const focusHref = (id: string) => `/graph?focus=${encodeURIComponent(id)}`;
const documentsText = (count: number) => `${count} ${count === 1 ? "document" : "documents"}`;
const rowsOf = (data: { results?: unknown[] }) =>
  (data.results ?? []).map(entityRow).filter((row): row is EntityRow => row !== null);

// Browser storage can be missing or blocked; Recent is then empty.
const readRecent = () => { try { return localStorage.getItem(RECENT_KEY); } catch { return null; } };
const subscribeRecent = (change: () => void) => {
  window.addEventListener("storage", change);
  return () => window.removeEventListener("storage", change);
};

/** Put a focused entity first in Recent. */
export function rememberRecent(entity: RecentEntity) {
  try {
    localStorage.setItem(RECENT_KEY, JSON.stringify(addRecent(parseRecent(readRecent()), entity)));
  } catch { /* Recent is a convenience */ }
}

function Marked({ text, query }: { text: string; query: string }) {
  return highlight(text, query).map((part, index) => part.match
    ? <mark key={index} className="rounded-[2px] bg-mark px-px text-on-mark">{part.text}</mark>
    : part.text);
}

/** One entity: kind shape, name, type label (not on phone) and evidence documents. */
function EntityLine({ entity, query = "", alias = null, bar }: { entity: RecentEntity; query?: string; alias?: string | null; bar?: number }) {
  return (
    <>
      <KindShape label={entity.label} bare />
      <span className="truncate copy-14 text-fg">
        <Marked text={entityName(entity)} query={query} />
        {alias && <span className="ml-2.5 label-12 text-fg-subtle">Also <Marked text={alias} query={query} /></span>}
      </span>
      <span className="w-[104px] truncate label-12 text-fg-subtle max-md:sr-only">{nodeLabelText(entity.label)}</span>
      {bar !== undefined && (
        <span aria-hidden className="hidden h-1 w-16 overflow-hidden rounded-[2px] bg-border md:block">
          <span className="block h-full rounded-r-[2px] bg-fg-subtle" style={{ width: `${bar}%` }} />
        </span>
      )}
      <span className="min-w-[84px] whitespace-nowrap text-right label-12 tabular-nums text-fg-muted">{documentsText(entity.documents)}</span>
    </>
  );
}

const lineGrid = (bars = false) => `grid items-center gap-3 grid-cols-[16px_minmax(0,1fr)_auto] ${bars ? "md:grid-cols-[16px_minmax(0,1fr)_auto_64px_auto]" : "md:grid-cols-[16px_minmax(0,1fr)_auto_auto]"}`;

function EntityList({ title, entities, bars = false, empty, children }: {
  title: string; entities: RecentEntity[]; bars?: boolean; empty: string; children?: React.ReactNode;
}) {
  const most = Math.max(1, ...entities.map(entity => entity.documents));
  return (
    <section aria-labelledby={`graph-${slug(title)}`}>
      <h2 id={`graph-${slug(title)}`} className="mb-2 label-13 font-semibold text-fg">{title}</h2>
      {entities.length ? (
        <ul className="border-t border-border">
          {entities.map(entity => (
            <li key={entity.id}>
              <Link href={focusHref(entity.id)}
                className={`${lineGrid(bars)} h-12 border-b border-border px-1 hover:bg-raised md:h-10`}>
                <EntityLine entity={entity} bar={bars ? Math.round((entity.documents / most) * 100) : undefined} />
              </Link>
            </li>
          ))}
        </ul>
      ) : <p className="border-t border-border pt-3 copy-14 text-fg-muted">{empty}</p>}
      {children}
    </section>
  );
}

/** `/graph` without a focus: entity search with grouped typeahead, Recent and Most connected. */
export function GraphLanding({ initialQuery }: { initialQuery: string }) {
  const router = useRouter();
  const input = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState(initialQuery);
  const [results, setResults] = useState<{ query: string; rows: EntityRow[] } | null>(null);
  const [searchError, setSearchError] = useState<{ query: string; message: string } | null>(null);
  const [closed, setClosed] = useState(false);
  const [active, setActive] = useState(0);
  const [retry, setRetry] = useState(0);
  const [top, setTop] = useState<EntityRow[] | null>(null);
  const [topError, setTopError] = useState<string | null>(null);
  const [topAttempt, setTopAttempt] = useState(0);
  const autoOpen = useRef(Boolean(initialQuery.trim()));
  const recentText = useSyncExternalStore(subscribeRecent, readRecent, () => null);
  const recent = useMemo(() => parseRecent(recentText), [recentText]);
  const search = query.trim();

  useEffect(() => {
    let current = true;
    // shortcut: 20 rows leave room for the owner and DocumentRef rows; page further if 10 entities are ever short.
    searchEntities("", 20, "documents")
      .then(data => { if (current) setTop(mostConnected(rowsOf(data))); })
      .catch(error => { if (current) setTopError(errMsg(error, "Request failed.")); });
    return () => { current = false; };
  }, [topAttempt]);

  useEffect(() => {
    if (search.length < 2) return;
    let current = true;
    const timer = setTimeout(() => {
      searchEntities(search, 20).then(data => {
        if (!current) return;
        const rows = rowsOf(data);
        const match = autoOpen.current && exactMatch(rows, search);
        autoOpen.current = false;
        if (match) router.replace(focusHref(match.id));
        else setResults({ query: search, rows });
      }).catch(error => { if (current) setSearchError({ query: search, message: errMsg(error, "Request failed.") }); });
    }, 150);
    return () => { current = false; clearTimeout(timer); };
  }, [search, retry, router]);

  useEffect(() => input.current?.focus(), []);
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
  const groups = useMemo(() => groupResults(shown ?? []), [shown]);
  const options = groups.flatMap(group => group.rows);
  const open = search.length >= 2 && !closed && (shown !== null || failed !== null);
  const choose = useCallback((row: EntityRow) => router.push(focusHref(row.id)), [router]);

  return (
    <div className="h-full overflow-y-auto">
      <div className="w-full max-w-[1180px] px-4 pb-16 pt-6 md:px-12 md:py-10">
        <h1 className="page-title">Graph</h1>
        <p className="mt-1.5 hidden max-w-[62ch] copy-14 text-fg-muted md:block">
          Find a person, organization, place or policy, then follow its links to the documents behind them.
        </p>
        <div className="relative mb-6 mt-4 md:mb-0 md:mt-7" onFocus={() => setClosed(false)}
          onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget)) setClosed(true); }}>
          <label className="flex h-12 items-center gap-3 rounded-[var(--r-panel)] border border-border-strong bg-surface pl-4 pr-3 text-fg-muted focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-ring md:h-[52px]">
            <Search aria-hidden className="size-4 shrink-0" strokeWidth={1.75} />
            <span className="sr-only">Search entities</span>
            <input ref={input} type="search" value={query} autoComplete="off" spellCheck={false}
              placeholder="Search entities by name or alias"
              role="combobox" aria-autocomplete="list" aria-expanded={open} aria-controls={open ? "graph-results" : undefined}
              aria-activedescendant={open && options[active] ? `graph-option-${active}` : undefined}
              className="min-w-0 flex-1 bg-transparent text-base text-fg outline-none focus-visible:outline-none! placeholder:text-fg-subtle md:text-[17px] [&::-webkit-search-cancel-button]:hidden"
              onChange={event => { autoOpen.current = false; setQuery(event.target.value); setActive(0); setClosed(false); }}
              onKeyDown={event => {
                if (event.nativeEvent.isComposing) return;
                if (event.key === "Escape") setClosed(true);
                else if (["ArrowDown", "ArrowUp"].includes(event.key) && options.length) {
                  event.preventDefault();
                  setClosed(false);
                  setActive((active + (event.key === "ArrowDown" ? 1 : -1) + options.length) % options.length);
                } else if (event.key === "Enter" && open && options[active]) {
                  event.preventDefault();
                  choose(options[active]);
                }
              }} />
            <Kbd aria-hidden className="hidden md:inline-flex">/</Kbd>
          </label>
          {open && (
            <div id="graph-results" role={options.length ? "listbox" : undefined}
              aria-label={options.length ? `${options.length} results` : undefined}
              className="absolute inset-x-0 top-[60px] z-20 rounded-[var(--r-panel)] border border-border bg-overlay px-2 pb-2 pt-1.5 shadow-[var(--shadow-overlay)]">
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
              ) : options.length ? groups.map(group => (
                <div key={group.title} role="group" aria-labelledby={`graph-group-${slug(group.title)}`}>
                  <p id={`graph-group-${slug(group.title)}`} aria-hidden className="px-2 pb-1 pt-2.5 label-12 text-fg-subtle">{group.title}</p>
                  {group.rows.map(row => {
                    const index = options.indexOf(row);
                    return (
                      <div key={row.id} id={`graph-option-${index}`} role="option" aria-selected={index === active}
                        className={`${lineGrid()} h-12 cursor-pointer rounded-[var(--r-ctl)] px-2 md:h-10 ${index === active ? "bg-raised shadow-[inset_0_0_0_1px_var(--border-strong)] [:root:not(.dark)_&]:bg-accent-bg" : ""}`}
                        onMouseDown={event => event.preventDefault()} onMouseMove={() => setActive(index)} onClick={() => choose(row)}>
                        <EntityLine entity={row} query={search} alias={row.alias} />
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
            </div>
          )}
        </div>
        <div className="grid gap-6 md:mt-10 md:grid-cols-2 md:gap-12">
          <EntityList title="Recent" entities={recent} empty="Entities you open appear here." />
          {topError ? (
            <section aria-label="Most connected">
              <h2 className="mb-2 label-13 font-semibold text-fg">Most connected</h2>
              <Alert variant="destructive">
                <CircleX aria-hidden strokeWidth={1.75} />
                <AlertTitle>The most connected entities could not be loaded.</AlertTitle>
                <AlertDescription>
                  <p>{topError}</p>
                  <Button size="sm" variant="outline" className="mt-1"
                    onClick={() => { setTopError(null); setTopAttempt(value => value + 1); }}>Retry</Button>
                </AlertDescription>
              </Alert>
            </section>
          ) : top ? (
            <EntityList title="Most connected" entities={top} bars empty="No entities yet.">
              <p className="mt-2.5 label-12 text-fg-subtle">Ranked by evidence documents. Your own entity is left out.</p>
            </EntityList>
          ) : (
            <section aria-label="Most connected" aria-busy="true">
              <h2 className="mb-2 label-13 font-semibold text-fg">Most connected</h2>
              <p role="status" className="border-t border-border pt-3 copy-14 text-fg-muted">Loading the most connected entities…</p>
            </section>
          )}
        </div>
      </div>
    </div>
  );
}
