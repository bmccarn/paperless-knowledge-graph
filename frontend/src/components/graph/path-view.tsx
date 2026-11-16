"use client";

import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { ArrowDown, ArrowLeftRight, ArrowUp, CircleX, Loader2, MessageSquareText } from "lucide-react";
import { KindShape } from "@/components/kind-shape";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { ApiError, getGraphPaths, searchEntities } from "@/lib/api";
import { kindColor, nodeKind } from "@/lib/categories";
import { nodeLabel, nodeRadius, shapePath } from "@/lib/graph-encoding";
import { plural, supportText, typeWords } from "@/lib/graph-inspector";
import { entityName, entityRow, type EntityRow } from "@/lib/graph-landing";
import { graphNavigation, listedViews } from "@/lib/graph-url-state";
import {
  askRelated, forward, linkDocs, MAX_LEN, parseMaxLen, pathLayout, routeText, weakestDocs,
  type GraphPath, type PathNode, type PathsResponse,
} from "@/lib/graph-paths";
import { cn, errMsg } from "@/lib/utils";
import { iconData } from "./canvas-icons";
import { ViewSheet, ViewSwitcher } from "./view-switcher";

const heading = "mt-6 mb-2 label-13 font-semibold text-fg";
const nameOf = (node: PathNode | undefined) => (node ? entityName(node) : "");
const askHref = (a: string, b: string) => `/query?draft=${encodeURIComponent(askRelated(a, b))}`;

/** A switch button, drawn like "Links between neighbors". */
function Toggle({ on, label, onClick }: { on: boolean; label: string; onClick: () => void }) {
  return (
    <button type="button" aria-pressed={on} onClick={onClick}
      className="inline-flex h-11 items-center gap-2 rounded-md pr-1 label-13 text-fg hover:text-fg md:h-8">
      <span aria-hidden className={cn("relative h-[18px] w-8 shrink-0 rounded-full border", on ? "border-fg bg-fg" : "border-border-strong")}>
        <span className={cn("absolute top-[2px] size-3 rounded-full", on ? "left-4 bg-bg" : "left-[2px] bg-fg-muted")} />
      </span>
      {label}
    </button>
  );
}

/** A search box that picks one entity: grouped typeahead as on the landing, Up, Down, Enter and Escape. */
function EntityPicker({ id, label, value, onChoose }: { id: string; label: string; value: PathNode | undefined; onChoose: (row: EntityRow) => void }) {
  const [text, setText] = useState<string | null>(null);
  const [found, setFound] = useState<{ query: string; rows: EntityRow[] | null } | null>(null);
  const [active, setActive] = useState(0);
  const query = text?.trim() ?? "";
  useEffect(() => {
    if (query.length < 2) return;
    let current = true;
    const timer = setTimeout(() => searchEntities(query, 8).then(
      data => { if (current) setFound({ query, rows: ((data.results ?? []) as unknown[]).map(entityRow).filter((row): row is EntityRow => row !== null) }); },
      () => { if (current) setFound({ query, rows: null }); }), 150);
    return () => { current = false; clearTimeout(timer); };
  }, [query]);
  const shown = found?.query === query ? found : null;
  const options = shown?.rows ?? [];
  const open = query.length >= 2 && shown !== null;
  const choose = (row: EntityRow) => { setText(null); onChoose(row); };
  return (
    <div className="relative min-w-0 max-md:w-full md:w-[240px]" onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget)) setText(null); }}>
      <label htmlFor={id} className="mb-1 block label-12 text-fg-muted">{label}</label>
      <div className="flex h-11 items-center gap-2 rounded-(--r-ctl) border border-border-strong bg-surface px-2.5 focus-within:outline-2 focus-within:outline-offset-2 focus-within:outline-ring md:h-8">
        {value && text === null && <KindShape label={value.label} bare />}
        <input id={id} type="text" autoComplete="off" spellCheck={false} placeholder="Search entities"
          value={text ?? nameOf(value)} role="combobox" aria-autocomplete="list" aria-expanded={open}
          aria-controls={open ? `${id}-results` : undefined}
          aria-activedescendant={open && options[active] ? `${id}-option-${active}` : undefined}
          className="min-w-0 flex-1 truncate bg-transparent text-base text-fg outline-none focus-visible:outline-none! placeholder:text-fg-subtle md:text-[13px]"
          onFocus={event => event.currentTarget.select()}
          onChange={event => { setText(event.target.value); setActive(0); }}
          onKeyDown={event => {
            if (event.nativeEvent.isComposing) return;
            if (event.key === "Escape") setText(null);
            else if (["ArrowDown", "ArrowUp"].includes(event.key) && options.length) {
              event.preventDefault();
              setActive((active + (event.key === "ArrowDown" ? 1 : -1) + options.length) % options.length);
            } else if (event.key === "Enter" && open && options[active]) {
              event.preventDefault();
              choose(options[active]);
            }
          }} />
      </div>
      {open && (
        <div id={`${id}-results`} role={options.length ? "listbox" : undefined} aria-label={options.length ? `${options.length} results` : undefined}
          className="absolute inset-x-0 top-full z-30 mt-1 min-w-[280px] rounded-(--r-panel) border border-border bg-overlay p-1.5 shadow-(--shadow-overlay)">
          {options.length ? options.map((row, index) => (
            <div key={row.id} id={`${id}-option-${index}`} role="option" aria-selected={index === active}
              className={cn("grid h-11 cursor-pointer grid-cols-[16px_minmax(0,1fr)_auto] items-center gap-3 rounded-(--r-ctl) px-2 md:h-9",
                index === active && "bg-raised shadow-[inset_0_0_0_1px_var(--border-strong)] [:root:not(.dark)_&]:bg-accent-bg")}
              onMouseDown={event => event.preventDefault()} onMouseMove={() => setActive(index)} onClick={() => choose(row)}>
              <KindShape label={row.label} bare />
              <span className="truncate label-13 text-fg">{entityName(row)}</span>
              <span className="label-12 tabular-nums text-fg-muted">{plural(row.documents, "doc")}</span>
            </div>
          )) : (
            <p role="status" className="px-2 py-2 copy-14 text-fg-muted">{shown?.rows ? `No match for ‘${query}’.` : "The search failed."}</p>
          )}
        </div>
      )}
    </div>
  );
}

/** A point and its direction on the cubic from p to q with horizontal tangents, or the quadratic of a direct link. */
function curve(p: { x: number; y: number }, q: { x: number; y: number }, lift: number, direct: boolean) {
  const mx = (p.x + q.x) / 2;
  if (direct) {
    const c = { x: mx, y: (p.y + q.y) / 2 + 2 * lift };
    const at = (t: number) => ({ x: (1 - t) ** 2 * p.x + 2 * (1 - t) * t * c.x + t * t * q.x, y: (1 - t) ** 2 * p.y + 2 * (1 - t) * t * c.y + t * t * q.y,
      dx: 2 * (1 - t) * (c.x - p.x) + 2 * t * (q.x - c.x), dy: 2 * (1 - t) * (c.y - p.y) + 2 * t * (q.y - c.y) });
    return { d: `M${p.x},${p.y}Q${c.x},${c.y} ${q.x},${q.y}`, at };
  }
  const [c1, c2] = [{ x: mx, y: p.y + lift }, { x: mx, y: q.y + lift }];
  const at = (t: number) => {
    const u = 1 - t;
    return {
      x: u ** 3 * p.x + 3 * u * u * t * c1.x + 3 * u * t * t * c2.x + t ** 3 * q.x,
      y: u ** 3 * p.y + 3 * u * u * t * c1.y + 3 * u * t * t * c2.y + t ** 3 * q.y,
      dx: 3 * u * u * (c1.x - p.x) + 6 * u * t * (c2.x - c1.x) + 3 * t * t * (q.x - c2.x),
      dy: 3 * u * u * (c1.y - p.y) + 6 * u * t * (c2.y - c1.y) + 3 * t * t * (q.y - c2.y),
    };
  };
  return { d: `M${p.x},${p.y}C${c1.x},${c1.y} ${c2.x},${c2.y} ${q.x},${q.y}`, at };
}

/** One node: the kind shape and icon of the encoding, faded off the selected path. */
function PathNodeMark({ node, x, y, r, faded }: { node: PathNode; x: number; y: number; r: number; faded: boolean }) {
  const { kind, outlined } = nodeKind(node.label);
  const color = faded ? "var(--graph-faded)" : kindColor(kind);
  const hollow = outlined || kind === "unknown";
  const size = r >= 14 ? 16 : 12;
  return (
    <g transform={`translate(${x},${y})`}>
      <path d={shapePath(node.label, r)} fill={hollow ? "var(--bg)" : color} stroke={hollow ? color : "var(--bg)"} strokeWidth={hollow ? 2 : 4}
        strokeDasharray={kind === "unknown" ? "3.2 2.4" : undefined} strokeLinejoin="round" paintOrder="stroke" />
      {r >= 10 && (
        <g transform={`translate(${-size / 2},${-size / 2 + (kind === "thing" ? r * 0.16 : 0)}) scale(${size / 24})`} fill="none"
          stroke={hollow ? color : faded ? "var(--bg)" : `var(--on-cat-${kind})`} strokeWidth={size < 16 ? 2 : 1.9} strokeLinecap="round" strokeLinejoin="round">
          {iconData(node.label)?.map((d, index) => <path key={index} d={d} />)}
        </g>
      )}
    </g>
  );
}

/** Layered paths, one lane per path; the selected path draws at 3.5 px in --fg with arrows and link labels. */
function PathCanvas({ data, a, b, selected, onSelect, width, height }: {
  data: PathsResponse; a: string; b: string; selected: number; onSelect: (index: number) => void; width: number; height: number;
}) {
  const { positions, bow } = pathLayout(data, a, b, width, height);
  // A second link between the same two entities bows 10 px further from the first, alternately up and down.
  const lifts = new Map<string, number>();
  const pairs = new Map<string, number>();
  for (const path of data.paths) path.links.forEach((key, k) => {
    const pair = [path.nodes[k], path.nodes[k + 1]].sort().join(" ");
    if (lifts.has(key)) return;
    const count = pairs.get(pair) ?? 0;
    pairs.set(pair, count + 1);
    lifts.set(key, (count % 2 ? 1 : -1) * Math.ceil(count / 2) * 10);
  });
  const order = [...data.paths.keys()].filter(index => index !== selected).concat(selected);
  const onPath = new Set(data.paths[selected].nodes);
  const ends = new Set([a, b]);
  const lines = [], marks = [];
  for (const index of order) {
    const path = data.paths[index];
    const on = index === selected;
    for (const [k, key] of path.links.entries()) {
      const link = data.links_detail[key];
      const from = positions.get(path.nodes[k])!, to = positions.get(path.nodes[k + 1])!;
      const { d, at } = curve(from, to, path.length === 1 ? bow[index] : lifts.get(key)!, path.length === 1);
      lines.push(
        <g key={`${index}-${key}`} onClick={() => onSelect(index)} className="cursor-pointer">
          <path d={d} fill="none" stroke="transparent" strokeWidth={14} />
          <path d={d} fill="none" stroke={on ? "var(--fg)" : "var(--border-strong)"} strokeWidth={on ? 3.5 : 1.5}
            strokeDasharray={link.support_state === "inferred" ? "6 4" : undefined} />
        </g>);
      if (!on) continue;
      // The arrow sits 90% along the link in its own direction, which can run against the path.
      const ahead = forward(data, path, k);
      const tip = at(ahead ? 0.9 : 0.1);
      const angle = (Math.atan2(ahead ? tip.dy : -tip.dy, ahead ? tip.dx : -tip.dx) * 180) / Math.PI;
      const mid = at(0.5);
      const flat = Math.abs(to.y - from.y) < 1;
      const anchor = flat ? "middle" : (to.y < from.y) === (to.x > from.x) ? "end" : "start";
      const text = `${link.type} · ${plural(linkDocs(link), "doc")}`;
      marks.push(
        <path key={`arrow-${key}`} d="M0,0L-6,-3.5L-6,3.5Z" fill="var(--fg)" transform={`translate(${tip.x},${tip.y}) rotate(${angle})`} />,
        <text key={`label-${key}`} x={mid.x + (flat ? 0 : anchor === "end" ? -14 : 14)} y={mid.y - (flat ? 10 : 4)} textAnchor={anchor}
          className="fill-fg text-[12px] font-medium" stroke="var(--bg)" strokeWidth={3} paintOrder="stroke" strokeLinejoin="round">{text}</text>);
    }
  }
  const nodes = [...positions].map(([id, { x, y }]) => {
    const node = data.nodes[id];
    const end = ends.has(id);
    const faded = !onPath.has(id);
    const r = end ? 18 : nodeRadius(node.evidence_doc_count ?? 0);
    return (
      <g key={id}>
        <PathNodeMark node={node} x={x} y={y} r={r} faded={faded} />
        <text x={x} y={y + r + 18} textAnchor="middle" stroke="var(--bg)" strokeWidth={3} paintOrder="stroke" strokeLinejoin="round"
          className={cn(end ? "text-[13px] font-medium" : "text-[12px]", faded ? "fill-fg-subtle" : "fill-fg")}>
          {nodeLabel(node.name, id, node.label)}
        </text>
      </g>
    );
  });
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" className="absolute inset-0 font-sans"
      aria-label={`${plural(data.paths.length, "path")} from ${nameOf(data.nodes[a])} to ${nameOf(data.nodes[b])}; path ${selected + 1} selected`}>
      {lines}{nodes}{marks}
    </svg>
  );
}

/** The links of one path as a stepper: each entity, then the link to the next with its support state. */
function Steps({ data, path }: { data: PathsResponse; path: GraphPath }) {
  return (
    <ol aria-label="Steps of the path">
      {path.nodes.map((id, k) => {
        const node = data.nodes[id];
        const link = data.links_detail[path.links[k]];
        const ahead = link && forward(data, path, k);
        const Arrow = ahead ? ArrowDown : ArrowUp;
        return (
          <li key={id}>
            <p className="flex min-h-7 items-center gap-2 label-13 font-medium text-fg"><KindShape label={node.label} bare />{nameOf(node)}</p>
            {link && (
              <div className="ml-[7px] border-l-2 border-fg py-2 pl-4">
                <p className="flex flex-wrap items-center gap-x-2 label-13 text-fg">
                  <Arrow aria-hidden className="size-3.5 text-fg-muted" />
                  <span aria-hidden>{typeWords(link.type)}</span>
                  <span className="sr-only">{nameOf(data.nodes[link.source])} {typeWords(link.type).toLowerCase()} {nameOf(data.nodes[link.target])}.</span>
                  <span className="label-12 text-fg-muted">{supportText(link)}, <span className="tabular-nums">{plural(linkDocs(link), "doc")}</span></span>
                </p>
              </div>
            )}
          </li>
        );
      })}
    </ol>
  );
}

/** "2 links · weakest link 3 docs · has an inferred link" */
function PathMeta({ data, path }: { data: PathsResponse; path: GraphPath }) {
  return (
    <span className="flex flex-wrap gap-x-3 label-12 tabular-nums text-fg-muted">
      <span>{plural(path.length, "link")}</span>
      <span>weakest link {plural(weakestDocs(data, path), "doc")}</span>
      {path.has_inferred && <span>has an inferred link</span>}
    </span>
  );
}

function PathList({ data, indexes, selected, onSelect }: { data: PathsResponse; indexes: number[]; selected: number; onSelect: (index: number) => void }) {
  return (
    <ol aria-label="Paths" className="grid gap-1">
      {indexes.map(index => (
        <li key={index}>
          <button type="button" aria-pressed={index === selected} onClick={() => onSelect(index)}
            className="grid min-h-12 w-full grid-cols-[20px_minmax(0,1fr)] gap-x-2 rounded-(--r-ctl) border border-transparent px-2.5 py-2 text-left hover:bg-raised aria-pressed:border-border-strong aria-pressed:bg-surface">
            <span className="label-13 tabular-nums text-fg-muted">{index + 1}</span>
            <span className="grid gap-0.5">
              <span className="label-13 text-fg">{routeText(data, data.paths[index], nameOf)}</span>
              <PathMeta data={data} path={data.paths[index]} />
            </span>
          </button>
        </li>
      ))}
    </ol>
  );
}

/** The ranked paths, the selected path's steps and "Ask how they are related". Phone puts the selected path first. */
function PathResults({ data, a, b, selected, onSelect, maxLen, includeOwner, allowInferred, phone }: {
  data: PathsResponse; a: string; b: string; selected: number; onSelect: (index: number) => void;
  maxLen: number; includeOwner: boolean; allowInferred: boolean; phone: boolean;
}) {
  const count = data.paths.length;
  const path = data.paths[selected];
  const rules = `${includeOwner ? "The owner is included." : "The owner is left out."}${allowInferred ? "" : " Inferred links are left out."}`;
  const intro = data.exhausted
    ? `${plural(count, "path")} of at most ${maxLen} links, strongest first. ${rules}`
    : `The search stopped before it checked every path of at most ${maxLen} links. These are the strongest ${count}. ${rules}`;
  const ask = (
    <div className="mt-6">
      <Button asChild><Link href={askHref(nameOf(data.nodes[a]), nameOf(data.nodes[b]))}><MessageSquareText />Ask how they are related</Link></Button>
    </div>
  );
  const listTitle = data.exhausted ? null : <h3 className={heading}>Best of the paths checked</h3>;
  if (phone) return (
    <div className="px-4 pb-8 pt-4">
      {listTitle}
      <p className="copy-14 text-fg-muted">{intro}</p>
      <h2 className="mt-4 mb-2 label-13 font-semibold text-fg">
        Path {selected + 1} of {count} <span className="font-normal text-fg-muted">· {plural(path.length, "link")}, weakest {plural(weakestDocs(data, path), "doc")}</span>
      </h2>
      <Steps data={data} path={path} />
      {ask}
      {count > 1 && (
        <>
          <h2 className={heading}>Other paths</h2>
          <PathList data={data} indexes={[...data.paths.keys()].filter(index => index !== selected)} selected={selected} onSelect={onSelect} />
        </>
      )}
    </div>
  );
  return (
    <div className="px-6 pb-8 pt-6" role="region" aria-label="Inspector: paths">
      <h2 className="page-title">How they are related</h2>
      <p className="mt-2 copy-14 text-fg-muted">{intro}</p>
      {listTitle ?? <div className="mt-4" />}
      <PathList data={data} indexes={[...data.paths.keys()]} selected={selected} onSelect={onSelect} />
      <h3 className={heading}>Path {selected + 1}</h3>
      <Steps data={data} path={path} />
      {ask}
    </div>
  );
}

type Result = { request: string; data?: PathsResponse; error?: string; timeout?: boolean };

/**
 * `view=path`: A and B pickers, "Most links", "Include the owner" and "Inferred links", then the ranked paths.
 * Desktop draws them layered beside the inspector; phone lists them as a stepper.
 */
export function PathView({ focus, phone }: { focus: PathNode | null; phone: boolean }) {
  const router = useRouter();
  const params = useSearchParams();
  const a = params.get("a") || null, b = params.get("b") || null;
  const maxLen = parseMaxLen(params.get("maxlen"));
  const includeOwner = params.get("owner") === "show";
  const allowInferred = params.get("support") !== "supported";
  const [picked, setPicked] = useState<Record<string, PathNode>>({});
  const [result, setResult] = useState<Result | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [chosen, setChosen] = useState<{ request: string | null; index: number }>({ request: null, index: 0 });
  const stage = useRef<HTMLElement>(null);
  const [size, setSize] = useState<{ width: number; height: number } | null>(null);

  const request = a && b && a !== b ? [a, b, maxLen, includeOwner, allowInferred].join("|") : null;
  useEffect(() => {
    if (!request || !a || !b) return;
    let active = true;
    getGraphPaths(a, b, maxLen, includeOwner, allowInferred).then(
      data => { if (active) setResult({ request, data }); },
      error => { if (active) setResult({ request, error: errMsg(error, "Request failed."), timeout: error instanceof ApiError && error.status === 503 }); });
    return () => { active = false; };
  }, [request, a, b, maxLen, includeOwner, allowInferred, attempt]);
  useEffect(() => {
    const element = stage.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => setSize({ width: Math.round(entry.contentRect.width), height: Math.round(entry.contentRect.height) }));
    observer.observe(element);
    return () => observer.disconnect();
  }, [phone]);

  const current = result?.request === request ? result : null;
  const data = current?.data ?? null;
  const selected = chosen.request === request && data && chosen.index < data.paths.length ? chosen.index : 0;
  const select = (index: number) => setChosen({ request, index });
  const known = (id: string | null) => (id ? data?.nodes[id] ?? picked[id] ?? (focus?.id === id ? focus : undefined) : undefined);
  const update = (changes: Record<string, string | null>, method: "push" | "replace") => {
    const next = new URLSearchParams(params);
    for (const [key, value] of Object.entries(changes)) if (value) next.set(key, value); else next.delete(key);
    router[method](`/graph?${next}`, { scroll: false });
  };
  // A and B are like the focus: a history step. The rules are filters: they replace it.
  const pick = (end: "a" | "b") => (row: EntityRow) => {
    setPicked(value => ({ ...value, [row.id]: { id: row.id, label: row.label, name: row.name } }));
    update({ [end]: row.id }, "push");
  };

  let message = null;
  if (!a || !b) message = <p className="copy-14 text-fg-muted">Pick two entities to see how they are connected.</p>;
  else if (a === b) message = <p className="copy-14 text-fg-muted">Pick two different entities.</p>;
  else if (!current) message = <p role="status" className="flex items-center gap-2 copy-14 text-fg-muted"><Loader2 className="size-4 animate-spin" />Searching for paths…</p>;
  else if (current.error) message = (
    <Alert variant="destructive" className="max-w-md">
      <CircleX aria-hidden strokeWidth={1.75} />
      <AlertTitle>{current.timeout ? "The path search took too long." : "The path search failed."}</AlertTitle>
      <AlertDescription>
        <p>{current.timeout ? "It stops after 5 seconds. Try fewer links, or leave out inferred links." : current.error}</p>
        <Button size="sm" variant="outline" className="mt-1" onClick={() => { setResult(null); setAttempt(value => value + 1); }}>Retry</Button>
      </AlertDescription>
    </Alert>
  );
  else if (!data?.paths.length) message = (
    <p role="status" className="max-w-md text-balance text-center copy-14 text-fg-muted">
      No path of at most {maxLen} links joins {nameOf(known(a))} and {nameOf(known(b))}.{includeOwner ? "" : " The owner is left out."}
    </p>
  );

  const results = data && data.paths.length > 0 && a && b && (
    <PathResults data={data} a={a} b={b} selected={selected} onSelect={select} maxLen={maxLen}
      includeOwner={includeOwner} allowInferred={allowInferred} phone={phone} />
  );
  return (
    <div className="flex min-h-0 flex-1 flex-col max-md:overflow-y-auto">
      <div className="flex flex-wrap items-end gap-x-4 gap-y-3 border-b px-4 py-3 md:px-5">
        <EntityPicker id="path-from" label="From" value={known(a)} onChoose={pick("a")} />
        <Button variant="ghost" size="icon" aria-label="Swap the two entities" title="Swap the two entities" disabled={!a && !b}
          className="size-11 max-md:hidden md:size-8" onClick={() => update({ a: b, b: a }, "push")}><ArrowLeftRight /></Button>
        <EntityPicker id="path-to" label="To" value={known(b)} onChoose={pick("b")} />
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 md:ml-auto">
          <div className="flex items-center gap-2">
            <span id="path-max-len" className="label-12 text-fg-muted">Most links</span>
            <div role="group" aria-labelledby="path-max-len" className="inline-flex items-center gap-0.5 rounded-(--r-ctl) border border-border-strong p-0.5">
              {MAX_LEN.map(length => (
                <button key={length} type="button" aria-pressed={length === maxLen} onClick={() => update({ maxlen: length === 4 ? null : String(length) }, "replace")}
                  className="h-11 min-w-11 rounded-[4px] px-2.5 text-[13px] font-medium tabular-nums text-fg-muted hover:bg-raised hover:text-fg aria-pressed:bg-accent-solid aria-pressed:text-on-accent md:h-[26px] md:min-w-7">
                  {length}
                </button>
              ))}
            </div>
          </div>
          <Toggle on={includeOwner} label="Include the owner" onClick={() => update({ owner: includeOwner ? null : "show" }, "replace")} />
          <Toggle on={allowInferred} label="Inferred links" onClick={() => update({ support: allowInferred ? "supported" : null }, "replace")} />
        </div>
      </div>
      {phone ? (
        <div>
          {message ? <div className="flex justify-center px-4 py-10">{message}</div> : results}
        </div>
      ) : (
        <div className="flex min-h-0 flex-1">
          {/* Below about 1100 px the inspector leaves the canvas too narrow to draw; it scrolls sideways instead. */}
          <div className="min-h-0 min-w-0 flex-1 overflow-x-auto">
          <section ref={stage} aria-label="Graph workspace" className="relative h-full min-w-[520px]">
            {message ? <div className="flex h-full items-center justify-center p-8">{message}</div>
              : data && a && b && size && <PathCanvas data={data} a={a} b={b} selected={selected} onSelect={select} width={size.width} height={size.height} />}
          </section>
          </div>
          {results && <aside aria-label="Evidence inspector" className="w-[380px] shrink-0 overflow-y-auto border-l border-border">{results}</aside>}
        </div>
      )}
    </div>
  );
}

const PHONE = "(max-width: 767px)";
const subscribePhone = (change: () => void) => {
  const query = window.matchMedia(PHONE);
  query.addEventListener("change", change);
  return () => query.removeEventListener("change", change);
};

/** Paths without a focus entity, opened by `p`, key 5 or a URL: the graph header and the path view. */
export function PathPage() {
  const router = useRouter();
  const params = useSearchParams();
  const phone = useSyncExternalStore(subscribePhone, () => window.matchMedia(PHONE).matches, () => false);
  const switcher = { views: listedViews(), current: "path" as const, focusName: null,
    onSwitch: (view: Parameters<typeof graphNavigation>[1]["view"]) => router.push(graphNavigation(new URLSearchParams(params), { view }).href, { scroll: false }) };
  return (
    <div className="flex h-full min-h-0 flex-col bg-background">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3 md:px-5">
        <h1 className="page-title">Graph</h1>
        <ViewSwitcher {...switcher} className="max-md:hidden" />
      </header>
      <div className="flex h-14 shrink-0 items-center border-b px-4 md:hidden"><ViewSheet {...switcher} /></div>
      <PathView focus={null} phone={phone} />
    </div>
  );
}
