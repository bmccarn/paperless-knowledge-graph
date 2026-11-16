"use client";

import { useState, type ReactNode } from "react";
import Link from "next/link";
import { ChevronDown, ChevronRight, FileText, MessageSquareText, Minus, Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { KindShape, TypeDot } from "@/components/kind-shape";
import {
  askAbout,
  documentsByDate,
  entityName,
  keyFacts,
  linksByFamily,
  lowestConfidence,
  plural,
  rankLinks,
  scoreText,
  shortDate,
  supportSummary,
  supportText,
  typeWords,
  type SummaryDocument,
} from "@/lib/graph-inspector";
import { FAMILIES, MORE_STEP, VIEW_LIMIT, type LayoutSector, type NodeSummary, type SummaryLink } from "@/lib/graph-layout";
import { LinkEvidence } from "./link-evidence";

/** Rows per family and documents before "Show all". */
const FAMILY_ROWS = 6;
const DOCUMENT_ROWS = 5;
const GROUP_ROWS = 10;

const familyName = (family: string) => FAMILIES.find(([key]) => key === family)?.[1] ?? "Other";
const askHref = (name: string) => `/query?draft=${encodeURIComponent(askAbout(name))}`;
const heading = "mt-6 mb-2 label-13 font-semibold text-fg";

interface RowEvents {
  onSelectLink: (key: string) => void;
  /** Keyboard focus on a row marks its neighbor on the canvas. */
  onFocusRow: (id: string | null) => void;
}

function Shell({ label, onClose, children }: { label: string; onClose?: () => void; children: ReactNode }) {
  return (
    <div className="relative px-4 pb-8 pt-5 md:px-6 md:pt-6" aria-label={label} role="region">
      {onClose && (
        <Button variant="ghost" size="icon-sm" aria-label="Clear selection" onClick={onClose} className="absolute right-2 top-2 md:right-4 md:top-4">
          <X />
        </Button>
      )}
      {children}
    </div>
  );
}

/** One link of the focus: the other entity, its kind and the relationship, and its documents. */
function LinkRow({ link, onSelectLink, onFocusRow }: { link: SummaryLink } & RowEvents) {
  const name = entityName(link.neighbor);
  return (
    <li>
      <button type="button" onClick={() => onSelectLink(link.key)}
        onFocus={() => onFocusRow(link.neighbor.id)} onBlur={() => onFocusRow(null)}
        className="grid w-full grid-cols-[minmax(0,1fr)_auto] gap-x-2 rounded-md px-2 py-1.5 text-left hover:bg-raised">
        <span className="truncate label-13 text-fg">{name}</span>
        <span className="label-12 tabular-nums text-fg-muted">{plural(link.records.length, "doc")}</span>
        <span className="col-span-2 flex min-w-0 flex-wrap items-center gap-x-1.5 label-12 text-fg-subtle">
          <KindShape label={link.neighbor.label} className="label-12 text-fg-subtle" />
          <span aria-hidden>·</span>
          <span>{typeWords(link.type)}</span>
          {link.support_state !== "supported" && <><span aria-hidden>·</span><span>{supportText(link)}</span></>}
        </span>
      </button>
    </li>
  );
}

function DocumentList({ documents, limit }: { documents: SummaryDocument[]; limit: number }) {
  const [all, setAll] = useState(false);
  return (
    <>
      <ul className="divide-y divide-border">
        {documents.slice(0, all ? undefined : limit).map(doc => (
          <li key={doc.id}>
            <Link href={`/documents/${doc.id}`} className="grid gap-0.5 py-2 hover:underline hover:underline-offset-4">
              <span className="font-[family-name:var(--font-source-serif)] text-[15px] leading-[22px] text-fg">{doc.title}</span>
              <span className="flex flex-wrap items-center gap-x-3 label-12 tabular-nums text-fg-subtle">
                {doc.doc_type && <TypeDot type={doc.doc_type} className="label-12" />}
                {shortDate(doc.date) && <span>{shortDate(doc.date)}</span>}
              </span>
            </Link>
          </li>
        ))}
      </ul>
      {documents.length > limit && (
        <Button variant="link" size="xs" className="mt-2 px-0" aria-expanded={all} onClick={() => setAll(value => !value)}>
          {all ? "Show fewer" : `Show all ${plural(documents.length, "document")}`}
        </Button>
      )}
    </>
  );
}

function FamilyLinks({ name, links, inferred, defaultOpen, ...events }: {
  name: string; links: SummaryLink[]; inferred: number; defaultOpen: boolean;
} & RowEvents) {
  const [all, setAll] = useState(false);
  return (
    <Collapsible defaultOpen={defaultOpen} className="group border-b border-border">
      <CollapsibleTrigger className="flex h-9 w-full items-center gap-2 text-left label-13 text-fg">
        <ChevronRight aria-hidden className="size-3.5 text-fg-muted group-data-[state=open]:hidden" />
        <ChevronDown aria-hidden className="size-3.5 text-fg-muted group-data-[state=closed]:hidden" />
        <span>{name}</span>
        {inferred > 0 && <span className="label-12 text-fg-subtle">{inferred} inferred</span>}
        <span className="ml-auto tabular-nums text-fg-muted">{links.length}</span>
      </CollapsibleTrigger>
      <CollapsibleContent className="pb-2 pl-4">
        <ul aria-label={`${name} links`}>
          {links.slice(0, all ? undefined : FAMILY_ROWS).map(link => <LinkRow key={link.key} link={link} {...events} />)}
        </ul>
        {links.length > FAMILY_ROWS && (
          <Button variant="link" size="xs" className="ml-2 px-0" aria-expanded={all} onClick={() => setAll(value => !value)}>
            {all ? "Show fewer" : `Show all ${links.length}`}
          </Button>
        )}
      </CollapsibleContent>
    </Collapsible>
  );
}

function Actions({ name, children }: { name: string; children?: ReactNode }) {
  return (
    <div className="mt-4 flex flex-wrap gap-2">
      <Button asChild><Link href={askHref(name)}><MessageSquareText />Ask about this</Link></Button>
      {children}
    </div>
  );
}

/** The focus entity: kind, name, aliases, actions, key facts, support summary, links by family and documents. */
export function FocusInspector({ summary, onClose, onOpenTable, ...events }: { summary: NodeSummary; onClose?: () => void; onOpenTable: () => void } & RowEvents) {
  const { focus } = summary;
  const name = entityName(focus);
  const documents = documentsByDate(summary);
  const facts = keyFacts(summary);
  const groups = linksByFamily(summary);
  return (
    <Shell label={`Inspector: ${name}`} onClose={onClose}>
      <p className="flex items-center label-12 text-fg-muted">
        <KindShape label={focus.label} className="label-12 text-fg-muted" />{focus.is_owner && ", the archive owner"}
      </p>
      <h2 className="mt-2 mb-1 page-title break-words pr-8">{name}</h2>
      {focus.aliases.length > 0 && <p className="label-13 text-fg-muted">Also {focus.aliases.join(", ")}</p>}
      <Actions name={name}>
        <Button variant="outline" asChild>
          <Link href={`/documents?entity=${encodeURIComponent(focus.id)}`}><FileText /><span className="tabular-nums">{plural(documents.length, "document")}</span></Link>
        </Button>
        <Button variant="outline" onClick={onOpenTable}>Open in table</Button>
      </Actions>
      {facts.length > 0 && (
        <>
          <h3 className={heading}>Key facts</h3>
          <dl className="label-13">
            {facts.map(link => (
              <div key={link.key} className="grid grid-cols-[88px_minmax(0,1fr)_auto] gap-2 border-b border-border py-1.5">
                <dt className="text-fg-muted">{typeWords(link.type)}</dt>
                <dd className="min-w-0 break-words text-fg">{entityName(link.neighbor)}</dd>
                <dd className="label-12 text-fg-muted">{supportText(link)}</dd>
              </div>
            ))}
          </dl>
        </>
      )}
      <p className="mt-3 copy-14 text-fg-muted">{supportSummary(summary)}</p>
      {summary.owner_duplicates.length > 0 && (
        <>
          <h3 className={heading}>Possible duplicates of this entity</h3>
          <ul aria-label="Possible duplicates of this entity">
            {summary.owner_duplicates.map(entity => (
              <li key={entity.id}>
                <Link href={`/graph?focus=${encodeURIComponent(entity.id)}`}
                  className="grid grid-cols-[minmax(0,1fr)_auto] gap-x-2 rounded-md px-2 py-1.5 hover:bg-raised">
                  <span className="truncate label-13 text-fg">{entityName(entity)}</span>
                  <span className="label-12 tabular-nums text-fg-muted">{plural(entity.evidence_doc_count, "document")}</span>
                </Link>
              </li>
            ))}
          </ul>
          <p className="mt-1 px-2 label-12 text-fg-muted">
            They also match the owner name. <Link href="/entities/review" className="text-fg underline underline-offset-4">Review on Entities</Link>
          </p>
        </>
      )}
      {groups.length > 0 && (
        <>
          <h3 className={heading}>Links by family</h3>
          {groups.map((group, index) => (
            <FamilyLinks key={group.family} name={group.name} links={group.links} inferred={group.inferred} defaultOpen={index === 0} {...events} />
          ))}
        </>
      )}
      {documents.length > 0 && (
        <>
          <h3 className={heading}>Mentioned in</h3>
          <DocumentList documents={documents} limit={DOCUMENT_ROWS} />
        </>
      )}
    </Shell>
  );
}

/**
 * A neighbor: built from the focus summary, so it shows the neighbor's links with the focus,
 * then its drawn links to other neighbors, seen from this neighbor.
 */
export function NeighborInspector({ summary, neighborId, crossLinks = [], onClose, onFocusEntity, ...events }: {
  summary: NodeSummary; neighborId: string; crossLinks?: SummaryLink[]; onClose: () => void; onFocusEntity: (id: string) => void;
} & RowEvents) {
  const links = rankLinks(summary, summary.links.filter(link => link.neighbor.id === neighborId));
  const neighbor = links[0]?.neighbor;
  if (!neighbor) return null;
  const name = entityName(neighbor);
  return (
    <Shell label={`Inspector: ${name}`} onClose={onClose}>
      <p className="label-12 text-fg-muted"><KindShape label={neighbor.label} className="label-12 text-fg-muted" /></p>
      <h2 className="mt-2 mb-1 page-title break-words pr-8">{name}</h2>
      <p className="label-13 text-fg-muted">{plural(neighbor.evidence_doc_count, "document")} in the archive.</p>
      <Actions name={name}>
        <Button variant="outline" onClick={() => onFocusEntity(neighbor.id)}>Focus this entity</Button>
      </Actions>
      <h3 className={heading}>Links with {entityName(summary.focus)}</h3>
      <ul>{links.map(link => <LinkRow key={link.key} link={link} {...events} />)}</ul>
      {crossLinks.length > 0 && (
        <>
          <h3 className={heading}>Linked to other neighbors</h3>
          <ul aria-label="Linked to other neighbors">{crossLinks.map(link => <LinkRow key={link.key} link={link} {...events} />)}</ul>
        </>
      )}
    </Shell>
  );
}

/**
 * An entity in Explore, at any hop: its kind, name and distance from the root, the action that grows the view from it
 * (or opens it in Focus at the last hop), Collapse when it was expanded, and its links drawn in this view.
 */
export function ExploreInspector({ entity, hop, rootName, links, grow, onCollapse, onFocusEntity, onClose, ...events }: {
  entity: { id: string; name: string; label: string; evidence_doc_count: number };
  hop: number;
  rootName: string;
  /** Links drawn in this view, seen from this entity. */
  links: SummaryLink[];
  grow: { label: string; disabled: boolean; onClick: () => void };
  onCollapse?: () => void;
  onFocusEntity: (id: string) => void;
  onClose: () => void;
} & RowEvents) {
  const name = entityName(entity);
  return (
    <Shell label={`Inspector: ${name}`} onClose={onClose}>
      <p className="label-12 text-fg-muted"><KindShape label={entity.label} className="label-12 text-fg-muted" /></p>
      <h2 className="mt-2 mb-1 page-title break-words pr-8">{name}</h2>
      <p className="label-13 text-fg-muted">
        {plural(entity.evidence_doc_count, "document")} in the archive.{hop > 0 && ` ${plural(hop, "step")} from ${rootName}.`}
      </p>
      <div className="mt-4 flex flex-wrap gap-2">
        <Button onClick={grow.onClick} disabled={grow.disabled}>{!grow.disabled && <Plus />}{grow.label}</Button>
        {onCollapse && <Button variant="outline" onClick={onCollapse}><Minus />Collapse</Button>}
        {hop < 3 && <Button variant="outline" onClick={() => onFocusEntity(entity.id)}>Open in Focus</Button>}
        <Button variant="outline" asChild><Link href={askHref(name)}><MessageSquareText />Ask about this</Link></Button>
      </div>
      {links.length > 0 && (
        <>
          <h3 className={heading}>Linked in this view</h3>
          <ul aria-label="Linked in this view">{links.map(link => <LinkRow key={link.key} link={link} {...events} />)}</ul>
        </>
      )}
    </Shell>
  );
}

/** A link: family, endpoints, type, support state, lowest score and its evidence. `end` replaces the focus for a link between neighbors. */
export function LinkInspector({ summary, link, end = summary.focus, onClose, onSelectEntity }: {
  summary: NodeSummary; link: SummaryLink; end?: { id: string; name: string; label: string }; onClose: () => void;
  /** Explore: each end's name selects that entity, the keyboard route to nodes. */
  onSelectEntity?: (id: string) => void;
}) {
  const endName = (entity: { id: string; name: string; label: string }) => onSelectEntity
    ? <button type="button" className="text-left underline-offset-4 hover:underline" onClick={() => onSelectEntity(entity.id)}>{entityName(entity)}</button>
    : <span>{entityName(entity)}</span>;
  const [from, to] = link.direction === "out" ? [end, link.neighbor] : [link.neighbor, end];
  const ids = new Set(link.records.map(record => record.doc_id));
  return (
    <Shell label="Inspector: link" onClose={onClose}>
      <p className="label-12 text-fg-muted">Link in {familyName(link.family)}</p>
      <h2 className="mt-2 mb-1 flex flex-wrap items-baseline gap-x-2 pr-8 font-[family-name:var(--font-geist-serif-regular)] text-xl leading-[26px] text-fg">
        {endName(from)}
        <span className="font-sans label-13 text-fg-muted">{typeWords(link.type).toLowerCase()}</span>
        {endName(to)}
      </h2>
      <p className="flex flex-wrap gap-x-4 gap-y-1 border-b border-border pb-3 pt-2 label-12 text-fg-muted">
        <span className="font-semibold text-fg">{supportText(link)}</span>
        <span className="tabular-nums">{plural(ids.size, "document")}</span>
        <span className="tabular-nums">{scoreText("Lowest confidence", lowestConfidence(link))}</span>
      </p>
      <LinkEvidence link={link} documents={summary.documents} names={[from.name, to.name]} />
      <div className="mt-6 flex flex-wrap gap-2">
        <Button asChild>
          <Link href={`/query?draft=${encodeURIComponent(`How is ${entityName(from)} related to ${entityName(to)}?`)}`}><MessageSquareText />Ask about this link</Link>
        </Button>
      </div>
    </Shell>
  );
}

/** A "+N" pill: the family's links that are not drawn, ranked, and the control that adds up to 25 to the canvas. */
export function GroupInspector({ sector, links, adds, onAdd, onClose, atLimit, onOpenTable, ...events }: {
  sector: LayoutSector; links: SummaryLink[]; adds: number; onAdd: () => void; onClose: () => void; atLimit: boolean; onOpenTable: () => void;
} & RowEvents) {
  const [all, setAll] = useState(false);
  return (
    <Shell label={`Inspector: ${sector.name}`} onClose={onClose}>
      <h2 className="pr-8 font-[family-name:var(--font-geist-serif-regular)] text-lg leading-6 text-fg">{sector.name}</h2>
      <p className="mt-3 copy-14 text-fg-muted">
        {plural(sector.total, "link")}. {sector.drawn} {sector.drawn === 1 ? "is" : "are"} on the canvas and {sector.hidden} {sector.hidden === 1 ? "is" : "are"} in
        this group, ranked by evidence documents.
      </p>
      <div className="mt-4 flex flex-wrap gap-2">
        {atLimit ? <Button onClick={onOpenTable}>Open in table</Button> : <Button onClick={onAdd} disabled={!adds}><Plus />Add {adds || MORE_STEP} to the canvas</Button>}
      </div>
      {!adds && <p role="status" className="mt-2 label-12 text-fg-muted">The canvas shows at most {VIEW_LIMIT} entities.</p>}
      <ul aria-label={`${sector.name} group`} className="mt-3 border-t border-border pt-1.5">
        {links.slice(0, all ? undefined : GROUP_ROWS).map(link => <LinkRow key={link.key} link={link} {...events} />)}
      </ul>
      {links.length > GROUP_ROWS && (
        <Button variant="link" size="xs" className="mt-1 px-0" aria-expanded={all} onClick={() => setAll(value => !value)}>
          {all ? "Show fewer" : `Show all ${links.length}`}
        </Button>
      )}
    </Shell>
  );
}
