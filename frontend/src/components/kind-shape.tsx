import {
  Building2, Calendar, CalendarClock, CircleHelp, Cpu, FileQuestion, FileSignature, FileText, FlaskConical, House,
  MapPin, Package, Receipt, ShieldCheck, Stethoscope, UserRound, type LucideIcon,
} from "lucide-react";
import { documentType, kindColor, nodeKind, nodeLabelText, type Kind } from "@/lib/categories";
import { cn } from "@/lib/utils";

// Each family has its own shape in a 20 x 20 box, so no two kinds share one (open question 5 in the redesign issue).
const SHAPES: Record<Kind, string> = {
  people: "M10 2.5a7.5 7.5 0 1 0 0 15a7.5 7.5 0 1 0 0-15z",
  org: "M6.36 3.5h7.28a2.86 2.86 0 0 1 2.86 2.86v7.28a2.86 2.86 0 0 1-2.86 2.86H6.36a2.86 2.86 0 0 1-2.86-2.86V6.36A2.86 2.86 0 0 1 6.36 3.5z",
  place: "M10 2L18 10L10 18L2 10z",
  money: "M17.5 10L13.75 16.5H6.25L2.5 10L6.25 3.5H13.75z",
  agree: "M10 3.1L17.13 8.28L14.41 16.67H5.59L2.87 8.28z",
  thing: "M10 2.5L17.5 16H2.5z",
  health: "M7.5 2.5h5v5h5v5h-5v5h-5v-5h-5v-5h5z",
  time: "M10 2.8L11.88 8.01L17.42 8.19L13.04 11.59L14.58 16.91L10 13.8L5.42 16.91L6.96 11.59L2.58 8.19L8.12 8.01z",
  docs: "M4.5 2.5h7l4 4v11h-11z",
  unknown: "M10 3a7 7 0 1 0 0 14a7 7 0 1 0 0-14z",
};

export const KIND_ICONS: Record<string, LucideIcon> = {
  Person: UserRound, Organization: Building2, Location: MapPin, Address: House, FinancialItem: Receipt,
  Contract: FileSignature, InsurancePolicy: ShieldCheck, Product: Package, System: Cpu, Condition: Stethoscope,
  MedicalResult: FlaskConical, Event: Calendar, DateEvent: CalendarClock, Document: FileText, DocumentRef: FileQuestion,
};

/** A node kind as its shape (or icon) plus its text label. Faded swaps the color, never the opacity. */
export function KindShape({ label, icon = false, faded = false, className }: { label: string; icon?: boolean; faded?: boolean; className?: string }) {
  const { kind, outlined } = nodeKind(label);
  const color = faded ? "var(--graph-faded)" : kindColor(kind);
  const Icon = kind === "unknown" ? CircleHelp : KIND_ICONS[label];
  return (
    <span data-kind={kind} data-outlined={outlined || undefined} className={cn("inline-flex items-center gap-1.5 label-13 text-fg", className)}>
      {icon ? <Icon aria-hidden className="size-4" strokeWidth={1.75} style={{ color }} /> : (
        <svg aria-hidden viewBox="0 0 20 20" className="size-4 shrink-0">
          {kind === "unknown"
            ? <path d={SHAPES.unknown} fill="none" stroke={color} strokeWidth={2} strokeDasharray="3 2.5" />
            : <path d={SHAPES[kind]} fill={outlined ? "var(--bg)" : color} stroke={outlined ? color : "none"} strokeWidth={2} strokeLinejoin="round" />}
          {kind === "docs" && <path d="M11.5 2.5v4h4" fill="none" stroke={outlined ? color : "var(--bg)"} strokeWidth={outlined ? 2 : 1.25} strokeLinejoin="round" />}
        </svg>
      )}
      {nodeLabelText(label)}
    </span>
  );
}

/** A document type as a colored dot with --fg-muted text, never colored text. */
export function TypeDot({ type, className }: { type: string; className?: string }) {
  const { label, color } = documentType(type);
  return (
    <span data-type={type} className={cn("inline-flex items-center gap-1.5 label-13 text-fg-muted", className)}>
      <span aria-hidden className="size-2 shrink-0 rounded-full" style={{ background: color }} />
      {label}
    </span>
  );
}
