/** Node kinds and document types from docs/design/categories.md; colors are the --cat-* and --doc-* tokens in globals.css. */
export type Kind = "people" | "org" | "place" | "money" | "agree" | "thing" | "health" | "time" | "docs" | "unknown";

// Per label: its kind and whether it is the family's second, outlined type.
const LABELS = new Map(Object.entries<{ kind: Kind; outlined: boolean }>({
  Person: { kind: "people", outlined: false },
  Organization: { kind: "org", outlined: false },
  Location: { kind: "place", outlined: false },
  Address: { kind: "place", outlined: true },
  FinancialItem: { kind: "money", outlined: false },
  Contract: { kind: "agree", outlined: false },
  InsurancePolicy: { kind: "agree", outlined: true },
  Product: { kind: "thing", outlined: false },
  System: { kind: "thing", outlined: true },
  Condition: { kind: "health", outlined: false },
  MedicalResult: { kind: "health", outlined: true },
  Event: { kind: "time", outlined: false },
  DateEvent: { kind: "time", outlined: true },
  Document: { kind: "docs", outlined: false },
  DocumentRef: { kind: "docs", outlined: true },
}));

export const NODE_LABELS = [...LABELS.keys()];

/** Derive a node's kind and outline from its label; the server does not send them. */
export function nodeKind(label: string): { kind: Kind; outlined: boolean } {
  return LABELS.get(label) ?? { kind: "unknown", outlined: false };
}

export function kindColor(kind: Kind): string {
  return kind === "unknown" ? "var(--fg-subtle)" : `var(--cat-${kind})`;
}

const sentenceCase = (words: string) => words.charAt(0).toUpperCase() + words.slice(1).toLowerCase();

/** "InsurancePolicy" becomes "Insurance policy". */
export function nodeLabelText(label: string): string {
  return sentenceCase(label.replace(/([a-z])([A-Z])/g, "$1 $2"));
}

const DOCUMENT_TYPES = new Map(Object.entries({
  financial_invoice: "Invoice",
  government_tax: "Tax",
  property_home: "Home",
  insurance: "Insurance",
  military: "Military",
  legal_contract: "Legal",
  personal: "Personal",
  work: "Work",
  medical_lab: "Medical",
}));

export const DOCUMENT_TYPE_KEYS = [...DOCUMENT_TYPES.keys()];

/** A type outside the nine keys uses --fg-subtle and its key in sentence case. */
export function documentType(key: string): { label: string; color: string } {
  const label = DOCUMENT_TYPES.get(key);
  if (label) return { label, color: `var(--doc-${key.replace(/_/g, "-")})` };
  return { label: sentenceCase(key.replace(/_/g, " ")), color: "var(--fg-subtle)" };
}
