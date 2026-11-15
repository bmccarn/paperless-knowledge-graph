/** Claim offsets and Markdown blocks for the answer prose. */

export interface LedgerClaim {
  id?: string;
  claim?: string;
  status?: string;
  start?: number;
  end?: number;
  references?: Array<{ document_id: number; quote: string; source_title?: string }>;
}

/** The UTF-16 index of a code-point offset: the ledger counts code points, JavaScript strings count UTF-16 units. */
export function utf16Index(text: string, codePoints: number): number {
  let index = 0;
  for (let n = 0; n < codePoints && index < text.length; n++) index += text.codePointAt(index)! > 0xffff ? 2 : 1;
  return index;
}

/** A claim's UTF-16 range and its number (ledger order, from 1). */
export interface ClaimRange { n: number; start: number; end: number }

/** Ranges in ledger order; a claim with offsets outside the text or overlapping an earlier claim is left plain. */
export function claimRanges(text: string, claims: LedgerClaim[]): ClaimRange[] {
  const length = [...text].length;
  const ranges: ClaimRange[] = [];
  claims.forEach((claim, i) => {
    const { start, end } = claim;
    if (!Number.isInteger(start) || !Number.isInteger(end) || start! < 0 || end! <= start! || end! > length) return;
    const range = { n: i + 1, start: utf16Index(text, start!), end: utf16Index(text, end!) };
    if (ranges.every(other => range.end <= other.start || range.start >= other.end)) ranges.push(range);
  });
  return ranges.sort((a, b) => a.start - b.start);
}

/** Text inside one block; `claim` is the claim number, `last` marks the piece that ends the claim. */
export interface Piece { text: string; claim?: number; last?: boolean }
export type BlockKind = "h2" | "h3" | "ul" | "ol" | "p" | "code" | "blank";
export interface Block { kind: BlockKind; pieces: Piece[] }

/**
 * Split the text into Markdown line blocks, then cut each block's content at claim boundaries.
 * Formatting is applied to each piece afterwards, so an underline never splits a bold pair or a heading.
 */
export function proseBlocks(text: string, ranges: ClaimRange[]): Block[] {
  const blocks: Block[] = [];
  let code: string[] | null = null;
  let offset = 0;
  for (const line of text.split("\n")) {
    const lineStart = offset;
    offset += line.length + 1;
    if (line.startsWith("```")) {
      if (code) blocks.push({ kind: "code", pieces: [{ text: code.join("\n") }] });
      code = code ? null : [];
      continue;
    }
    if (code) { code.push(line); continue; }
    if (line.trim() === "") { blocks.push({ kind: "blank", pieces: [] }); continue; }
    const marker = /^(?:#{1,3} |[-*] |\d+\.\s)/.exec(line)?.[0] ?? "";
    const kind: BlockKind = marker.startsWith("###") ? "h3" : marker.startsWith("#") ? "h2"
      : marker === "- " || marker === "* " ? "ul" : marker ? "ol" : "p";
    let at = lineStart + marker.length;
    const end = lineStart + line.length;
    const pieces: Piece[] = [];
    for (const range of ranges) {
      if (range.end <= at || range.start >= end) continue;
      if (range.start > at) pieces.push({ text: text.slice(at, range.start) });
      const stop = Math.min(range.end, end);
      pieces.push({ text: text.slice(Math.max(at, range.start), stop), claim: range.n });
      at = stop;
    }
    if (at < end) pieces.push({ text: text.slice(at, end) });
    blocks.push({ kind, pieces });
  }
  if (code) blocks.push({ kind: "code", pieces: [{ text: code.join("\n") }] });
  // The number and the note follow the claim's final visible piece.
  const seen = new Set<number>();
  for (const block of [...blocks].reverse()) for (const piece of [...block.pieces].reverse()) {
    if (piece.claim && !seen.has(piece.claim)) { seen.add(piece.claim); piece.last = true; }
  }
  return blocks;
}

// Values a claim states, most telling first: amounts, quoted names, dates, then other numbers.
const MONTH = "(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\\.?";
const VALUE_PATTERNS = [
  /\$\s?\d+(?:,\d{3})*(?:\.\d+)?/g,
  /["“]([^"”\n]{2,80})["”]/g,
  new RegExp(`${MONTH} \\d{1,2}(?:, \\d{4})?|\\d{1,2} ${MONTH} \\d{4}|\\d{4}-\\d{2}-\\d{2}|\\d{1,2}/\\d{1,2}/\\d{2,4}`, "g"),
  // Single digits only as a percentage: alone they also number lists and sections.
  /\d[\d,.]*\d%?|\d%/g,
];
/** Characters of context kept before the first matched value, so it falls inside a three-line quote. */
const CONTEXT = 40;
const escape = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
/** A value as a whole token: "$25" must not match inside "$250" or "$25.50". */
const valuePattern = (values: string[]) =>
  new RegExp(`(?<![\\w$])(?:${values.map(escape).join("|")})(?![\\w]|[.,]\\d)`, "gi");

/**
 * The values a claim states that also appear in a quote, and where a short view of the quote should start:
 * a word boundary just before the first match of the highest-ranked value, or 0 when nothing matches.
 * `rank` is that value's kind (0 for an amount), or the number of kinds when nothing matches.
 */
export function quoteWindow(claim: string, quote: string): { start: number; values: string[]; rank: number } {
  let rest = claim;
  const found: string[] = [];
  let anchor = -1;
  let rank = VALUE_PATTERNS.length;
  for (const [kindIndex, pattern] of VALUE_PATTERNS.entries()) {
    const kind: string[] = [];
    // Blank each value out so a later pattern does not take its digits again.
    rest = rest.replace(pattern, (match, quoted?: string) => {
      kind.push(typeof quoted === "string" ? quoted : match);
      return " ".repeat(match.length);
    });
    const present = kind.filter(value => valuePattern([value]).test(quote));
    if (anchor < 0 && present.length) {
      anchor = Math.min(...present.map(value => quote.search(valuePattern([value]))));
      rank = kindIndex;
    }
    found.push(...present);
  }
  if (anchor <= CONTEXT) return { start: 0, values: found, rank };
  const space = quote.indexOf(" ", anchor - CONTEXT);
  return { start: space >= 0 && space < anchor ? space + 1 : anchor, values: found, rank };
}

/**
 * A passage as readable text: indexed passages can hold Markdown and HTML from text extraction.
 * Drops tags, bold markers, rules, heading marks and table rules; table cells become " · "-separated.
 */
export function plainQuote(text: string): string {
  return text
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<\/?[a-z][^>]*>/gi, "")
    .replace(/&nbsp;/g, " ").replace(/&amp;/g, "&")
    .replace(/^[ \t]*\|?(?:[ \t]*:?-{3,}:?[ \t]*\|)+[ \t]*:?-*:?[ \t]*$/gm, "")
    .replace(/^[ \t]*(?:-{3,}|\*{3,}|_{3,})[ \t]*$/gm, "")
    .replace(/^[ \t]*#{1,6}[ \t]+/gm, "")
    .replace(/\*\*|__/g, "")
    .replace(/^[ \t]*\|[ \t]*|[ \t]*\|[ \t]*$/gm, "")
    .replace(/\s*\|\s*/g, " · ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Split text into plain and matched pieces for emphasis. */
export function emphasize(text: string, values: string[]): Array<{ text: string; match?: boolean }> {
  if (!values.length) return [{ text }];
  const pieces: Array<{ text: string; match?: boolean }> = [];
  let at = 0;
  for (const match of text.matchAll(valuePattern(values))) {
    if (match.index > at) pieces.push({ text: text.slice(at, match.index) });
    pieces.push({ text: match[0], match: true });
    at = match.index + match[0].length;
  }
  if (at < text.length) pieces.push({ text: text.slice(at) });
  return pieces;
}
