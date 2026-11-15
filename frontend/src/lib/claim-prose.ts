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
