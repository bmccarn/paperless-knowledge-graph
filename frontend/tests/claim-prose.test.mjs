import test from "node:test";
import assert from "node:assert/strict";
import { claimRanges, proseBlocks, utf16Index } from "../.test-build/claim-prose.js";

// The backend counts offsets in code points (Python len); "🏠" is one code point and two UTF-16 units.
const TEXT = "The policy covers the house on Elm Street 🏠.\n\n## Premium\n**Amount:** The January statement lists a premium of $25.";
const CLAIMS = [
  { claim: "The policy covers the house on Elm Street 🏠.", start: 0, end: 44 },
  { claim: "## Premium\n**Amount:** The January statement lists a premium of $25.", start: 46, end: 114 },
];

test("a code-point offset after a character outside the Basic Multilingual Plane converts to UTF-16", () => {
  assert.equal(utf16Index(TEXT, 46), 47);
  const [, second] = claimRanges(TEXT, CLAIMS);
  assert.equal(TEXT.slice(second.start, second.end), CLAIMS[1].claim);
  // Without the conversion the slice is off by one unit.
  assert.notEqual(TEXT.slice(46, 114), CLAIMS[1].claim);
});

test("claims keep ledger numbers; invalid and overlapping offsets stay plain", () => {
  const ranges = claimRanges(TEXT, [CLAIMS[1], { start: 3, end: 2 }, { start: 0, end: 999 }, { start: 50, end: 60 }, CLAIMS[0]]);
  assert.deepEqual(ranges.map(range => range.n), [5, 1]);
});

test("a claim under a heading and a bold label is cut per block, after the Markdown markers", () => {
  const blocks = proseBlocks(TEXT, claimRanges(TEXT, CLAIMS));
  assert.deepEqual(blocks.map(block => block.kind), ["p", "blank", "h2", "p"]);
  assert.deepEqual(blocks[0].pieces, [{ text: CLAIMS[0].claim, claim: 1, last: true }]);
  assert.deepEqual(blocks[2].pieces, [{ text: "Premium", claim: 2 }]);
  // The bold pair stays whole inside one piece, so formatting can pair its asterisks.
  assert.deepEqual(blocks[3].pieces, [{ text: "**Amount:** The January statement lists a premium of $25.", claim: 2, last: true }]);
});

test("text around claims, list items and code stay plain", () => {
  const text = "Intro. Claim one.\n- Item claim.\n```\ncode\n```";
  const ranges = [{ n: 1, start: 7, end: 17 }, { n: 2, start: 18, end: 31 }];
  const blocks = proseBlocks(text, ranges);
  assert.deepEqual(blocks.map(block => block.kind), ["p", "ul", "code"]);
  assert.deepEqual(blocks[0].pieces, [{ text: "Intro. " }, { text: "Claim one.", claim: 1, last: true }]);
  assert.deepEqual(blocks[1].pieces, [{ text: "Item claim.", claim: 2, last: true }]);
  assert.deepEqual(blocks[2].pieces, [{ text: "code" }]);
});

test("no claims gives the plain Markdown blocks", () => {
  const blocks = proseBlocks("# Title\n1. One\n\nPlain **bold**.", []);
  assert.deepEqual(blocks.map(block => [block.kind, block.pieces.map(piece => piece.text).join("")]),
    [["h2", "Title"], ["ol", "One"], ["blank", ""], ["p", "Plain **bold**."]]);
});
