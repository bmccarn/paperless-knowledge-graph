import test from "node:test";
import assert from "node:assert/strict";
import { claimRanges, emphasize, plainQuote, proseBlocks, quoteWindow, utf16Index } from "../.test-build/claim-prose.js";

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

// A whole indexed chunk: the header repeats the claim's dates, the amount comes later.
const PASSAGE = "Auto Policy Declarations Policy number: CX-AUTO-440192 Named insured: Jordan Example Policy period: "
  + "March 1, 2025 to March 1, 2026. Liability: $100,000 each person. Collision deductible: $500. Six-month premium: $612.00. Due on the effective date.";
const CLAIM = "The declarations for the March 1, 2025 to March 1, 2026 term list a six-month premium of $612.00.";

test("the quote window starts shortly before the amount the claim states, not at the dates in the header", () => {
  const { start, values } = quoteWindow(CLAIM, PASSAGE);
  const shown = PASSAGE.slice(start);
  assert.ok(start > 0 && PASSAGE[start - 1] === " ", "starts on a word boundary");
  assert.ok(shown.indexOf("$612.00") <= 40, "the amount is near the start of the window");
  assert.deepEqual(values, ["$612.00", "March 1, 2025", "March 1, 2026"]);
  // An amount outranks a date, so a passage with only the dates ranks below this one.
  assert.equal(quoteWindow(CLAIM, PASSAGE).rank, 0);
  assert.equal(quoteWindow(CLAIM, "Effective March 1, 2025. Expires March 1, 2026.").rank, 2);
});

test("values match whole tokens only", () => {
  assert.deepEqual(quoteWindow("The fee is $25.", "Late fee $250. Other fee $25.50.").values, []);
  assert.deepEqual(quoteWindow("The fee is $25.", "Late fee $250. Fee: $25.").values, ["$25"]);
  // A comma after an amount ends it; a percentage keeps a single digit.
  assert.deepEqual(quoteWindow("It rose from $500, then to $1,000.", "Deductible: $500. Now $1,000.").values, ["$500", "$1,000"]);
  assert.deepEqual(quoteWindow("The rate rose by 5%.", "Rate change: 5% from March.").values, ["5%"]);
  // Single digits are too common to anchor on.
  assert.deepEqual(quoteWindow("Invoice 88-2031 lists 3 items.", "Invoice 88-2031, 3 items").values, ["88", "2031"]);
});

test("a quoted name is a value; with no match the window starts at the beginning", () => {
  const passage = "x ".repeat(40) + "Product: Cordless drill kit, model CT-18V.";
  assert.deepEqual(quoteWindow('The warranty names "Cordless drill kit".', passage).values, ["Cordless drill kit"]);
  assert.ok(quoteWindow('The warranty names "Cordless drill kit".', passage).start > 0);
  assert.deepEqual(quoteWindow("The policy covers the house.", PASSAGE), { start: 0, values: [], rank: 4 });
});

test("emphasize splits the text around each matched value, case-insensitively", () => {
  assert.deepEqual(emphasize("Premium: $612.00, due March 1, 2025.", ["$612.00", "march 1, 2025"]), [
    { text: "Premium: " }, { text: "$612.00", match: true }, { text: ", due " }, { text: "March 1, 2025", match: true }, { text: "." },
  ]);
  assert.deepEqual(emphasize("No values here.", []), [{ text: "No values here." }]);
});

test("a quote from a passage with table pipes, bold markers, rules and line-break tags shows none of them", () => {
  const passage = "## Results\n| Test | Value | Unit |\n|---|:---:|---|\n| **Glucose** | 95 | mg/dL |\n---\nCollected<br>2026-03-01 &amp; reviewed<br/>by <b>Example Lab</b>.";
  const quote = plainQuote(passage);
  assert.equal(quote, "Results Test · Value · Unit Glucose · 95 · mg/dL Collected 2026-03-01 & reviewed by Example Lab.");
  assert.doesNotMatch(quote, /\||\*\*|<|---/);
  // Value emphasis still finds the claim's values in the cleaned text.
  const { values } = quoteWindow("Glucose was 95 mg/dL on 2026-03-01.", quote);
  assert.deepEqual(emphasize(quote, values).filter(piece => piece.match).map(piece => piece.text), ["95", "2026-03-01"]);
});

test("a block tag between two values keeps them apart, and an inline tag joins its text", () => {
  assert.equal(plainQuote("<p>Total: $100.00</p><p>2026 statement</p>"), "Total: $100.00 2026 statement");
  assert.equal(plainQuote("Premium: <strong>$1,284</strong>.00 per <em>year</em>"), "Premium: $1,284.00 per year");
});
