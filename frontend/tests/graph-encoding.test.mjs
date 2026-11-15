import { test } from "node:test";
import assert from "node:assert/strict";
import {
  labelCandidates,
  linkWidth,
  nodeLabel,
  nodeRadius,
  placeLabels,
  shapePath,
} from "../.test-build/graph-encoding.js";

test("node radius grows with the log of evidence documents between 6 and 18 px", () => {
  assert.equal(nodeRadius(0), 6);
  assert.equal(nodeRadius(1), 8.5);
  assert.equal(nodeRadius(3), 11);
  assert.equal(nodeRadius(120), 18);
});

test("link width grows with the log of supporting documents between 1 and 4 px", () => {
  assert.equal(linkWidth(0), 1);
  assert.equal(linkWidth(1), 1);
  assert.equal(linkWidth(4), 2.5);
  assert.equal(linkWidth(64), 4);
});

test("labels cut at 28 characters, and a node without a name never shows its id", () => {
  assert.equal(nodeLabel("Harbor Mutual Insurance", "a", "Organization"), "Harbor Mutual Insurance");
  assert.equal(nodeLabel("A".repeat(40), "a", "Organization"), `${"A".repeat(27)}…`);
  const uuid = "3f2a1c9e-0000-4000-8000-000000000001";
  for (const name of ["", uuid]) {
    const text = nodeLabel(name, uuid, "InsurancePolicy");
    assert.equal(text, "Insurance policy 3f2a1c");
    assert.ok(!text.includes(uuid));
  }
  assert.equal(nodeLabel("", "doc-4120", "Document"), "Document 4120");
});

test("every kind has a closed shape path", () => {
  for (const label of ["Person", "Organization", "Location", "FinancialItem", "Contract", "Product", "Condition",
    "Event", "Document", "Gadget"]) assert.match(shapePath(label, 10), /^M.*Z$/);
});

test("a label skips a candidate under a pill and takes the next free one", () => {
  const candidates = labelCandidates(100, 0, 8, 60, [1, 0]);
  assert.ok(candidates[0].x0 > 100, "The first candidate is on the outer side");
  const pill = { x0: 110, y0: -12, x1: 260, y1: 12 };
  const [box] = placeLabels([{ candidates, rule: "optional" }], [pill]);
  assert.ok(box, "A free candidate exists");
  assert.ok(box.x1 <= pill.x0 || box.x0 >= pill.x1 || box.y1 <= pill.y0 || box.y0 >= pill.y1, "The label is clear of the pill");
});

test("crowded labels yield by rule: optional skips, all never covers a pill, force always draws", () => {
  const candidates = labelCandidates(0, 0, 8, 60, null);
  const cover = { x0: -500, y0: -500, x1: 500, y1: 500 };
  assert.deepEqual(placeLabels(["optional", "all", "force"].map(rule => ({ candidates, rule })), [cover]),
    [null, null, candidates[0]]);
});

test("an all-nodes label may overlap another label but stays inside the canvas", () => {
  const candidates = labelCandidates(0, 0, 8, 60, null);
  const bounds = { x0: -100, y0: -100, x1: 100, y1: 100 };
  const [first, second] = placeLabels(["all", "all"].map(rule => ({ candidates, rule })), [], bounds);
  assert.ok(first && second && first !== second, "Two labels take two free candidates");
  for (const box of [first, second]) assert.ok(box.x0 >= -100 && box.x1 <= 100 && box.y0 >= -100 && box.y1 <= 100);
  const outside = labelCandidates(95, 0, 8, 60, [1, 0]);
  assert.ok(outside[0].x1 > 100);
  const [moved] = placeLabels([{ candidates: outside, rule: "all" }], [], bounds);
  assert.ok(moved && moved.x1 <= 100, "A label at the edge moves inside");
});
