import { test } from "node:test";
import assert from "node:assert/strict";
import {
  askAbout,
  canvasName,
  documentsByDate,
  entityName,
  keyFacts,
  legendItems,
  linksByFamily,
  linksOfDocument,
  lowestConfidence,
  scoreText,
  shortDate,
  supportSummary,
  supportText,
  typeWords,
} from "../.test-build/graph-inspector.js";
import { neighborhoodLayout } from "../.test-build/graph-layout.js";

function link(family, type, neighbor, docs, extra = {}) {
  const inferred = extra.inferred ?? [];
  const records = docs.map(doc => ({ doc_id: doc, inferred: inferred.includes(doc), confidence: 0.9 }));
  const count = inferred.length;
  return {
    key: `focus~${type}~${neighbor}`, type, family, direction: "out",
    support_state: count === 0 ? "supported" : count === docs.length ? "inferred" : "mixed",
    stated_count: docs.length - count, inferred_count: count, records,
    neighbor: { id: neighbor, label: extra.label || "Organization", name: extra.name ?? neighbor,
                evidence_doc_count: docs.length, link_count: 1, is_owner: false },
  };
}
const summary = (links, documents = {}) => ({
  focus: { id: "focus", label: "Person", name: "Focus", aliases: [], evidence_doc_count: 0, link_count: links.length,
           is_hub: false, is_owner: false },
  links, documents, owner_status: "found", owner_duplicates: [],
});
const doc = (title, date, doc_type = "insurance") => ({ title, date, doc_type });

test("support words: supported, inferred, and stated and inferred counts for a mixed link", () => {
  assert.equal(supportText({ support_state: "supported", stated_count: 3, inferred_count: 0 }), "Supported");
  assert.equal(supportText({ support_state: "inferred", stated_count: 0, inferred_count: 2 }), "Inferred");
  assert.equal(supportText({ support_state: "mixed", stated_count: 1, inferred_count: 1 }), "Stated in 1, inferred in 1");
  assert.equal(typeWords("PREMIUM_PAID_TO"), "Premium paid to");
  assert.equal(shortDate("2026-04-01"), "1 Apr 2026");
  assert.equal(shortDate(null), "");
});

test("documents are newest first, undated last, with a stable order for equal dates", () => {
  const data = summary([], { 3: doc("B", "2026-09-01"), 1: doc("A", "2026-09-01"), 2: doc("Old", "2024-01-05"),
    4: doc("", null), 5: doc("A", "2026-09-01") });
  assert.deepEqual(documentsByDate(data).map(item => item.id), [1, 5, 3, 2, 4]);
  assert.equal(documentsByDate(data).at(-1).title, "Untitled document");
});

test("support summary: span, same-day and undated forms, and the count of inferred links", () => {
  const links = [link("parties", "EMPLOYED_BY", "a", [1], { inferred: [1] }), link("money", "PAID_TO", "b", [2, 3], { inferred: [3] })];
  assert.equal(supportSummary(summary(links, { 1: doc("x", "2024-03-03"), 2: doc("y", "2026-09-02"), 3: doc("z", null) })),
    "Seen in 3 documents from 3 Mar 2024 to 2 Sep 2026. 1 link is inferred.");
  assert.equal(supportSummary(summary([], { 1: doc("x", "2026-09-01"), 2: doc("y", "2026-09-01") })),
    "Seen in 2 documents, all dated 1 Sep 2026.");
  assert.equal(supportSummary(summary([], { 1: doc("x", "2026-09-01"), 2: doc("y", null) })),
    "Seen in 2 documents, dated 1 Sep 2026 or undated.");
  assert.equal(supportSummary(summary([], { 1: doc("x", null) })), "Seen in 1 document.");
});

test("links by family follow sector order, rank by documents, skip evidence and documents, and fold unknown families into Other", () => {
  const data = summary([
    link("money", "PAID_TO", "m1", [1]),
    link("parties", "EMPLOYED_BY", "p1", [2]),
    link("parties", "SIGNED_BY", "p2", [3, 4, 5]),
    link("evidence", "MENTIONS", "doc-9", [6], { label: "Document" }),
    link("doclinks", "PREPARED_BY", "doc-7", [7], { label: "Document" }),
    link("mystery", "ODD_LINK", "o1", [8], { inferred: [8] }),
  ]);
  const groups = linksByFamily(data);
  assert.deepEqual(groups.map(group => group.family), ["parties", "money", "other"]);
  assert.deepEqual(groups[0].links.map(item => item.neighbor.id), ["p2", "p1"]);
  assert.equal(groups[2].inferred, 1);
  assert.deepEqual(keyFacts(data).map(item => item.neighbor.id), ["p2", "p1", "m1"]);
});

test("key facts: supported before mixed before inferred, then more documents, one per type, never a DocumentRef", () => {
  const data = summary([
    link("parties", "SIGNED_BY", "signature", [1, 2, 3, 4, 5, 6], { label: "DocumentRef" }),
    link("money", "BILLED_TO", "billing-1", [7, 8, 9, 10], { inferred: [7, 8, 9, 10] }),
    link("money", "BILLED_TO", "billing-2", [11, 12, 13], { inferred: [11, 12, 13] }),
    link("coverage", "COVERS", "policy", [14, 15, 16], { inferred: [14] }),
    link("parties", "EMPLOYED_BY", "employer", [17, 18]),
    link("parties", "EMPLOYED_BY", "employer-2", [19, 20, 21]),
    link("time", "EFFECTIVE_FROM", "start", [22]),
    link("loose", "RELATED_TO", "friend", [23, 24, 25, 26, 27]),
  ]);
  assert.deepEqual(keyFacts(data).map(item => item.neighbor.id), ["employer-2", "start", "policy", "billing-1"]);
  // Four types fill the four rows when they exist.
  const many = summary(["A_BY", "B_BY", "C_BY", "D_BY", "E_BY"].map((type, index) => link("parties", type, `n${index}`, [index + 1])));
  assert.deepEqual(keyFacts(many).map(item => item.type), ["A_BY", "B_BY", "C_BY", "D_BY"]);
});

test("a document highlights the links its records cite", () => {
  const data = summary([link("parties", "A_BY", "a", [1, 2]), link("money", "PAID_TO", "b", [2]), link("time", "ON_DATE", "c", [3])]);
  assert.deepEqual([...linksOfDocument(data, 2)].sort(), ["focus~A_BY~a", "focus~PAID_TO~b"]);
});

test("an unnamed entity shows its kind and a short id, never the id", () => {
  assert.equal(entityName({ id: "4f0c2a9e-1b2c-4d5e-8f90-123456789abc", name: "", label: "InsurancePolicy" }), "Insurance policy 4f0c2a");
  assert.equal(entityName({ id: "x", name: "Harbor", label: "Organization" }), "Harbor");
  assert.equal(askAbout("Harbor"), "What do my documents say about Harbor?");
});

test("the legend lists only the drawn kinds and link styles", () => {
  const links = [
    link("parties", "EMPLOYED_BY", "a", [1], { label: "Organization" }),
    link("parties", "SIGNED_BY", "b", [2], { label: "Person", inferred: [2] }),
    link("other", "ODD", "c", [3], { label: "Gadget" }),
    link("other", "ODDER", "d", [4], { label: "Widget" }),
  ];
  const all = legendItems(neighborhoodLayout(summary(links), 6));
  assert.deepEqual(all, { labels: ["Person", "Organization", "Other"], styles: ["supported", "inferred"] });
  // A cap of 1 hides links into pills: the aggregate style appears, the hidden kinds do not.
  const capped = legendItems(neighborhoodLayout(summary(links.slice(0, 2)), 1));
  assert.deepEqual(capped, { labels: ["Person", "Organization"], styles: ["supported", "aggregate"] });
});

test("the canvas name counts drawn-family links and only fully inferred ones; a mixed link is not inferred", () => {
  const links = [link("parties", "EMPLOYED_BY", "a", [1], { inferred: [1] }), link("money", "PAID_TO", "b", [2, 3], { inferred: [3] }),
    link("money", "OWES", "c", [4]), link("evidence", "MENTIONS", "d", [5], { inferred: [5] })];
  const data = summary(links);
  assert.equal(canvasName(data, neighborhoodLayout(data, 6)), "Neighborhood of Focus: 3 links in 2 families, 1 inferred");
  assert.equal(canvasName(data, neighborhoodLayout(data, 6), 11), "Neighborhood of Focus: 3 links in 2 families, 1 inferred; 11 links between neighbors");
  assert.equal(canvasName(data, neighborhoodLayout(data, 6), 1), "Neighborhood of Focus: 3 links in 2 families, 1 inferred; 1 link between neighbors");
  const one = summary([link("parties", "EMPLOYED_BY", "a", [1])]);
  assert.equal(canvasName(one, neighborhoodLayout(one, 6)), "Neighborhood of Focus: 1 link in 1 family, 0 inferred");
});

test("a link's score: the lowest record confidence, or No score when any record or every record is missing", () => {
  const scored = link("coverage", "COVERS", "car", [1, 2]);
  scored.records[1].confidence = 0.62;
  assert.equal(scoreText("Lowest confidence", lowestConfidence(scored)), "Lowest confidence 0.62");
  scored.records[0].confidence = null;
  assert.equal(scoreText("Lowest confidence", lowestConfidence(scored)), "No score");
  assert.equal(lowestConfidence({ records: [] }), null);
  assert.equal(scoreText("Confidence", 0.9), "Confidence 0.90");
});
