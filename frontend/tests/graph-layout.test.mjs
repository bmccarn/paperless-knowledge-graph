import { test } from "node:test";
import assert from "node:assert/strict";
import {
  drawableLinks,
  familyCap,
  neighborhoodLayout,
  placeNeighbors,
  showMore,
} from "../.test-build/graph-layout.js";

const FAMILY_TYPES = { parties: "PARTY_TO", money: "PAID_TO", places: "LIVES_IN", health: "PATIENT_OF" };
let docId = 1;
function link(family, neighbor, documents, extra = {}) {
  const records = Array.from({ length: documents }, () => ({ doc_id: docId++, inferred: false, confidence: 0.9 }));
  const type = extra.type || FAMILY_TYPES[family];
  return {
    key: `focus~${type}~${neighbor}`, type, family, direction: "out", support_state: "supported",
    stated_count: documents, inferred_count: 0, records,
    neighbor: { id: neighbor, label: extra.label || "Organization", name: extra.name || neighbor,
                evidence_doc_count: documents, link_count: 1, is_owner: false },
  };
}
function summary(links, isHub = false) {
  const documents = {};
  for (const item of links) for (const record of item.records)
    documents[record.doc_id] = { title: `Doc ${record.doc_id}`, date: "2026-01-01", doc_type: "invoice" };
  return {
    focus: { id: "focus", label: "Person", name: "Focus", aliases: [], evidence_doc_count: 1,
             link_count: links.length, is_hub: isHub, is_owner: false },
    links, documents, owner_status: "found",
  };
}
// A hub: 60 parties, 120 money and 3 places links, a two-family neighbor and evidence that is never drawn.
function hub() {
  docId = 1;
  const links = [];
  for (let i = 0; i < 60; i++) links.push(link("parties", `party-${String(i).padStart(2, "0")}`, 1 + (i % 4)));
  for (let i = 0; i < 120; i++) links.push(link("money", `money-${String(i).padStart(3, "0")}`, 1));
  for (let i = 0; i < 3; i++) links.push(link("places", `place-${i}`, 2));
  links.push(link("parties", "two-family", 2, { type: "ISSUED_BY" }), link("money", "two-family", 5, { type: "PREMIUM_PAID_TO" }));
  links.push(link("evidence", "doc-1", 1, { type: "MENTIONS", label: "Document" }));
  links.push(link("doclinks", "doc-2", 1, { type: "REFERENCES", label: "Document" }));
  return summary(links, true);
}
const familyTotals = data => drawableLinks(data).reduce((totals, item) =>
  ({ ...totals, [item.family]: (totals[item.family] || 0) + 1 }), {});

test("caps: a hub keeps 2 per family, others 6, or 5 on phone", () => {
  assert.equal(familyCap(true, false), 2);
  assert.equal(familyCap(true, true), 2);
  assert.equal(familyCap(false, false), 6);
  assert.equal(familyCap(false, true), 5);
  const layout = neighborhoodLayout(hub(), familyCap(true, false));
  for (const sector of layout.sectors) assert.ok(sector.shown <= 2, `${sector.family} shows ${sector.shown}`);
  const perFamily = {};
  for (const node of layout.nodes.filter(node => node.kind === "neighbor")) perFamily[node.family] = (perFamily[node.family] || 0) + 1;
  assert.deepEqual(perFamily, { parties: 2, money: 2, places: 2 });
});

test("pill counts plus drawn links equal the family link totals", () => {
  const data = hub();
  for (const extra of [{}, { parties: 25 }, { parties: 50, money: 25 }]) {
    const layout = neighborhoodLayout(data, 2, extra);
    const drawn = {};
    for (const item of layout.links.filter(item => item.link)) drawn[item.family] = (drawn[item.family] || 0) + 1;
    const pills = Object.fromEntries(layout.nodes.filter(node => node.kind === "pill").map(node => [node.family, node.count]));
    for (const [family, total] of Object.entries(familyTotals(data)))
      assert.equal((drawn[family] || 0) + (pills[family] || 0), total, `${family} with ${JSON.stringify(extra)}`);
  }
  // One pill per family with hidden links, and none for a family that is fully drawn.
  const full = neighborhoodLayout(data, 6);
  assert.deepEqual(full.nodes.filter(node => node.kind === "pill").map(node => node.name), ["+55 Parties and roles", "+115 Money and accounts"]);
});

test("one press adds up to 25 and never passes 150 entities in view", () => {
  const data = hub();
  const once = showMore(data, 2, {}, "parties");
  assert.deepEqual(once, { parties: 25 });
  assert.equal(neighborhoodLayout(data, 2, once).sectors.find(s => s.family === "parties").shown, 27);
  // 60 parties members: the third press adds the last 8.
  const thrice = showMore(data, 2, showMore(data, 2, once, "parties"), "parties");
  assert.equal(neighborhoodLayout(data, 2, thrice).sectors.find(s => s.family === "parties").shown, 60);
  assert.equal(showMore(data, 2, thrice, "parties"), thrice, "nothing left to add");
  // 1 focus + 60 parties + 2 places + 2 money = 65; three money presses reach 140, the fourth adds 10.
  let extra = thrice;
  for (let i = 0; i < 3; i++) extra = showMore(data, 2, extra, "money");
  assert.equal(neighborhoodLayout(data, 2, extra).entities, 140);
  extra = showMore(data, 2, extra, "money");
  assert.equal(extra.money, 85);
  assert.equal(neighborhoodLayout(data, 2, extra).entities, 150);
  assert.equal(showMore(data, 2, extra, "money"), extra, "the limit stops further presses");
  const none = {};
  assert.equal(showMore(data, 2, none, "evidence"), none, "a family that is never drawn adds nothing");
});

test("caps and presses count links, not neighbors", () => {
  docId = 1;
  const links = [];
  // Each neighbor has two parties links.
  for (let i = 0; i < 30; i++)
    links.push(link("parties", `n-${String(i).padStart(2, "0")}`, 30 - i), link("parties", `n-${String(i).padStart(2, "0")}`, 1, { type: "SIGNED_BY" }));
  const data = summary(links, true);
  const sector = extra => neighborhoodLayout(data, 2, extra).sectors[0];
  assert.deepEqual([sector({}).shown, sector({}).drawn, sector({}).hidden], [1, 2, 58]);
  const once = showMore(data, 2, {}, "parties");
  assert.equal(sector(once).drawn - sector({}).drawn, 24, "one press draws at most 25 more links");
  // A neighbor whose links exceed the cap is still drawn when it ranks first.
  assert.equal(neighborhoodLayout(data, 1).sectors[0].shown, 1);
});

test("a two-family neighbor sits once, in the family with more evidence documents", () => {
  const placed = placeNeighbors(hub());
  assert.ok(placed.get("money").some(member => member.id === "two-family"));
  assert.ok(!placed.get("parties").some(member => member.id === "two-family"));
  // Ranked first in money (5 documents beat 1); its parties link stays off the canvas.
  const layout = neighborhoodLayout(hub(), 2);
  assert.equal(layout.nodes.filter(node => node.id === "two-family").length, 1);
  assert.deepEqual(layout.links.filter(item => item.target === "two-family").map(item => item.type), ["PREMIUM_PAID_TO"]);
  // Ties go to the earlier family.
  docId = 1;
  const tie = placeNeighbors(summary([link("money", "n", 2), link("parties", "n", 2, { type: "ISSUED_BY" })]));
  assert.deepEqual([...tie.keys()], ["parties"]);
});

test("ranking: documents, then latest document, then name", () => {
  docId = 1;
  const data = summary([link("parties", "b-low", 1, { name: "Beta" }), link("parties", "a-high", 3, { name: "Zed" }),
    link("parties", "c-recent", 1, { name: "Gamma" }), link("parties", "d-name", 1, { name: "Alpha" })]);
  data.documents[data.links[2].records[0].doc_id].date = "2026-05-01";
  assert.deepEqual(placeNeighbors(data).get("parties").map(member => member.id), ["a-high", "c-recent", "d-name", "b-low"]);
});

test("no Document node and no evidence link is laid out", () => {
  const layout = neighborhoodLayout(hub(), 6);
  assert.ok(layout.nodes.every(node => node.label !== "Document"));
  assert.ok(layout.links.every(item => item.family !== "evidence" && item.type !== "MENTIONS"));
});

test("fixed sectors: parties at 12 o'clock, then clockwise; same input gives same positions", () => {
  const data = hub();
  const layout = neighborhoodLayout(data, 2);
  assert.deepEqual(layout.sectors.map(sector => [sector.family, sector.start, sector.end]),
    [["parties", -60, 60], ["money", 60, 180], ["places", 180, 300]]);
  const focus = layout.nodes[0];
  assert.deepEqual([focus.kind, focus.x, focus.y], ["focus", 0, 0]);
  for (const node of layout.nodes.slice(1))
    assert.ok(Math.abs(Math.hypot(node.x, node.y) - (node.kind === "pill" ? 244 : 210)) < 0.01, node.id);
  // Parties sits above the focus, money to the lower right, places to the lower left.
  const at = id => layout.nodes.find(node => node.id === id);
  assert.ok(at("pill:parties").y < 0);
  assert.ok(at("pill:money").x > 0);
  assert.ok(at("place-0").x < 0);
  // Pill last: it follows the shown members clockwise.
  const parties = layout.nodes.filter(node => node.family === "parties");
  assert.equal(parties.at(-1).kind, "pill");
  const angle = node => (Math.atan2(node.x, -node.y) * 180) / Math.PI;
  assert.ok(parties.every((node, index) => index === 0 || angle(node) > angle(parties[index - 1])));
  // Shuffled input gives byte-identical output.
  const shuffled = { ...data, links: [...data.links].reverse() };
  assert.equal(JSON.stringify(neighborhoodLayout(shuffled, 2)), JSON.stringify(layout));
});
