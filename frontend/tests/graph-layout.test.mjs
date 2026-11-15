import { test } from "node:test";
import assert from "node:assert/strict";
import {
  RING_RADIUS,
  crossLinks,
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
    links, documents, owner_status: "found", owner_duplicates: [],
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

test("on the owner's view, links to other matches of the owner setting are not drawn", () => {
  const data = summary([link("parties", "employer", 2), link("loose", "owner-copy", 4, { type: "RELATED_TO", label: "Person" })]);
  assert.deepEqual(drawableLinks(data).map(item => item.neighbor.id), ["employer", "owner-copy"], "Another focus draws it");
  data.focus.is_owner = true;
  data.owner_duplicates = [{ id: "owner-copy", label: "Person", name: "Owner", evidence_doc_count: 4 }];
  assert.deepEqual(drawableLinks(data).map(item => item.neighbor.id), ["employer"]);
  assert.deepEqual(neighborhoodLayout(data, 6).nodes.map(node => node.id), ["focus", "employer"]);
});

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
  // A neighbor whose links exceed the cap is not drawn, even when it ranks first.
  assert.deepEqual([neighborhoodLayout(data, 1).sectors[0].shown, neighborhoodLayout(data, 1).sectors[0].hidden], [0, 60]);
  // Budget the cap leaves unused never lets a press draw more than 25 links.
  docId = 1;
  const slack = summary([link("parties", "a", 5), link("parties", "b", 4), link("parties", "b", 1, { type: "SIGNED_BY" }),
    ...Array.from({ length: 30 }, (_, i) => link("parties", `c-${String(i).padStart(2, "0")}`, 1))]);
  const layer = extra => neighborhoodLayout(slack, 2, extra).sectors[0];
  assert.equal(layer({}).drawn, 1, "b's two links do not fit the one link left");
  const press = showMore(slack, 2, {}, "parties");
  assert.ok(layer(press).drawn - layer({}).drawn <= 25, `press drew ${layer(press).drawn - layer({}).drawn}`);
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

// Links among the hub's neighbors, as GET /graph/subgraph returns them.
function between(source, target, documents, type = "RELATED_TO", family = "loose") {
  return { key: `${source}~${type}~${target}`, type, family, source, target, support_state: "supported",
    stated_count: documents, inferred_count: 0,
    records: Array.from({ length: documents }, (_, index) => ({ doc_id: 1000 + index, inferred: false, confidence: 0.9 })) };
}
const response = links => ({ links, documents: {}, total: links.length, truncated: false });

test("links between neighbors: drawn neighbors only, each once, strongest first, no evidence and no pill member", () => {
  const layout = neighborhoodLayout(hub(), familyCap(true, false));
  const drawn = layout.nodes.filter(node => node.kind === "neighbor").map(node => node.id);
  const hidden = "party-59";
  assert.ok(!drawn.includes(hidden), "party-59 sits in the parties pill");
  const [a, b, c] = drawn;
  const result = crossLinks(layout, response([
    between(a, b, 1), between(b, c, 3, "EMPLOYED_BY", "parties"), between(a, b, 1), between(a, hidden, 9),
    between(a, c, 2, "MENTIONS", "evidence"), between(c, c, 4), between(a, "focus", 5), between(c, a, 3, "PAID_TO", "money"),
  ]), 60);
  assert.deepEqual(result.links.map(link => link.key), [`${b}~EMPLOYED_BY~${c}`, `${c}~PAID_TO~${a}`, `${a}~RELATED_TO~${b}`]);
  assert.equal(result.total, 3);
  assert.deepEqual(crossLinks(layout, response([between(a, b, 1)]), 60), crossLinks(layout, response([between(a, b, 1)]), 60),
    "The same data gives the same curves");
});

test("links between neighbors: over the cap the strongest are drawn and the total counts all of them", () => {
  const layout = neighborhoodLayout(hub(), familyCap(false, false), { parties: 60, money: 120 });
  const drawn = layout.nodes.filter(node => node.kind === "neighbor").map(node => node.id);
  const links = drawn.slice(1, 62).map((id, index) => between(drawn[0], id, 1 + (index % 5)));
  assert.equal(links.length, 61);
  const result = crossLinks(layout, response(links), 60);
  assert.equal(result.links.length, 60);
  assert.equal(result.total, 61);
  assert.ok(result.links.every(link => link.link.records.length >= result.links.at(-1).link.records.length));
  assert.equal(crossLinks(layout, { ...response(links), total: 700, truncated: true }, 30).total, 700, "A truncated response keeps its total");
});

test("each curve bows away from the focus, from either direction, and a pair across the ring passes at a quarter radius", () => {
  // force-graph's control point: the chord midpoint plus curvature * length along (dy, -dx) / length.
  const control = (start, end, curvature) => {
    const dx = end.x - start.x, dy = end.y - start.y;
    return { x: (start.x + end.x) / 2 + curvature * dy, y: (start.y + end.y) / 2 - curvature * dx };
  };
  const ring = degrees => ({ x: RING_RADIUS * Math.sin(degrees * Math.PI / 180), y: -RING_RADIUS * Math.cos(degrees * Math.PI / 180) });
  const layoutOf = angles => ({ nodes: angles.map((angle, index) => ({ id: `n${index}`, kind: "neighbor", ...ring(angle) })), links: [], sectors: [], entities: 0 });
  for (const [from, to] of [[0, 60], [60, 0], [350, 20], [100, 250], [250, 100]]) {
    const layout = layoutOf([from, to]);
    const [link] = crossLinks(layout, response([between("n0", "n1", 1)]), 60).links;
    const [start, end] = layout.nodes;
    const point = control(start, end, link.curvature);
    const chord = Math.hypot((start.x + end.x) / 2, (start.y + end.y) / 2);
    assert.ok(Math.abs(Math.hypot(point.x, point.y) - (chord + (RING_RADIUS - chord) / 2)) < 0.01, `${from} to ${to}: halfway to the ring`);
    const arc = Math.min(Math.abs(to - from), 360 - Math.abs(to - from));
    assert.ok(Math.abs(Math.abs(link.curvature) - Math.tan(arc * Math.PI / 720) / 4) < 1e-5, `${from} to ${to}: tan(d/4)/4`);
  }
  const across = layoutOf([30, 210]);
  const [link] = crossLinks(across, response([between("n0", "n1", 1)]), 60).links;
  const [start, end] = across.nodes;
  const point = control(start, end, link.curvature);
  // A quadratic curve's middle is (start + 2 * control + end) / 4.
  const middle = { x: (start.x + 2 * point.x + end.x) / 4, y: (start.y + 2 * point.y + end.y) / 4 };
  assert.ok(Math.abs(Math.hypot(middle.x, middle.y) - RING_RADIUS / 4) < 0.01, "It passes the focus at a quarter of the radius");
  assert.equal(crossLinks(across, response([between("n0", "n1", 1)]), 60).links[0].curvature, link.curvature, "On the same side every time");
});
