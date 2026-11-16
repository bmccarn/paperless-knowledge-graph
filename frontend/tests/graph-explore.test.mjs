import { test } from "node:test";
import assert from "node:assert/strict";
import { collapse, exploreCrossLinks, exploreLayout, exploreLimits, exploreName, hiddenNeighbors, MAX_HOP, START_RADIUS } from "../.test-build/graph-explore.js";
import { nodeRadius, shapeExtent } from "../.test-build/graph-encoding.js";

const FAMILY = { PARTY_TO: "parties", PAID_TO: "money", COVERS: "coverage", LIVES_IN: "places", RELATED_TO: "loose" };
const DESKTOP = exploreLimits(false);

/** Summaries for every entity of an undirected list of [source, type, target, documents] links. */
function world(edges, labels = {}) {
  let doc = 1;
  const records = edges.map(([, , , documents = 1]) => Array.from({ length: documents }, () => ({ doc_id: doc++, inferred: false, confidence: 0.9 })));
  const ids = [...new Set(edges.flatMap(([source, , target]) => [source, target]))].sort();
  const docsOf = id => new Set(edges.flatMap((edge, index) => (edge[0] === id || edge[2] === id ? records[index].map(record => record.doc_id) : [])));
  const entity = id => ({ id, label: labels[id] || "Organization", name: id, evidence_doc_count: docsOf(id).size,
    link_count: edges.filter(([source, , target]) => source === id || target === id).length, is_owner: false });
  const summaries = new Map(ids.map(id => {
    const links = edges.flatMap(([source, type, target], index) => source !== id && target !== id ? [] : [{
      key: `${source}~${type}~${target}`, type, family: FAMILY[type], direction: source === id ? "out" : "in",
      support_state: "supported", stated_count: records[index].length, inferred_count: 0, records: records[index],
      neighbor: entity(source === id ? target : source),
    }]);
    const documents = Object.fromEntries([...docsOf(id)].map(docId => [docId, { title: `Doc ${docId}`, date: "2026-01-01", doc_type: "invoice" }]));
    return [id, { focus: { ...entity(id), aliases: [], is_hub: false }, links, documents, owner_status: "found", owner_duplicates: [] }];
  }));
  return summaries;
}

// A root with 10 links in four families; "car" has 8 new neighbors and two loops; a chain car > loan > lender > hq.
function sample() {
  const edges = [
    ["root", "PAID_TO", "insurer", 6], ["root", "PARTY_TO", "holder", 5], ["root", "COVERS", "car", 4], ["root", "PAID_TO", "premium", 3],
    ["root", "RELATED_TO", "bank", 2], ["root", "LIVES_IN", "town", 2], ["root", "RELATED_TO", "prior", 2], ["root", "PARTY_TO", "agent", 2],
    ["root", "PARTY_TO", "clerk", 1], ["root", "PAID_TO", "fee", 1],
    ["car", "PAID_TO", "loan", 5], ["car", "PARTY_TO", "shop", 3], ["car", "PARTY_TO", "dmv", 2], ["car", "RELATED_TO", "recall", 2],
    ["car", "PARTY_TO", "dealer", 2], ["car", "COVERS", "tires", 1], ["car", "RELATED_TO", "plate", 1], ["car", "PAID_TO", "tag", 1],
    ["car", "RELATED_TO", "bank", 3], ["car", "PARTY_TO", "holder", 2], ["car", "PAID_TO", "dealer", 1],
    ["loan", "PAID_TO", "lender", 3], ["loan", "PAID_TO", "payment", 1], ["loan", "RELATED_TO", "bank", 1],
    ["lender", "LIVES_IN", "hq", 2], ["hq", "LIVES_IN", "harbor", 1],
    ["shop", "LIVES_IN", "town", 1],
  ];
  return world(edges);
}
const positions = layout => Object.fromEntries(layout.nodes.map(node => [node.id, [node.x, node.y]]));

test("the start ring holds the 8 strongest links at 160 units, from 12 o'clock in family order, then rank", () => {
  const all = sample();
  const layout = exploreLayout(all.get("root"), all, [], DESKTOP);
  const ring = layout.nodes.filter(node => node.hop === 1);
  assert.deepEqual(ring.map(node => node.id), ["holder", "agent", "insurer", "premium", "car", "town", "bank", "prior"]);
  assert.deepEqual([ring[0].x, ring[0].y], [0, -START_RADIUS]);
  for (const node of ring) assert.equal(Math.round(Math.hypot(node.x, node.y)), START_RADIUS);
  assert.equal(hiddenNeighbors(all.get("root"), new Set(layout.nodes.map(node => node.id))), 2, "clerk and fee are not on the ring");
  assert.equal(exploreLayout(all.get("root"), all, [], exploreLimits(true)).nodes.length, 7, "phone: 6 starting links");
});

test("the same summaries and x list yield identical positions, and an expansion never moves a placed node", () => {
  const x = ["car", "loan", "root", "lender", "shop"];
  const first = exploreLayout(sample().get("root"), sample(), x, DESKTOP);
  const again = sample();
  assert.deepEqual(exploreLayout(again.get("root"), again, x, DESKTOP), first);
  for (let size = 0; size < x.length; size++) {
    const before = positions(exploreLayout(again.get("root"), again, x.slice(0, size), DESKTOP));
    const after = positions(exploreLayout(again.get("root"), again, x.slice(0, size + 1), DESKTOP));
    for (const [id, at] of Object.entries(before)) assert.deepEqual(after[id], at, `${id} moved when ${x[size]} expanded`);
  }
});

test("an expansion adds at most 6 new entities, strongest first, and draws its links to entities already in view", () => {
  const all = sample();
  const before = exploreLayout(all.get("root"), all, [], DESKTOP);
  const layout = exploreLayout(all.get("root"), all, ["car"], DESKTOP);
  assert.deepEqual(layout.expansions, [{ index: 0, id: "car", added: ["loan", "shop", "dealer", "dmv", "recall", "plate"] }]);
  assert.equal(layout.nodes.length - before.nodes.length, 6, "dealer has two links but counts once");
  const keys = layout.links.map(link => link.key);
  for (const loop of ["car~RELATED_TO~bank", "car~PARTY_TO~holder", "car~PARTY_TO~dealer", "car~PAID_TO~dealer"]) assert.ok(keys.includes(loop), loop);
  assert.ok(!keys.includes("car~COVERS~tires"), "a link to an entity not added is not drawn");
  const car = layout.nodes.find(node => node.id === "car");
  for (const node of layout.nodes.filter(node => node.parent === "car")) {
    assert.equal(node.hop, 2);
    assert.ok([120, 150, 180, 210].includes(Math.round(Math.hypot(node.x - car.x, node.y - car.y))), `${node.id} sits on a fan radius`);
  }
  assert.equal(hiddenNeighbors(all.get("car"), new Set(layout.nodes.map(node => node.id))), 2, "tires and tag remain");
  const again = exploreLayout(all.get("root"), all, ["car", "car"], DESKTOP);
  assert.deepEqual(again.expansions[1].added, ["tag", "tires"], "a second expansion of a node adds what it still has");
});

test("a hop-3 node does not expand", () => {
  const all = sample();
  const layout = exploreLayout(all.get("root"), all, ["car", "loan", "lender"], DESKTOP);
  const lender = layout.nodes.find(node => node.id === "lender");
  assert.equal(lender.hop, MAX_HOP);
  assert.deepEqual(layout.expansions.map(expansion => expansion.id), ["car", "loan"]);
  assert.ok(!layout.nodes.some(node => node.id === "hq"));
  assert.equal(layout.hops, 3);
});

test("an expansion past the entity limit adds what fits and reports it", () => {
  const all = sample();
  const limits = { ...DESKTOP, entities: 12 };
  const layout = exploreLayout(all.get("root"), all, ["car", "root"], limits);
  assert.equal(layout.nodes.length, 12);
  assert.deepEqual(layout.expansions.map(expansion => expansion.added.length), [3, 0]);
  assert.equal(layout.clipped, true);
  assert.equal(exploreLayout(all.get("root"), all, ["car"], DESKTOP).clipped, false);
  // Desktop can never pass 150: 1 + 8 + 20 expansions of 6 is 129. Phone can: 1 + 6 + 14 expansions of 4 passes 60.
  assert.ok(1 + DESKTOP.start + 20 * DESKTOP.step < DESKTOP.entities);
});

test("layout stops at the first expansion whose summary has not arrived, so later ones cannot move", () => {
  const all = sample();
  const partial = new Map([...all].filter(([id]) => id !== "car"));
  const layout = exploreLayout(all.get("root"), partial, ["insurer", "car", "loan"], DESKTOP);
  assert.equal(layout.pending, 1);
  assert.deepEqual(layout.expansions.map(expansion => expansion.id), ["insurer"]);
  assert.deepEqual(positions(layout), positions(exploreLayout(all.get("root"), all, ["insurer"], DESKTOP)));
});

test("collapse removes the expansion and everything added through it; nodes from earlier expansions stay", () => {
  const all = sample();
  const x = ["car", "insurer", "loan", "shop", "missing"];
  const layout = exploreLayout(all.get("root"), all, x, DESKTOP);
  assert.deepEqual(collapse(layout, x, [0]), ["insurer"], "loan and shop came from car; missing was never in view");
  assert.deepEqual(collapse(layout, x, [2]), ["car", "insurer", "shop"]);
  const after = exploreLayout(all.get("root"), all, collapse(layout, x, [2]), DESKTOP);
  assert.ok(after.nodes.some(node => node.id === "loan"), "loan was added by car, not by its own expansion");
  assert.ok(!after.nodes.some(node => node.id === "lender"));
  // A repeated expansion grows from the first: collapsing the first removes the repeat and its batch too.
  const twice = exploreLayout(all.get("root"), all, ["car", "car"], DESKTOP);
  assert.deepEqual(collapse(twice, ["car", "car"], [0]), []);
  assert.deepEqual(collapse(twice, ["car", "car"], [1]), ["car"]);
});

test("cross links add only links among entities in view that no summary drew, strongest first, capped", () => {
  const all = sample();
  const layout = exploreLayout(all.get("root"), all, ["car"], DESKTOP);
  const link = (source, type, target, documents) => ({ key: `${source}~${type}~${target}`, type, family: FAMILY[type], source, target,
    support_state: "supported", stated_count: documents, inferred_count: 0, records: Array.from({ length: documents }, (_, i) => ({ doc_id: 900 + i, inferred: false, confidence: 1 })) });
  const subgraph = { links: [link("shop", "LIVES_IN", "town", 1), link("dmv", "LIVES_IN", "town", 2), link("car", "PAID_TO", "loan", 5),
    link("hq", "LIVES_IN", "town", 3), { ...link("shop", "LIVES_IN", "dmv", 1), family: "evidence" }], documents: {}, total: 5, truncated: false };
  const result = exploreCrossLinks(layout, subgraph, 60);
  assert.deepEqual(result.links.map(item => item.key), ["dmv~LIVES_IN~town", "shop~LIVES_IN~town"]);
  assert.equal(exploreCrossLinks(layout, subgraph, 1).links.length, 1);
  assert.equal(exploreCrossLinks(layout, subgraph, 1).total, 2);
});

test("shapes in the sample stay at least 32 units apart, and the canvas name counts entities, links and hops", () => {
  const all = sample();
  const layout = exploreLayout(all.get("root"), all, ["car", "loan", "root", "shop", "insurer"], DESKTOP);
  const extent = node => shapeExtent(node.label, nodeRadius(node.evidence_doc_count));
  const overlaps = layout.nodes.flatMap((a, i) => layout.nodes.slice(i + 1)
    .filter(b => Math.hypot(a.x - b.x, a.y - b.y) < extent(a) + extent(b) + 32).map(b => `${a.id}/${b.id}`));
  assert.deepEqual(overlaps, []);
  assert.equal(exploreName({ id: "root", name: "Policy KT-9030", label: "InsurancePolicy" }, 1, 0, 0), "Explore from Policy KT-9030: 1 entity, 0 links, 0 hops.");
  assert.equal(exploreName({ id: "abcdef123", name: "", label: "Person" }, 2, 1, 1), "Explore from Person abcdef: 2 entities, 1 link, 1 hop.");
});

test("150 entities lay out well inside one frame", () => {
  // Single-digit ids keep name order equal to number order.
  const edges = [];
  for (let i = 0; i < 9; i++) edges.push(["root", "PAID_TO", `a${i}`, 3]);
  for (let i = 0; i < 8; i++) for (let j = 0; j < 8; j++) edges.push([`a${i}`, "PARTY_TO", `b${i}-${j}`, 2]);
  for (let i = 0; i < 8; i++) for (let j = 0; j < 8; j++) for (let k = 0; k < 8; k++) edges.push([`b${i}-${j}`, "RELATED_TO", `c${i}-${j}-${k}`, 1]);
  const all = world(edges);
  const x = [...Array.from({ length: 8 }, (_, i) => `a${i}`), ...Array.from({ length: 12 }, (_, i) => `b${i % 8}-${Math.floor(i / 8)}`)];
  const started = performance.now();
  const layout = exploreLayout(all.get("root"), all, x, DESKTOP);
  const elapsed = performance.now() - started;
  assert.equal(layout.nodes.length, 129);
  assert.ok(elapsed < 100, `took ${elapsed.toFixed(1)} ms`);
});
