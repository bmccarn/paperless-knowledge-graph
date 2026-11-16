import { test } from "node:test";
import assert from "node:assert/strict";
import { arrangeOverview, BRIDGES, bridgeLinks, GROUP_MIN, nameBox, overviewLinks, packCircles, propagateLabels } from "../.test-build/graph-overview.js";

const ALL = ["parties", "money", "coverage", "health", "service", "places", "time", "doclinks", "loose", "other"];
const filters = (change = {}) => ({ families: ALL, support: "all", minDocs: 1, ...change });

/** A synthetic response: ids in index order, a hub star, a chain, a pair, an owner and its only-linked entity. */
function response() {
  const nodes = [];
  const links = [];
  const node = (label, name, documents = 1, count = 1) => nodes.push([`n-${String(nodes.length).padStart(3, "0")}`, label, name, documents, count]) - 1;
  const owner = node("Person", "Owner", 90, 40);
  const hub = node("Organization", "Harbor Mutual", 33, 34);
  // 33 copies of one address around one organization, as near-duplicates are in a real archive.
  for (let k = 0; k < 33; k++) links.push([hub, node("Address", k % 3 ? "2 Harbor Way" : "", 1, 1), 1, 0, 1 << 5]);
  const chain = [node("Person", "Avery Brooks", 4, 3), node("Contract", "Lease", 6, 2), node("FinancialItem", "Rent", 6, 2), node("Event", "Move", 2, 1)];
  for (let k = 1; k < chain.length; k++) links.push([chain[k - 1], chain[k], 2, k === 2 ? 2 : 0, 1 << 1]);
  const pair = [node("Product", "Car", 3, 1), node("Contract", "Loan", 2, 1)];
  links.push([pair[0], pair[1], 3, 1, 1 << 0]);
  const lonely = node("Organization", "Library", 1, 1);
  links.push([owner, lonely, 4, 0, 1 << 4], [owner, hub, 9, 0, 1 << 0]);
  // The route sorts pairs by index.
  links.forEach(link => { if (link[0] > link[1]) [link[0], link[1]] = [link[1], link[0]]; });
  links.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  return { version: "v1", columns: ["id", "label", "name", "evidence_doc_count", "link_count"], nodes,
    link_columns: ["source", "target", "weight", "support", "families"], links, owner, unlinked: 12, truncated: false };
}

test("the same response yields the same groups and positions", () => {
  const first = arrangeOverview(response(), filters());
  const second = arrangeOverview(structuredClone(response()), filters());
  assert.deepEqual(second.groups, first.groups);
  assert.deepEqual([...second.positions], [...first.positions]);
  assert.deepEqual(second.small, first.small);
});

test("groups have at least four entities, are named after their lead, and leave out the owner", () => {
  const map = arrangeOverview(response(), filters());
  assert.deepEqual(map.groups.map(group => [map.nodes[group.lead].name, group.members.length, group.links]), [["Harbor Mutual", 34, 33], ["Lease", 4, 3]]);
  // The lead has the most evidence documents; Lease and Rent tie on 6 and 2 links, so the smaller id leads.
  assert.equal(map.groups[1].id, map.nodes[map.groups[1].lead].id);
  assert.deepEqual(map.groups[1].members.map(node => map.nodes[node].name), ["Lease", "Rent", "Avery Brooks", "Move"]);
  assert.deepEqual(map.groups[0].kinds, [{ label: "Address", count: 33 }, { label: "Organization", count: 1 }]);
  assert.deepEqual(map.small.map(set => set.map(node => map.nodes[node].name)), [["Car", "Loan"]]);
  assert.equal(map.ownerOnly, 1);
  assert.ok(Number.isNaN(map.positions[0]) && Number.isNaN(map.positions[2 * 41]), "neither the owner nor its only neighbor is drawn");
  assert.ok(map.groups.every(group => group.members.length >= GROUP_MIN));
});

test("group circles never touch and every member sits inside its circle", () => {
  const map = arrangeOverview(response(), filters());
  const [a, b] = map.groups;
  assert.ok(Math.hypot(a.x - b.x, a.y - b.y) >= a.r + b.r);
  for (const group of map.groups) for (const node of group.members)
    assert.ok(Math.hypot(map.positions[node * 2] - group.x, map.positions[node * 2 + 1] - group.y) <= group.r);
  const radii = [80, 60, 60, 40, 30, 30, 20, 12, 12, 12];
  const centers = packCircles(radii);
  for (let i = 0; i < radii.length; i++) for (let j = i + 1; j < radii.length; j++)
    assert.ok(Math.hypot(centers[i].x - centers[j].x, centers[i].y - centers[j].y) >= radii[i] + radii[j]);
  assert.deepEqual(centers[0], { x: 0, y: 0 });
  // Groups come in entity order: a chain of 17 has a smaller circle than stars of 14. The largest circle still goes first.
  const mixed = [40, 60, 60, 60, 12, 80];
  const spread = packCircles(mixed);
  for (let i = 0; i < mixed.length; i++) for (let j = i + 1; j < mixed.length; j++)
    assert.ok(Math.hypot(spread[i].x - spread[j].x, spread[i].y - spread[j].y) >= mixed[i] + mixed[j], `circles ${i} and ${j} overlap`);
  assert.deepEqual(spread[5], { x: 0, y: 0 });
});

test("label propagation settles a star on one label and visits nodes in id order", () => {
  // Leaves 1 to 4 around hub 0: the hub takes the smallest leaf label on a tie, and the leaves follow it.
  const star = propagateLabels(5, [1, 2, 3, 4].map(leaf => ({ source: 0, target: leaf })));
  assert.equal(new Set(star).size, 1);
  // Two separate triangles stay two groups. (One bridge would merge them: ties go to the smaller label.)
  const triangles = propagateLabels(6, [[0, 1], [1, 2], [0, 2], [3, 4], [4, 5], [3, 5]].map(([source, target]) => ({ source, target })));
  assert.deepEqual([...triangles], [1, 1, 1, 4, 4, 4]);
  assert.deepEqual([...propagateLabels(3, [])], [0, 1, 2]);
});

test("two neighbors of a hub that link to each other stay in the hub's group", () => {
  // Hub 5 has the most documents; leaves 0 and 1 also link to each other. Started from their index, the two leaves tie
  // and keep their own smaller label; started from the lead rank, the tie goes to the hub.
  const links = [[0, 5], [1, 5], [2, 5], [3, 5], [4, 5], [0, 1]].map(([source, target]) => ({ source, target }));
  assert.notEqual(new Set(propagateLabels(6, links)).size, 1);
  assert.equal(new Set(propagateLabels(6, links, Int32Array.from([1, 2, 3, 4, 5, 0]))).size, 1);
  const nodes = [0, 1, 2, 3, 4, 5].map(index => [`n-${index}`, "Address", `Copy ${index}`, index === 5 ? 30 : 1, 1]);
  const map = arrangeOverview({ ...response(), nodes, links: links.map(({ source, target }) => [source, target, 1, 0, 1]), owner: null }, filters());
  assert.deepEqual(map.groups.map(group => group.members.length), [6]);
});

test("the family filter hides a pair only when none of its families is shown", () => {
  const links = [{ source: 1, target: 2, weight: 3, support: 0, families: 0b11 }, { source: 2, target: 3, weight: 1, support: 2, families: 0b100 },
    { source: 0, target: 3, weight: 0, support: 0, families: 1 }, { source: 4, target: 5, weight: 0, support: 0, families: 1 }];
  const keys = list => list.map(link => `${link.source}-${link.target}`);
  assert.deepEqual(keys(overviewLinks(links, 0, filters())), ["1-2", "2-3", "4-5"], "the owner's pairs go; a pair without records stays");
  assert.deepEqual(keys(overviewLinks(links, 0, filters({ families: ["money"] }))), ["1-2"]);
  assert.deepEqual(keys(overviewLinks(links, 0, filters({ support: "supported" }))), ["1-2", "4-5"]);
  assert.deepEqual(keys(overviewLinks(links, 0, filters({ support: "inferred" }))), ["2-3"]);
  assert.deepEqual(keys(overviewLinks(links, 0, filters({ minDocs: 2 }))), ["1-2"]);
});

test("filters regroup the map and count what they hide", () => {
  const map = arrangeOverview(response(), filters({ families: ["money", "parties"] }));
  assert.deepEqual(map.groups.map(group => group.members.length), [4]);
  assert.equal(map.hidden, 34);
});

test("a link to a node outside the response is an arranging error", () => {
  const broken = { ...response(), links: [[0, 99, 1, 0, 1]] };
  assert.throws(() => arrangeOverview(broken, filters()), /outside the 42 nodes/);
});

test("an archive with no entity links arranges to nothing", () => {
  const empty = arrangeOverview({ ...response(), nodes: [], links: [], owner: null }, filters());
  assert.deepEqual([empty.groups.length, empty.small.length, empty.band], [0, 0, null]);
});

test("a name box touches no other circle or name box", () => {
  const radii = [90, 70, 60, 50, 40, 30, 30, 20, 20, 12, 12, 12];
  const names = radii.map((_, index) => (index < 6 ? { w: 220, h: 30 } : null));
  const gap = 10;
  const centers = packCircles(radii, gap, names, 1.7);
  const boxes = names.map((name, index) => name && nameBox(centers[index].x, centers[index].y, radii[index], name));
  const reach = (x, y, box) => Math.hypot(Math.max(box.x0 - x, 0, x - box.x1), Math.max(box.y0 - y, 0, y - box.y1));
  for (let i = 0; i < radii.length; i++) for (let j = 0; j < radii.length; j++) {
    if (i === j) continue;
    assert.ok(Math.hypot(centers[i].x - centers[j].x, centers[i].y - centers[j].y) >= radii[i] + radii[j] + gap, `circles ${i} and ${j}`);
    if (boxes[i]) assert.ok(reach(centers[j].x, centers[j].y, boxes[i]) >= radii[j] + gap, `name ${i} and circle ${j}`);
    if (boxes[i] && boxes[j]) assert.ok(boxes[i].x1 <= boxes[j].x0 || boxes[j].x1 <= boxes[i].x0 || boxes[i].y1 <= boxes[j].y0 || boxes[j].y1 <= boxes[i].y0, `names ${i} and ${j}`);
  }
});

test("the small groups band sits above the map, with room for its caption inside the bounds", () => {
  const map = arrangeOverview(response(), filters());
  assert.ok(map.band.y1 < Math.min(...map.groups.map(group => group.y - group.r)), "the band ends above every group");
  assert.ok(map.bounds.y0 < map.band.y0, "the caption's line is inside the bounds");
  assert.ok(map.scale > 0 && map.scale <= 2);
});

test("map zoom draws the strongest links between groups, and every link of a selected group", () => {
  // Groups 0 to 3 and the band (-1). Pairs: 0-1 weight 5 in two links, 1-2 weight 5 in two links, 0-2 weight 4, 2-3 weight 1.
  const groupOf = Int32Array.from([0, 0, 1, 1, 2, 2, 3, -1, -1]);
  const link = (source, target, weight) => ({ source, target, weight, support: 0, families: 1 });
  const links = [link(0, 1, 9), link(0, 2, 2), link(1, 3, 3), link(0, 4, 4), link(2, 5, 4), link(4, 6, 1), link(0, 7, 6), link(7, 8, 1), link(5, 3, 1)];
  // Inside a group (0) and inside the band (7) never count; the band link (6) shows only for a selected group.
  assert.deepEqual(bridgeLinks(links, groupOf, null, 2), [2, 4], "0-1 and 1-2 tie on weight and count, ahead of 0-2");
  assert.deepEqual(bridgeLinks(links, groupOf, null, 1), [2], "the pair 0-1 is drawn as its heaviest link");
  assert.deepEqual(bridgeLinks(links, groupOf, null), [2, 3, 4, 5]);
  assert.deepEqual(bridgeLinks(links, groupOf, 0, 0), [1, 2, 3, 6], "a selected group shows all of its links to other groups and the band");
  assert.deepEqual(bridgeLinks(links, groupOf, 3, 1), [2, 5]);
  assert.equal(BRIDGES, 16);
});
