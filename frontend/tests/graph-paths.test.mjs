import test from "node:test";
import assert from "node:assert/strict";
import { askRelated, bestQuote, forward, parseMaxLen, pathLayout, routeText, weakestDocs } from "../.test-build/graph-paths.js";

const link = (source, type, target, docs, state = "supported") =>
  ({ source, type, target, family: "parties", weight: docs, support_state: state, stated_count: state === "inferred" ? 0 : docs, inferred_count: state === "inferred" ? docs : 0 });
const key = ({ source, type, target }) => `${source}~${type}~${target}`;
function response(routes) {
  const links_detail = {}, nodes = {};
  const paths = routes.map(route => {
    const keys = [];
    for (let k = 0; k < route.length - 1; k++) {
      const [from, type, docs, reverse, state] = route[k + 1];
      const item = reverse ? link(from, type, route[k][0], docs, state) : link(route[k][0], type, from, docs, state);
      links_detail[key(item)] = item;
      keys.push(key(item));
    }
    for (const [id] of route) nodes[id] = { id, label: "Organization", name: `Name ${id}` };
    const weights = keys.map(k => links_detail[k].weight);
    return { nodes: route.map(([id]) => id), links: keys, length: keys.length, weakest_weight: Math.min(...weights), has_inferred: false };
  });
  return { paths, exhausted: true, nodes, links_detail };
}
const A = ["a"];
const data = response([
  [A, ["m", "LOAN_FROM", 3], ["b", "LOAN_FROM", 5]],
  [A, ["n", "EMPLOYED_BY", 6], ["b", "PAYS", 2, true]],
  [A, ["x", "SIGNED", 2], ["y", "PARTY_TO", 2], ["b", "ACCOUNT_AT", 1]],
]);

test("the layout puts A left, B right, node k of L links at k / L, and one lane per path in rank order", () => {
  const { positions } = pathLayout(data, "a", "b", 840, 600);
  assert.deepEqual(positions.get("a"), { x: 120, y: 300 });
  assert.deepEqual(positions.get("b"), { x: 720, y: 300 });
  assert.deepEqual(positions.get("m"), { x: 420, y: 160 });
  assert.deepEqual(positions.get("n"), { x: 420, y: 300 });
  assert.deepEqual(positions.get("x"), { x: 320, y: 440 });
  assert.deepEqual(positions.get("y"), { x: 520, y: 440 });
  assert.deepEqual(pathLayout(data, "a", "b", 840, 600), pathLayout(data, "a", "b", 840, 600), "same input, same picture");
});

test("a node shared by two paths keeps the place of the better path, and lanes shrink to fit", () => {
  const shared = response([[A, ["m", "T", 4], ["b", "T", 4]], [A, ["m", "T", 4], ["z", "U", 2], ["b", "V", 2]]]);
  const { positions } = pathLayout(shared, "a", "b", 840, 240);
  assert.deepEqual(positions.get("m"), { x: 420, y: 70 });
  assert.equal(positions.get("z").y, 170);
  assert.equal(positions.size, 4);
});

test("with a direct link no lane lies on the straight line, and parallel direct links bow apart", () => {
  const direct = response([[A, ["b", "OWNS", 9]], [A, ["b", "PAYS", 4]], [A, ["m", "T", 3], ["b", "T", 3]], [A, ["b", "SUES", 1]]]);
  const { positions, bow } = pathLayout(direct, "a", "b", 840, 600);
  assert.notEqual(positions.get("m").y, 300);
  assert.deepEqual(bow.map(Math.abs), [0, 40, 0, 40]);
  assert.equal(Math.sign(bow[1]), -Math.sign(bow[3]));
});

test("path text: route, weakest link documents, link direction and the Ask question", () => {
  const name = node => node.name;
  assert.equal(routeText(data, data.paths[0], name), "Name m");
  assert.equal(routeText(data, data.paths[2], name), "Name x → Name y");
  assert.equal(routeText(response([[A, ["b", "OWNS", 9]]]), { nodes: ["a", "b"], links: [] }, name), "Direct link");
  assert.equal(weakestDocs(data, data.paths[2]), 1);
  assert.equal(forward(data, data.paths[1], 0), true);
  assert.equal(forward(data, data.paths[1], 1), false, "b PAYS n runs against the path");
  assert.equal(askRelated("Dana Whitlock", "Northgate Credit Union"), "How is Dana Whitlock related to Northgate Credit Union?");
  assert.deepEqual(["2", "3", "4", "5", "1", null, "x"].map(parseMaxLen), [2, 3, 4, 4, 4, 4, 4]);
});

test("the best quote comes from a stated record first, then the highest score, then the document id", () => {
  const record = (doc_id, inferred, confidence, quotes) => ({ doc_id, inferred, confidence, quotes });
  assert.equal(bestQuote([record(1, true, 0.99, ["inferred"]), record(2, false, 0.5, ["stated"])]), "stated");
  assert.equal(bestQuote([record(3, false, null, ["no score"]), record(4, false, 0.7, ["scored"])]), "scored");
  assert.equal(bestQuote([record(6, false, 0.7, ["six"]), record(5, false, 0.7, ["five"])]), "five");
  assert.equal(bestQuote([record(7, false, 0.9, []), record(8, true, 0.2, ["only quote"])]), "only quote");
  assert.equal(bestQuote([record(9, false, 0.9, [])]), null);
});
