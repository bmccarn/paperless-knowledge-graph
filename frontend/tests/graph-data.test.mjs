import { test } from "node:test";
import assert from "node:assert/strict";
import {
  emptyGraph,
  mergeGraph,
  nodeFromPayload,
  relationshipDirection,
  relationshipSupport,
  sourceDocumentIds,
  escapeGraphLabel,
} from "../.test-build/graph-data.js";

test("per-document support retains quotes and rationale while rejecting malformed records", () => {
  const support = relationshipSupport({support_records: ["{bad", JSON.stringify({
    source_doc: 101, inferred: true, evidence_spans: JSON.stringify([{quote: "Alice owns <Example>"}]),
    rationale: "The source records ownership.",
  })]});
  assert.deepEqual(support, [{documentId: 101, inferred: true, quotes: ["Alice owns <Example>"], rationale: ["The source records ownership."]}]);
});

test("ingestion and legacy relationship evidence stays visible without duplicate quotes", () => {
  const stored = {source_doc: 101, implied: true,
    evidence_json: JSON.stringify([{quote: "Alice owns <Example>"}, {quote: "Recorded in the source."}]),
    evidence_spans: JSON.stringify([{quote: "Alice owns <Example>"}]), rationale: "Source ownership."};
  for (const props of [stored, {support_records: [JSON.stringify(stored)]},
    {...stored, support_records: [JSON.stringify(stored)]}]) {
    assert.deepEqual(relationshipSupport(props), [{documentId: 101, inferred: true,
      quotes: ["Alice owns <Example>", "Recorded in the source."], rationale: ["Source ownership."]}]);
  }
});

const payload = {
  nodes: [
    { labels: ["Person"], props: { uuid: "p", name: "Person" } },
    { labels: ["Organization"], props: { uuid: "o", name: "Organization" } },
  ],
  relationships: [
    { start: "p", end: "o", type: "CUSTOMER_OF", props: { source_doc: 1 } },
  ],
};

test("repeated neighborhood expansion preserves relationship count and evidence", () => {
  const once = mergeGraph(emptyGraph(), payload);
  const twice = mergeGraph(once, payload);
  assert.equal(twice.nodes.length, 2);
  assert.equal(twice.links.length, 1);
  assert.deepEqual(twice.links[0].props, { source_doc: 1 });
});

test("different source support and relationship types remain distinct", () => {
  const more = {
    ...payload,
    relationships: [
      ...payload.relationships,
      { ...payload.relationships[0], props: { source_doc: 2 } },
      { ...payload.relationships[0], type: "EMPLOYED_BY" },
    ],
  };
  assert.equal(mergeGraph(emptyGraph(), more).links.length, 3);
});

test("stable backend relationship IDs update properties without duplication", () => {
  const first = {
    ...payload,
    relationships: [{ ...payload.relationships[0], id: "edge-1" }],
  };
  const second = {
    ...payload,
    relationships: [
      { ...first.relationships[0], props: { source_doc: 1, weight: 3 } },
    ],
  };
  const graph = mergeGraph(mergeGraph(emptyGraph(), first), second);
  assert.equal(graph.links.length, 1);
  assert.equal(graph.links[0].props.weight, 3);
});

test("missing identities and dangling relationships are reported, never fabricated", () => {
  assert.equal(
    nodeFromPayload({
      labels: ["Person"],
      props: { name: "Unnamed identity" },
    }),
    null,
  );
  assert.equal(
    nodeFromPayload({ labels: ["Document"], props: { paperless_id: 7 } }).id,
    "doc-7",
  );
  const graph = mergeGraph(emptyGraph(), {
    nodes: [{}],
    relationships: payload.relationships,
  });
  assert.equal(graph.discarded, 2);
  assert.deepEqual(graph.links, []);
});

test("backend fallback endpoint names normalize consistently", () => {
  const graph = mergeGraph(emptyGraph(), {
    nodes: payload.nodes,
    relationships: [{ start_uuid: "p", end_uuid: "o", type: "RELATED_TO" }],
  });
  assert.equal(graph.links[0].target, "o");
});

test("source records do not determine relationship direction", () => {
  assert.equal(relationshipDirection("out"), "outgoing");
  assert.equal(relationshipDirection("in"), "incoming");
  assert.equal(relationshipDirection(undefined), "unknown");
  assert.deepEqual(
    sourceDocumentIds({
      source_doc: 2,
      source_doc_ids: [2, "3", null, "", true, -1, "1.5"],
    }),
    [2, 3],
  );
});

test("source strings cannot inject HTML into canvas library tooltips", () => {
  assert.equal(
    escapeGraphLabel('<img src=x onerror="bad">'),
    "&lt;img src=x onerror=&quot;bad&quot;&gt;",
  );
});

test("graph metadata is detached from response objects", () => {
  const graph = mergeGraph(emptyGraph(), payload);
  graph.links[0].props.source_doc = 90;
  assert.equal(payload.relationships[0].props.source_doc, 1);
});
