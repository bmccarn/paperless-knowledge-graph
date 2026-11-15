import { test } from "node:test";
import assert from "node:assert/strict";
import {
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

test("missing identities are reported, never fabricated", () => {
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

