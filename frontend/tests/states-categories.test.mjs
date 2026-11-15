import test from "node:test";
import assert from "node:assert/strict";
import { answerState, scoreLevel } from "../.test-build/answer-state.js";
import { documentType, nodeKind } from "../.test-build/categories.js";

const complete = disposition => ({ finalization: { disposition } });

test("every backend disposition maps to its answer state", () => {
  const expected = {
    supported: "verified", qualified: "documented", partial: "partial",
    unsupported: "withheld", incomplete: "withheld", current_unresolved: "withheld", timeout: "withheld", audit_failed: "withheld",
    unaudited: "not_checked", stored_binding_unavailable: "not_checked", corpus_changed: "retry",
  };
  assert.equal(Object.keys(expected).length, 11);
  for (const [disposition, state] of Object.entries(expected)) {
    assert.equal(answerState(complete(disposition)).state, state, disposition);
  }
});

test("the two Not checked dispositions and Retry needed carry their fixed reasons", () => {
  assert.equal(answerState(complete("unaudited")).reason,
    "Fast mode does not check claims. Verify against the sources before relying on this.");
  assert.equal(answerState(complete("stored_binding_unavailable")).reason,
    "This saved answer could not be re-verified. Ask again to check it.");
  assert.equal(answerState(complete("corpus_changed")).reason,
    "The document index changed during the check. Ask again after indexing finishes.");
});

test("no complete event (stream error, HTTP error or dropped stream) is Failed", () => {
  for (const missing of [null, undefined]) {
    assert.deepEqual(answerState(missing),
      { state: "failed", reason: "The answer failed. Your question was not answered." });
  }
});

test("a complete event without a known disposition is never shown as checked", () => {
  for (const event of [{}, { finalization: {} }, complete("something_new")]) {
    assert.deepEqual(answerState(event), { state: "not_checked" });
  }
});

test("scores map to the evidence level the backend's fixed scores imply", () => {
  assert.deepEqual([0.8, 0.75, 0.65, 0.5, 0.49, 0.25, 0].map(scoreLevel),
    ["high", "high", "medium", "medium", "low", "low", "low"]);
});

test("every node label in the palette table maps to its kind and outline", () => {
  const expected = {
    Person: ["people", false], Organization: ["org", false],
    Location: ["place", false], Address: ["place", true],
    FinancialItem: ["money", false],
    Contract: ["agree", false], InsurancePolicy: ["agree", true],
    Product: ["thing", false], System: ["thing", true],
    Condition: ["health", false], MedicalResult: ["health", true],
    Event: ["time", false], DateEvent: ["time", true],
    Document: ["docs", false], DocumentRef: ["docs", true],
  };
  for (const [label, [kind, outlined]] of Object.entries(expected)) {
    assert.deepEqual(nodeKind(label), { kind, outlined }, label);
  }
});

test("any other node label is unknown and filled", () => {
  for (const label of ["Medical_Result", "person", "Vehicle", "", "toString", "__proto__"]) {
    assert.deepEqual(nodeKind(label), { kind: "unknown", outlined: false }, label);
  }
});

test("document types use their label and token, other keys their key in sentence case", () => {
  assert.deepEqual(documentType("financial_invoice"), { label: "Invoice", color: "var(--doc-financial-invoice)" });
  assert.deepEqual(documentType("medical_lab"), { label: "Medical", color: "var(--doc-medical-lab)" });
  assert.deepEqual(documentType("vehicle_registration"), { label: "Vehicle registration", color: "var(--fg-subtle)" });
  assert.deepEqual(documentType("constructor"), { label: "Constructor", color: "var(--fg-subtle)" });
});
