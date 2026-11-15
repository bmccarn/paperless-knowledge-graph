import test from "node:test";
import assert from "node:assert/strict";
import { answerState, answerView, formatSourceDate, plainAnswer, scoreLevel } from "../.test-build/answer-state.js";
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

test("an older answer without a disposition uses its known verification status", () => {
  assert.equal(answerState({ verification: { status: "verified" } }).state, "verified");
  assert.equal(answerState({ verification: { status: "unsupported" } }).state, "withheld");
  assert.deepEqual(answerState({ verification: { status: "needs_review" } }), { state: "not_checked" });
  // The disposition wins over a stale status.
  assert.equal(answerState({ finalization: { disposition: "timeout" }, verification: { status: "verified" } }).state, "withheld");
});

const ref = id => ({ document_id: id });
test("each state gets its reason sentence and level, never from the answer text", () => {
  const view = message => answerView({ content: "", ...message });
  assert.deepEqual(view({ finalization: { disposition: "supported" }, evidence: { level: "high" },
    claim_ledger: { claims: [{ references: [ref(1), ref(2)] }, { references: [ref(2)] }] } }),
  { state: "verified", reason: "All 2 claims match passages in 2 documents.", level: "high", prose: "" });
  assert.equal(view({ finalization: { disposition: "supported" }, claim_ledger: { claims: [{ references: [ref(1)] }] } }).reason,
    "The claim matches a passage in 1 document.");
  assert.equal(view({ finalization: { disposition: "qualified" }, source_dates: { 1: "2025-12-31", 2: "2026-02-03" } }).reason,
    "These facts are documented. The newest source is dated 3 Feb 2026. The sources do not show whether they are still true today.");
  assert.equal(view({ finalization: { disposition: "qualified" }, source_dates: {} }).reason,
    "These facts are documented. The sources do not show whether they are still true today.");
  assert.equal(view({ finalization: { disposition: "partial" }, verification: { partial: { omitted_count: 2, original_total: 5 } } }).reason,
    "2 of 5 claims were left out because they did not match a source passage.");
  assert.equal(view({ finalization: { disposition: "partial" }, verification: { partial: { omitted_count: 1, original_total: 2 } } }).reason,
    "1 of 2 claims was left out because it did not match a source passage.");
  assert.equal(view({ finalization: { disposition: "timeout" }, verification: { missing_evidence: ["The source audit exceeded its time budget."] } }).reason,
    "The source audit exceeded its time budget.");
  assert.deepEqual(view({ content: "Unsupported draft", failed: true, confidence: 0.8 }),
    { state: "failed", reason: "The answer failed. Your question was not answered.", level: undefined, prose: null });
});

test("a legacy answer is Not checked with the level of its stored score", () => {
  assert.deepEqual(answerView({ content: "Stored text.", confidence: 0.8 }), { state: "not_checked",
    reason: "This answer was saved before the current source checks. Ask again to check it.", level: "high", prose: "Stored text." });
  assert.equal(answerView({ content: "", evidence: { score: 0.65 } }).level, "medium");
  assert.equal(answerView({ content: "", evidence: { level: "unknown", score: 0.25 } }).level, "low");
});

test("withheld, retry and failed answers show no prose; the others prefer the candidate text", () => {
  const answer = "Draft [Document 4](/documents/4)";
  for (const disposition of ["unsupported", "incomplete", "current_unresolved", "timeout", "audit_failed", "corpus_changed"]) {
    assert.equal(answerView({ content: answer, finalization: { disposition }, claim_ledger: { candidate_text: "Draft" } }).prose, null, disposition);
  }
  assert.equal(answerView({ content: answer, finalization: { disposition: "supported" }, claim_ledger: { candidate_text: "Candídate 🧾" } }).prose, "Candídate 🧾");
  assert.equal(answerView({ content: answer, finalization: { disposition: "stored_binding_unavailable" } }).prose, "Draft");
});

test("the stored answer loses its citations, notices and Fast prefix", () => {
  // The backend's render_verified_answer output for two claims, partial and qualified.
  const candidate = "- The premium is $25.\n\n- The 🧾 receipt is dated 2026-01-31.";
  const cited = "- The premium is $25. [Document 101](/documents/101)\n\n- The 🧾 receipt is dated 2026-01-31. [Document 101](/documents/101) [Document 7](/documents/7)";
  assert.equal(plainAnswer(cited), candidate);
  assert.equal(plainAnswer(cited + "\n\nPartial answer: some claims could not be verified and were omitted. This does not answer every part of your question."), candidate);
  assert.equal(plainAnswer(cited + "\n\nThese are documented facts. Current status as of 2026-09-04T12:00:00+00:00 is not established by the retrieved evidence."), candidate);
  assert.equal(plainAnswer("Unaudited quick answer — verify the source documents before relying on it.\n\n" + candidate), candidate);
  assert.equal(formatSourceDate("2026-09-01"), "1 Sep 2026");
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
