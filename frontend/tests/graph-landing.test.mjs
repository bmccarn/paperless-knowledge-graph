import { test } from "node:test";
import assert from "node:assert/strict";
import {
  addRecent,
  defaultFocus,
  entityName,
  entityRow,
  exactMatch,
  groupResults,
  highlight,
  markTerms,
  mostConnected,
  parseRecent,
} from "../.test-build/graph-landing.js";

const raw = (uuid, label, name, extra = {}) => ({ labels: [label], properties: { uuid, name }, evidence_doc_count: 2, ...extra });
const row = (id, label, name = id, owner = false, ownerMatch = false) => ({ id, label, name, documents: 1, alias: null, owner, ownerMatch });

test("search rows keep entities only and read the derived fields", () => {
  assert.deepEqual(entityRow(raw("a", "Organization", "Harbor", { matched_alias: "HM", is_owner: true })),
    { id: "a", label: "Organization", name: "Harbor", documents: 2, alias: "HM", owner: true, ownerMatch: false });
  assert.equal(entityRow(raw("d", "Person", "Harbor", { owner_match: true })).ownerMatch, true);
  assert.equal(entityRow({ labels: ["Document"], properties: { paperless_id: 4 } }), null);
  assert.equal(entityRow(raw("r", "DocumentRef", "Referenced letter")), null);
  assert.equal(entityRow({ labels: ["Person"], properties: { name: "No id" } }), null);
  assert.equal(entityRow(raw("b", "Person", "Dana")).documents, 2);
  assert.equal(entityRow({ labels: ["Person"], properties: { uuid: "c" } }).documents, 0);
});

test("an unnamed entity shows its kind and a short id, never the id", () => {
  assert.equal(entityName(row("3f2a9c1e-77aa-4c1b-9d00-1234567890ab", "InsurancePolicy", "")), "Insurance policy 3f2a9c");
  assert.equal(entityName(row("abc", "Person", "abc")), "Person abc");
  assert.equal(entityName(row("abc", "Person", "Dana")), "Dana");
});

test("typeahead shows at most 8 results grouped by kind in the fixed order", () => {
  const rows = [row("1", "Address"), row("2", "Person"), row("3", "InsurancePolicy"), row("4", "Organization"),
    row("5", "FinancialItem"), row("6", "Gadget"), row("7", "Person"), row("8", "DateEvent"), row("9", "Person")];
  const groups = groupResults(rows);
  assert.deepEqual(groups.map(group => [group.title, group.rows.map(item => item.id)]), [
    ["People", ["2", "7"]], ["Organizations", ["4"]], ["Money and agreements", ["3", "5"]], ["Places", ["1"]],
    ["Time", ["8"]], ["Other", ["6"]],
  ]);
  assert.equal(groups.flatMap(group => group.rows).length, 8);
});

test("?q= opens an entity only when exactly one name equals the text", () => {
  assert.equal(exactMatch([row("1", "Person", "Dana Whitlock"), row("2", "Person", "Dana Whitlock Jr")], " dana whitlock ").id, "1");
  assert.equal(exactMatch([row("1", "Person", "Dana"), row("2", "Organization", "DANA")], "dana"), null);
  assert.equal(exactMatch([row("1", "Person", "Dana Whitlock")], "dana"), null);
});

test("most connected leaves out the owner and its other matches, and keeps 10", () => {
  const rows = [row("owner", "Person", "Owner", true), row("copy-1", "Person", "Owner", false, true),
    ...Array.from({ length: 11 }, (_, index) => row(String(index), "Organization")), row("copy-2", "Person", "Owner", false, true)];
  const top = mostConnected(rows);
  assert.equal(top.length, 10);
  assert.deepEqual(top.map(item => item.id), Array.from({ length: 10 }, (_, index) => String(index)));
});

test("/graph opens the owner, else the first Most connected, else nothing", () => {
  const rows = [row("dup", "Person", "Owner", false, true), row("top", "Organization"), row("next", "Person")];
  assert.equal(defaultFocus("owner-id", rows), "owner-id");
  // The owner rule found no owner: its other matches still stay out.
  assert.equal(defaultFocus(null, rows), "top");
  assert.equal(defaultFocus(undefined, [row("dup", "Person", "Owner", false, true)]), null);
  assert.equal(defaultFocus(null, []), null);
});

test("Recent puts the latest first, once, and keeps 10", () => {
  let list = [];
  for (let index = 0; index < 12; index++) list = addRecent(list, { id: String(index), name: "", label: "Person", documents: 0 });
  list = addRecent(list, { id: "5", name: "Again", label: "Person", documents: 3 });
  assert.equal(list.length, 10);
  assert.deepEqual(list.slice(0, 3).map(item => item.id), ["5", "11", "10"]);
  assert.equal(list.filter(item => item.id === "5").length, 1);
});

test("stored Recent that is malformed reads as empty", () => {
  assert.deepEqual(parseRecent(null), []);
  assert.deepEqual(parseRecent("{"), []);
  assert.deepEqual(parseRecent('{"id":"x"}'), []);
  assert.deepEqual(parseRecent('[{"id":"x","label":"Person"},{"id":3},null]').map(item => item.id), ["x"]);
});

test("highlight marks each query word without changing the text", () => {
  const parts = highlight("Harbor Mutual harbor", "harbor mut");
  assert.deepEqual(parts, [{ text: "Harbor", match: true }, { text: " ", match: false }, { text: "Mut", match: true },
    { text: "ual ", match: false }, { text: "harbor", match: true }]);
  assert.equal(highlight("<b>Acme</b>", "acme").map(part => part.text).join(""), "<b>Acme</b>");
  assert.deepEqual(highlight("Dana", ""), [{ text: "Dana", match: false }]);
});

test("markTerms marks whole names, the longest first, and skips empty names", () => {
  const parts = markTerms("Policy HM-2208 (auto) covers the 2014 HONDA FIT.", ["Policy HM-2208 (auto)", "2014 Honda Fit", "", "Policy"]);
  assert.deepEqual(parts, [{ text: "Policy HM-2208 (auto)", match: true }, { text: " covers the ", match: false },
    { text: "2014 HONDA FIT", match: true }, { text: ".", match: false }]);
  assert.deepEqual(markTerms("A <b>quote</b>", ["", ""]), [{ text: "A <b>quote</b>", match: false }]);
});
