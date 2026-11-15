import assert from "node:assert/strict";
import { test } from "node:test";
import { graphUrl, parseGraphState, linkKey, parseLinkKey, graphNavigation } from "../.test-build/graph-url-state.js";

test("focus, every view and selection round-trip without losing later graph parameters", () => {
  const filters = new URLSearchParams("fam=parties,money&support=inferred&minconf=0.70&noscore=1&mindocs=2&from=2024-01&to=2026-11&dtype=insurance&owner=hide&a=a&b=b&maxlen=4&tab=evidence&q=Harbor+%26+Sons");
  for (const view of ["neighborhood", "table", "path"]) {
    for (const selection of [null, "entity-a", linkKey("entity-a", "PAID_TO", "entity-b")]) {
      const state = { focus: "entity-a", view, selection };
      const url = new URL(graphUrl(state, filters), "https://example.test");
      assert.deepEqual(parseGraphState(url.searchParams), state);
      for (const [key, value] of filters) assert.equal(url.searchParams.get(key), value);
    }
  }
});

test("link keys match summary keys and preserve direction", () => {
  const key = linkKey("doc-123", "PREPARED_BY", "entity-b");
  assert.equal(key, "doc-123~PREPARED_BY~entity-b");
  assert.deepEqual(parseLinkKey(key), { source: "doc-123", type: "PREPARED_BY", target: "entity-b" });
  assert.notEqual(key, linkKey("entity-b", "PREPARED_BY", "doc-123"));
  assert.equal(new URL(graphUrl({ focus: "entity-b", view: "neighborhood", selection: key }), "https://example.test").searchParams.get("sel"), key);
  for (const bad of ["element-id", "a~TYPE", "a~~b", "a~TYPE~b~c"]) assert.equal(parseLinkKey(bad), null);
});

test("invalid view defaults to neighborhood and absent state stays empty", () => {
  assert.deepEqual(parseGraphState(new URLSearchParams("view=unknown&focus=&sel=")), { focus: null, view: "neighborhood", selection: null });
  assert.equal(graphUrl(parseGraphState(new URLSearchParams())), "/graph");
});

test("focus and view push; selection replaces; focus clears old selection", () => {
  const current = new URLSearchParams("focus=a&view=table&sel=a~PAID_TO~b&fam=money&q=Harbor");
  const next = graphNavigation(current, { focus: "b" });
  assert.equal(next.method, "push");
  assert.deepEqual(parseGraphState(new URL(next.href, "https://example.test").searchParams), { focus: "b", view: "table", selection: null });
  assert.equal(graphNavigation(current, { view: "path" }).method, "push");
  assert.equal(graphNavigation(current, { selection: "b" }).method, "replace");
  assert.equal(graphNavigation(current, { selection: null }).method, "replace");
  const cleared = new URL(graphNavigation(current, { focus: null }).href, "https://example.test");
  assert.equal(cleared.searchParams.has("focus"), false);
  assert.equal(cleared.searchParams.has("sel"), false);
  assert.equal(cleared.searchParams.get("fam"), "money");
});
