import assert from "node:assert/strict";
import { test } from "node:test";
import { graphUrl, parseGraphState, linkKey, parseLinkKey, graphNavigation, GRAPH_VIEWS, listedViews, shownView, drawnView, viewForKey, keyedView, rovingIndex } from "../.test-build/graph-url-state.js";

test("focus, every view and selection round-trip without losing later graph parameters", () => {
  const filters = new URLSearchParams("fam=parties,money&support=inferred&minconf=0.70&noscore=1&mindocs=2&from=2024-01&to=2026-11&dtype=insurance&owner=hide&a=a&b=b&maxlen=4&tab=evidence&q=Harbor+%26+Sons");
  for (const view of ["focus", "explore", "timeline", "table", "path", "overview"]) {
    for (const [selection, cross] of [[null, true], ["entity-a", false], [linkKey("entity-a", "PAID_TO", "entity-b"), true]]) {
      const state = { focus: "entity-a", view, selection, cross, expanded: view === "explore" ? ["entity-b", "entity-c"] : [] };
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
  assert.equal(new URL(graphUrl({ focus: "entity-b", view: "focus", selection: key, cross: true, expanded: [] }), "https://example.test").searchParams.get("sel"), key);
  for (const bad of ["element-id", "a~TYPE", "a~~b", "a~TYPE~b~c"]) assert.equal(parseLinkKey(bad), null);
});

test("invalid view defaults to Focus, older neighborhood links open Focus, and absent state stays empty", () => {
  assert.deepEqual(parseGraphState(new URLSearchParams("view=unknown&focus=&sel=")), { focus: null, view: "focus", selection: null, cross: true, expanded: [] });
  assert.deepEqual(parseGraphState(new URLSearchParams("focus=a&view=neighborhood")), { focus: "a", view: "focus", selection: null, cross: true, expanded: [] });
  assert.equal(graphUrl({ focus: "a", view: "focus", selection: null, cross: true, expanded: [] }, new URLSearchParams("view=neighborhood")), "/graph?focus=a");
  assert.equal(graphUrl(parseGraphState(new URLSearchParams())), "/graph");
});

test("focus and view push; selection replaces; focus clears old selection", () => {
  const current = new URLSearchParams("focus=a&view=table&sel=a~PAID_TO~b&fam=money&q=Harbor");
  const next = graphNavigation(current, { focus: "b" });
  assert.equal(next.method, "push");
  assert.deepEqual(parseGraphState(new URL(next.href, "https://example.test").searchParams), { focus: "b", view: "table", selection: null, cross: true, expanded: [] });
  assert.equal(graphNavigation(current, { view: "path" }).method, "push");
  assert.equal(graphNavigation(current, { selection: "b" }).method, "replace");
  assert.equal(graphNavigation(current, { selection: null }).method, "replace");
  const cleared = new URL(graphNavigation(current, { focus: null }).href, "https://example.test");
  assert.equal(cleared.searchParams.has("focus"), false);
  assert.equal(cleared.searchParams.has("sel"), false);
  assert.equal(cleared.searchParams.get("fam"), "money");
});

test("the cross-link switch writes cross=0 when off, replaces the history entry and survives a focus change", () => {
  const off = graphNavigation(new URLSearchParams("focus=a&sel=b"), { cross: false });
  assert.equal(off.method, "replace");
  assert.equal(off.href, "/graph?focus=a&sel=b&cross=0");
  assert.equal(graphNavigation(new URLSearchParams("focus=a&cross=0"), { cross: true }).href, "/graph?focus=a");
  assert.equal(parseGraphState(new URLSearchParams("cross=1")).cross, true);
  assert.equal(new URL(graphNavigation(new URLSearchParams("focus=a&cross=0"), { focus: "b" }).href, "https://example.test").searchParams.get("cross"), "0");
});

// A registry with the later views turned on, so the switcher rules are proven before those views ship.
const allViews = GRAPH_VIEWS.map(view => ({ ...view, listed: view.id !== "overview" }));

test("the switcher order is fixed and only shipped views are listed", () => {
  assert.deepEqual(GRAPH_VIEWS.map(view => view.label), ["Focus", "Explore", "Timeline", "Table", "Paths", "Overview"]);
  assert.deepEqual(listedViews().map(view => view.id), ["focus", "explore", "timeline", "table", "path"]);
  assert.deepEqual(listedViews(allViews).map(view => view.id), ["focus", "explore", "timeline", "table", "path"]);
  assert.equal(shownView("overview"), "focus", "an unlisted view in the URL draws as Focus");
  assert.equal(shownView("explore"), "explore");
  assert.equal(shownView("table"), "table");
  assert.equal(shownView("table", allViews), "table");
  assert.deepEqual(GRAPH_VIEWS.filter(view => !view.needsFocus).map(view => view.id), ["path", "overview"]);
});

test("keys 1 to 6 follow the switcher order, n t p are aliases, and unlisted views ignore their key", () => {
  assert.equal(viewForKey("1", allViews), "focus");
  assert.equal(viewForKey("2", allViews), "explore");
  assert.equal(viewForKey("3", allViews), "timeline");
  assert.equal(viewForKey("4", allViews), "table");
  assert.equal(viewForKey("5", allViews), "path");
  assert.equal(viewForKey("6", allViews), null, "Overview waits for its acceptance ticket");
  assert.equal(viewForKey("n", allViews), "focus");
  assert.equal(viewForKey("t", allViews), "table");
  assert.equal(viewForKey("p", allViews), "path");
  for (const key of ["0", "7", "x", "N", "T", "Enter"]) assert.equal(viewForKey(key, allViews), null);
  // Focus, Explore, Table and Paths ship.
  assert.equal(viewForKey("1"), "focus");
  assert.equal(viewForKey("2"), "explore");
  assert.equal(viewForKey("n"), "focus");
  assert.equal(viewForKey("4"), "table");
  assert.equal(viewForKey("t"), "table");
  assert.equal(viewForKey("5"), "path");
  assert.equal(viewForKey("p"), "path");
  for (const key of ["6"]) assert.equal(viewForKey(key), null);
});

test("Left and Right move through the toolbar and wrap; Home and End jump", () => {
  assert.equal(rovingIndex(0, "ArrowRight", 5), 1);
  assert.equal(rovingIndex(4, "ArrowRight", 5), 0);
  assert.equal(rovingIndex(0, "ArrowLeft", 5), 4);
  assert.equal(rovingIndex(3, "Home", 5), 0);
  assert.equal(rovingIndex(1, "End", 5), 4);
  for (const key of ["ArrowUp", "ArrowDown", "Tab", "a"]) assert.equal(rovingIndex(2, key, 5), null);
});

test("a view change pushes one entry and keeps a selection the new view can hold", () => {
  const link = linkKey("a", "PAID_TO", "b");
  const views = (sel, view, from = "") => parseGraphState(new URL(graphNavigation(new URLSearchParams(`focus=a&sel=${sel}&fam=money${from}`), { view }).href, "https://example.test").searchParams);
  const first = graphNavigation(new URLSearchParams("focus=a"), { view: "explore" });
  assert.equal(first.method, "push");
  assert.equal(first.href, "/graph?focus=a&view=explore");
  assert.equal(graphNavigation(new URLSearchParams("focus=a&view=explore"), { view: "focus" }).href, "/graph?focus=a");
  assert.equal(views("b", "timeline").selection, "b");
  assert.equal(views(link, "table").selection, link);
  // A pill exists only in Focus, and a group only in Overview.
  assert.equal(views("pill:parties", "explore").selection, null);
  assert.equal(views("pill:parties", "focus", "&view=table").selection, "pill:parties");
  assert.equal(views("grp:a", "focus", "&view=overview").selection, null);
  assert.equal(views("grp:a", "overview").selection, "grp:a");
  assert.equal(new URL(graphNavigation(new URLSearchParams("focus=a&sel=pill:x&fam=money"), { view: "table" }).href, "https://example.test").searchParams.get("fam"), "money");
});

test("keys 1 and 2 switch views with one pushed entry each, and do nothing in a text field", () => {
  const press = key => ({ key, ctrlKey: false, metaKey: false, altKey: false, defaultPrevented: false });
  const twoViews = GRAPH_VIEWS.map(view => ({ ...view, listed: view.id === "focus" || view.id === "explore" }));
  let params = new URLSearchParams("focus=a&sel=b");
  assert.equal(keyedView(press("1"), false, parseGraphState(params), twoViews), null, "Focus is already shown");
  for (const [key, view] of [["2", "explore"], ["1", "focus"]]) {
    assert.equal(keyedView(press(key), true, parseGraphState(params), twoViews), null);
    assert.equal(keyedView(press(key), false, parseGraphState(params), twoViews), view);
    const next = graphNavigation(params, { view });
    assert.equal(next.method, "push");
    params = new URL(next.href, "https://example.test").searchParams;
    assert.equal(parseGraphState(params).view, view);
  }
  for (const modifier of ["ctrlKey", "metaKey", "altKey", "defaultPrevented"]) assert.equal(keyedView({ ...press("2"), [modifier]: true }, false, parseGraphState(params), twoViews), null);
  assert.equal(keyedView(press("2"), false, parseGraphState(new URLSearchParams()), twoViews), null, "Explore needs a focus entity");
  assert.equal(keyedView(press("5"), false, parseGraphState(new URLSearchParams()), allViews), "path", "Paths has its own pickers");
  // Overview draws from its URL before #204 lists it; key 1 leaves it for Focus.
  assert.equal(drawnView("overview"), "overview");
  assert.equal(drawnView("timeline"), "timeline");
  const overview = parseGraphState(new URLSearchParams("focus=a&view=overview"));
  assert.equal(keyedView(press("1"), false, overview), "focus");
  assert.equal(keyedView(press("6"), false, overview), null, "Overview is unlisted, so key 6 does nothing");
  // Unlisted views ignore their keys. Table aliases return to Focus.
  for (const key of ["6", "t"]) assert.equal(keyedView(press(key), false, parseGraphState(new URLSearchParams("focus=a&view=table"))), null);
  assert.equal(keyedView(press("2"), false, parseGraphState(new URLSearchParams("focus=a&view=table"))), "explore");
  assert.equal(keyedView(press("3"), false, parseGraphState(new URLSearchParams("focus=a&view=table"))), "timeline");
  // p opens Paths, with or without a focus, and does nothing in a text field.
  for (const query of ["focus=a&view=table", "", "q=Harbor"]) {
    assert.equal(keyedView(press("p"), false, parseGraphState(new URLSearchParams(query))), "path");
    assert.equal(keyedView(press("p"), true, parseGraphState(new URLSearchParams(query))), null);
  }
  assert.equal(keyedView(press("p"), false, parseGraphState(new URLSearchParams("view=path"))), null, "Paths is already shown");
  for (const key of ["1", "n"]) assert.equal(keyedView(press(key), false, parseGraphState(new URLSearchParams("focus=a&view=table"))), "focus");
});

test("a switch to Paths sets A to the focus when A is empty, and keeps a chosen A", () => {
  const to = (query, change = { view: "path" }) => new URL(graphNavigation(new URLSearchParams(query), change).href, "https://example.test").searchParams;
  assert.equal(to("focus=f").get("a"), "f");
  assert.equal(to("focus=f&a=").get("a"), "f");
  assert.equal(to("focus=f&a=x&b=y").get("a"), "x");
  assert.equal(to("focus=f&a=x&b=y").get("b"), "y");
  assert.equal(to("").has("a"), false, "no focus leaves A empty");
  assert.equal(to("focus=f", { view: "table" }).has("a"), false);
  assert.equal(to("focus=f&view=path&a=x", { focus: "g" }).get("a"), "x", "only a switch to Paths fills A");
});

test("an Explore expansion or collapse is a history step; a selection keeps x and a new focus clears it", () => {
  const current = new URLSearchParams("focus=a&view=explore&x=b,c&sel=c");
  assert.deepEqual(parseGraphState(current).expanded, ["b", "c"]);
  const expand = graphNavigation(current, { expanded: ["b", "c", "d"] });
  assert.equal(expand.method, "push");
  assert.equal(new URL(expand.href, "https://example.test").searchParams.get("x"), "b,c,d");
  assert.equal(graphNavigation(current, { expanded: ["b"] }).method, "push");
  const select = graphNavigation(current, { selection: "d" });
  assert.equal(select.method, "replace");
  assert.deepEqual(parseGraphState(new URL(select.href, "https://example.test").searchParams).expanded, ["b", "c"]);
  const refocus = graphNavigation(current, { focus: "c" });
  assert.equal(new URL(refocus.href, "https://example.test").searchParams.has("x"), false);
  assert.equal(graphNavigation(current, { expanded: [] }).href, "/graph?focus=a&view=explore&sel=c");
  const many = Array.from({ length: 25 }, (_, index) => `e${index}`).join(",");
  assert.equal(parseGraphState(new URLSearchParams(`focus=a&x=${many}`)).expanded.length, 20, "x holds at most 20 ids");
});
