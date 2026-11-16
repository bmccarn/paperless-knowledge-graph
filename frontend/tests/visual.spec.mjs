import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { test, expect } from "playwright/test";
import { unexpectedViolations, visualMode } from "./visual-policy.mjs";

const require = createRequire(import.meta.url);
const exceptions = JSON.parse(await readFile(new URL("./accessibility-exceptions.json", import.meta.url)));
const mode = visualMode();
const fixedTime = new Date("2026-09-04T12:00:00Z");
const mainStates = [
  { name: "ask", route: "/query", ready: page => page.getByPlaceholder("Ask a question...") },
  // The empty Recent answers line appears only after the status and all three columns have loaded.
  { name: "status", route: "/", ready: page => page.getByText("No answers yet.", { exact: false }) },
  { name: "documents", route: "/documents", ready: page => page.getByText("303 indexed documents", { exact: true }) },
  { name: "document", route: "/documents/101", ready: page => page.getByRole("heading", { name: "January premium statement", exact: true }) },
  { name: "entities", route: "/entities/review", ready: page => page.getByText("Cedar Lab 1", { exact: true }) },
  { name: "graph", route: "/graph", ready: page => page.getByRole("region", { name: "Most connected", exact: true }).getByRole("link").first() },
  { name: "hubs", route: "/hubs", ready: page => page.getByText("303 matching indexed documents", { exact: true }) },
  { name: "logs", route: "/debug", ready: page => page.getByText("Connected", { exact: true }) },
  { name: "not-found", route: "/visual-missing-page", ready: page => page.getByRole("heading", { name: "Page not found", exact: true }) },
  // The links between neighbors come from a second request: wait for its caption, so the curves are in every capture.
  { name: "graph-neighborhood", route: "/graph?focus=nb-rowan", ready: page => page.getByText("13 links between neighbors.", { exact: true }) },
];
mainStates.push({ name: "graph-table", route: "/graph?focus=nb-rowan&view=table", ready: page => page.getByRole("table", { name: "Links of Rowan Vale" }) });
mainStates.push({ name: "graph-filters", route: "/graph?focus=nb-rowan&support=supported&minconf=0.70&from=2026-01", ready: page => page.getByText("10 links between neighbors.", { exact: true }) });
const pathRoute = "/graph?focus=nb-money-0&view=path&a=nb-money-0&b=nb-places-1";
// The best quotes arrive per link after the paths; path 1 has three links.
mainStates.push({ name: "graph-path", route: pathRoute, ready: page => page.getByRole("list", { name: "Steps of the path" }).locator("blockquote").nth(2) });
const gallery = { name: "gallery", route: "/gallery", ready: page => page.getByRole("heading", { name: "Component gallery", exact: true }) };
const statusWith = (extra, routes = {}) => async page => {
  const healthy = await (await page.request.get("/api/status")).json();
  await page.route("**/api/status", route => route.fulfill({ json: { ...healthy, ...extra } }));
  for (const [path, json] of Object.entries(routes)) await page.route(`**/api${path}`, route => route.fulfill({ json }));
};
const graphText = page => page.addInitScript(() => {
  window.__graphText = {};
  const fillText = CanvasRenderingContext2D.prototype.fillText;
  CanvasRenderingContext2D.prototype.fillText = function (text, ...rest) {
    const point = new DOMPoint(0, 0).matrixTransform(this.getTransform());
    if (this.canvas.isConnected) window.__graphText[text] = { x: point.x / devicePixelRatio, y: point.y / devicePixelRatio };
    return fillText.call(this, text, ...rest);
  };
});
const graphReady = async page => {
  await page.waitForFunction(() => window.__visualGraphPainted === true);
  // Let the initial fit run and the renderer refresh its hit canvas (at most every 800 ms).
  await page.clock.runFor(1000);
};

const extraStates = [
  { ...mainStates[11], name: "graph-filters-types", state: "filter-types", prepare: async page => {
    await page.getByRole("button", { name: /^Filters/ }).filter({ visible: true }).click();
    const types = page.getByRole("dialog", { name: "Filters", exact: true }).getByRole("group", { name: "Document type", exact: true });
    await types.scrollIntoViewIfNeeded();
    await types.getByRole("button", { name: /^Work / }).click();
    await page.waitForURL(url => url.searchParams.has("dtype"));
  } },
  { ...mainStates[11], name: "graph-filters-sheet", state: "filters", prepare: async page => {
    await page.getByRole("button", { name: /^Filters/ }).filter({ visible: true }).click();
    await page.getByRole("dialog", { name: "Filters", exact: true }).waitFor();
  } },
  { name: "graph-missing", route: "/graph?focus=removed-entity",
    ready: page => page.getByText("This entity no longer exists. It may have been merged.", { exact: true }) },
  ...["initial", "results", "empty", "error"].map(state => ({
    ...mainStates[0], name: `palette-${state}`, state: `palette-${state}`,
    seed: async page => { await page.request.post("/api/_fixture/saved-answers"); },
    prepare: async page => {
      await page.getByRole("button", { name: "Search Ctrl K, pages and archive", exact: true }).click();
      const input = page.getByRole("combobox", { name: "Search pages and archive", exact: true });
      if (state !== "initial") {
        await input.fill(state === "results" ? "Example" : state === "error" ? "fixture-error" : "unmatched-archive-term");
        await page.clock.runFor(200);
        await page.getByText(state === "error" ? "Some searches failed." : state === "empty" ? "0 results" : "2 results", { exact: true }).waitFor();
      }
    },
  })),
  ...["empty", "error"].map(state => ({
    name: `logs-${state}`, route: "/debug", state,
    seed: page => page.route("**/api/logs?**", route => route.fulfill(state === "error"
      ? { status: 503, json: { detail: "Synthetic history failure" } }
      : { json: { lines: [] } })),
    ready: page => state === "error" ? page.getByRole("alert").filter({ hasText: "Logs unavailable" }) : page.getByText("No log lines yet.", { exact: true }),
  })),
  ...["empty", "error"].map(state => ({
    name: `hubs-${state}`, route: "/hubs", state,
    seed: page => page.route("**/api/graph/search?**", route => route.fulfill(state === "error"
      ? { status: 503, json: { detail: "Synthetic hub search failure" } }
      : { json: { results: [], total: 0 } })),
    ready: page => state === "error" ? page.getByRole("alert").filter({ hasText: "Could not load documents" }) : page.getByText("No documents found for this hub search.", { exact: true }),
  })),

  { ...mainStates[0], name: "ask-model", state: "model-select", prepare: async page => {
    await page.getByRole("combobox", { name: "Model", exact: true }).click();
    await page.getByRole("listbox").waitFor();
  } },
  { ...mainStates[0], name: "ask-delete", state: "delete-confirmation",
    seed: page => page.request.post("/api/_fixture/saved-answers"), prepare: async page => {
      const history = page.getByRole("button", { name: "Open conversations", exact: true });
      if (await history.isVisible()) await history.click();
      await page.getByRole("button", { name: "Delete Older verified answer", exact: true }).click();
      await page.getByRole("alertdialog").waitFor();
  } },
  { ...mainStates[0], name: "ask-conversations", state: "conversations",
    seed: page => page.request.post("/api/_fixture/saved-answers"), prepare: async page => {
      const history = page.getByRole("button", { name: "Open conversations", exact: true });
      if (await history.isVisible()) await history.click();
      await page.getByRole("button", { name: "Older verified answer", exact: true }).waitFor();
  } },
  { ...mainStates[1], name: "status-answers", state: "recent-answers", seed: page => page.request.post("/api/_fixture/saved-answers"),
    ready: page => page.getByRole("region", { name: "Recent answers" }).getByText("Verified", { exact: true }) },
  { ...mainStates[1], name: "status-stale", state: "stale", ready: page => page.getByRole("button", { name: "Sync now", exact: true }),
    seed: statusWith({ freshness: {
      stale: true, paperless_documents: 310, indexed_documents: 303, missing_documents: 7, missing_embedding_documents: 2,
      latest_paperless_title: "Synthetic late arrival", latest_paperless_modified: "2026-09-04T11:30:00+00:00",
      drift: { missing_from_graph: [{ id: 9400, title: "Synthetic late arrival" }], missing_embeddings: [{ id: 9401 }] },
    } }) },
  { ...mainStates[1], name: "status-task", state: "task", ready: page => page.getByRole("log", { name: "Sync log" }),
    seed: statusWith({ active_tasks: { "visual-sync": { status: "running", type: "sync" } } }, { "/task/visual-sync": {
      status: "running", total_docs: 303, processed: 41, skipped: 2, errors: 1, current_doc: "Synthetic archive 044",
      elapsed_seconds: 30, docs_per_minute: 88, estimated_remaining_seconds: 175,
      recent_results: Array.from({ length: 12 }, (_, index) => ({ doc_id: 9032 + index, title: `Synthetic archive ${String(32 + index).padStart(3, "0")}`,
        status: index === 4 ? "error" : index === 7 ? "skipped" : "processed", entities: 3, error: "Synthetic extraction timeout" })),
    } }) },
  { ...mainStates[1], name: "status-reindex", state: "reindex-dialog", prepare: async page => {
    await page.getByRole("button", { name: "Full reindex", exact: true }).click();
    await page.getByRole("alertdialog").waitFor();
  } },
  { ...mainStates[3], name: "document-ocr", state: "ocr-open", prepare: async page => {
    await page.getByRole("button", { name: "Original OCR text" }).click();
    await page.getByRole("region", { name: "Original OCR text" }).focus();
  } },
  ...["feedback", "resolution"].map(state => ({ ...mainStates[3], name: `document-${state}`, state, prepare: async page => {
    await page.getByRole("textbox", { name: "Extraction review report" }).fill("Check the premium against the original statement.");
    await page.getByRole("button", { name: "Mark extraction wrong", exact: true }).click();
    const resolution = page.getByRole("combobox", { name: "Resolution", exact: true });
    await resolution.scrollIntoViewIfNeeded();
    if (state === "resolution") await resolution.click();
  } })),
  { name: "document-missing", route: "/documents/999999", state: "missing", ready: page => page.getByRole("heading", { name: "Document not found", exact: true }) },
  { name: "document-empty", route: "/documents/102", state: "empty", ready: page => page.getByText("No entities extracted.", { exact: false }) },
  { ...mainStates[0], name: "phone-more", state: "more-sheet", phoneOnly: true, prepare: async page => {
    await page.getByRole("button", { name: "More", exact: true }).click();
    await page.getByRole("dialog", { name: "More", exact: true }).waitFor();
  } },
  ...["dialog", "sheet"].map(kind => ({ ...gallery, name: `gallery-${kind}`, state: kind, prepare: async page => {
    await page.getByRole("button", { name: `Open ${kind}`, exact: true }).click();
    await page.getByRole("dialog").waitFor();
  } })),
  { ...gallery, name: "gallery-tooltip", state: "tooltip", prepare: async page => {
    await page.getByRole("button", { name: "Add to archive", exact: true }).hover();
    await page.clock.runFor(300);
    await page.getByRole("tooltip").waitFor();
  } },
  { ...gallery, name: "gallery-view-sheet", state: "view-sheet", phoneOnly: true, prepare: async page => {
    await page.getByRole("button", { name: "View: Focus", exact: true }).click();
    await page.getByRole("dialog", { name: "View", exact: true }).waitFor();
  } },
  { ...mainStates[0], name: "ask-history", state: "history-sheet", phoneOnly: true, prepare: async page => {
    await page.getByRole("button", { name: "Open conversations", exact: true }).click();
    await page.getByRole("dialog").waitFor();
  } },
  { ...gallery, name: "gallery-alert-dialog", state: "alert-dialog", prepare: async page => {
    await page.getByRole("button", { name: "Open alert dialog", exact: true }).click();
    await page.getByRole("alertdialog").waitFor();
  } },
  { ...mainStates[4], name: "entities-dialog", state: "merge-dialog", prepare: async page => {
    await page.getByRole("button", { name: "Merge", exact: true }).first().click();
    await page.getByRole("alertdialog").waitFor();
  } },
  { ...mainStates[4], name: "entities-merged", state: "merged-toast", prepare: async page => {
    await page.getByRole("button", { name: "Merge", exact: true }).first().click();
    await page.getByRole("alertdialog").getByRole("button", { name: "Merge", exact: true }).click();
    // The toast focuses its Undo, which holds it open through the clock jump below.
    await page.getByRole("status", { name: "Notifications" }).getByRole("button", { name: "Undo", exact: true }).waitFor();
  } },
  { ...mainStates[2], name: "documents-expanded", state: "expanded", prepare: async page => {
    await page.getByRole("button", { name: "Show details for January premium statement", exact: true }).click();
    await page.getByRole("link", { name: "Example Utility", exact: true }).waitFor();
  } },
  { ...mainStates[0], name: "ask-verified", state: "verified", prepare: async page => {
    await page.getByPlaceholder("Ask a question...").fill("What premium does the January statement list?");
    await page.getByPlaceholder("Ask a question...").press("Enter");
    await page.getByRole("button", { name: /^Claim table/ }).waitFor();
  } },
  { ...mainStates[0], name: "ask-claims", state: "claims", prepare: async page => {
    await page.getByPlaceholder("Ask a question...").fill("What does the policy say? margin-claims");
    await page.getByPlaceholder("Ask a question...").press("Enter");
    await page.getByRole("button", { name: "Claim 2", exact: true }).click();
  } },
  { ...mainStates[0], name: "ask-claim-table", state: "claim-table", prepare: async page => {
    await page.getByPlaceholder("Ask a question...").fill("What does the policy say? margin-claims");
    await page.getByPlaceholder("Ask a question...").press("Enter");
    await page.getByRole("button", { name: /^Claim table/ }).click();
    await page.getByRole("button", { name: "Select claim 2", exact: true }).click();
    await page.getByRole("button", { name: "How this was checked", exact: true }).click();
  } },
  { ...mainStates[0], name: "ask-partial-table", state: "partial-claim-table", prepare: async page => {
    await page.getByRole("button", { name: "Timeline", exact: true }).click();
    await page.getByPlaceholder("Ask a question...").fill("Show partial recorded history");
    await page.getByPlaceholder("Ask a question...").press("Enter");
    await page.getByRole("button", { name: /^Claim table/ }).click();
  } },
  { ...mainStates[0], name: "ask-source-sheet", state: "source-sheet", prepare: async page => {
    await page.getByPlaceholder("Ask a question...").fill("What premium does the January statement list?");
    await page.getByPlaceholder("Ask a question...").press("Enter");
    await page.getByRole("region", { name: "Sources" }).getByRole("button", { name: "January premium statement", exact: true }).click();
    await page.getByRole("dialog").waitFor();
  } },
  // One capture per other answer state; the question's token selects the fixture outcome.
  ...[
    ["documented", "Is the premium still current? state-documented"],
    ["partial", "Show partial recorded history", "Timeline"],
    ["withheld", "What premium is due? state-withheld-unsupported"],
    ["not-checked", "What premium does the January statement list?", "Fast"],
    ["retry", "What premium is due? state-retry"],
    ["failed", "Run stream-error synthetic scenario"],
    ["real", "How did my auto insurance change at renewal? real-answer"],
  ].map(([state, question, mode]) => ({ ...mainStates[0], name: `ask-${state}`, state, prepare: async page => {
    if (mode) await page.getByRole("button", { name: mode, exact: true }).click();
    await page.getByPlaceholder("Ask a question...").fill(question);
    await page.getByPlaceholder("Ask a question...").press("Enter");
    await page.getByRole("article", { name: "Answer" }).waitFor();
  } })),
  ...[
    ["saved-unverifiable", "Saved answer that fails re-verification"],
    ["legacy", "Legacy answer without finalization"],
    ["older-verified", "Older verified answer"],
  ].map(([state, title]) => ({ ...mainStates[0], name: `ask-${state}`, state,
    seed: page => page.request.post("/api/_fixture/saved-answers"), prepare: async page => {
    const sheet = page.getByRole("button", { name: "Open conversations", exact: true });
    if (await sheet.isVisible()) await sheet.click();
    await page.getByRole("button", { name: title, exact: true }).click();
    await page.getByRole("article", { name: "Answer" }).waitFor();
  } })),
  { ...mainStates[5], name: "graph-results", state: "results", prepare: async page => {
    await page.getByRole("combobox", { name: "Search entities", exact: true }).fill("ar");
    await page.clock.runFor(200);
    await page.getByRole("listbox", { name: "8 results", exact: true }).waitFor();
  } },
  { ...mainStates[5], name: "graph-recent", state: "recent", seed: page => page.addInitScript(() => localStorage.setItem("graph-recent", JSON.stringify([
    { id: "nb-money-0", name: "Harbor Mutual Insurance", label: "Organization", documents: 10 },
    { id: "nb-coverage-0", name: "Policy HM-2208 (auto)", label: "InsurancePolicy", documents: 6 },
    { id: "nb-unnamed", name: "", label: "Organization", documents: 2 },
    { id: "nb-places-0", name: "14 Larkspur Ct", label: "Address", documents: 6 },
  ]))) },
  // The hub with its focus node selected: selection rings, faded links, arrows and link labels.
  { ...mainStates[9], name: "graph-neighborhood-selected", state: "selected-node", prepare: async page => {
    const canvas = page.locator("canvas").first();
    await canvas.waitFor({ state: "visible" });
    await page.waitForFunction(() => window.__visualGraphPainted === true);
    // Let the initial fit run and the renderer refresh its hit canvas (at most every 800 ms).
    await page.clock.runFor(1000);
    const box = await canvas.boundingBox();
    const zoom = await canvas.evaluate(element => element.__zoom);
    await page.mouse.click(box.x + zoom.x, box.y + zoom.y);
    await page.getByRole("complementary", { name: "Evidence inspector" }).waitFor();
  } },
  // The hub with the parties pill selected: its hidden links in the inspector.
  { ...mainStates[9], name: "graph-neighborhood-group", state: "selected-group", seed: graphText, prepare: async page => {
    await graphReady(page);
    const box = await page.locator("canvas").first().boundingBox();
    // On phone a pill shows only its count.
    const pill = await page.evaluate(() => window.__graphText["+23 Parties and roles"] ?? window.__graphText["+23"]);
    await page.mouse.click(box.x + pill.x, box.y + pill.y);
    await page.getByRole("list", { name: "Parties and roles group" }).waitFor();
  } },
  // On phone, the inspector sheet of a selected neighbor expanded from its 300 px peek.
  { ...mainStates[9], name: "graph-neighborhood-sheet", state: "sheet-expanded", phoneOnly: true, seed: graphText, prepare: async page => {
    await graphReady(page);
    const box = await page.locator("canvas").first().boundingBox();
    const node = await page.evaluate(() => window.__graphText["Juniper Okafor"]);
    await page.mouse.click(box.x + node.x, box.y + node.y);
    await page.getByRole("button", { name: "Expand inspector", exact: true }).click();
    await page.getByRole("button", { name: "Collapse inspector", exact: true }).waitFor();
  } },
  // The legend opened on desktop (a strip above the evidence shelf), and its sheet on phone.
  { ...mainStates[9], name: "graph-neighborhood-legend", state: "legend", prepare: async page => {
    await page.getByRole("button", { name: "Legend", exact: true }).click();
    if (page.viewportSize().width < 768) await page.getByRole("dialog", { name: "Legend" }).waitFor();
  } },
  // A chip in focus marks the link its document supports.
  { name: "graph-neighborhood-evidence", route: "/graph?focus=nb-desk", state: "evidence-marked",
    ready: page => page.getByText("No links between these neighbors.", { exact: true }), prepare: async page => {
      await graphReady(page);
      await page.getByRole("region", { name: /^Evidence/ }).getByRole("link").first().focus();
    } },
  // The mixed link selected from the inspector: "Stated in 1, inferred in 1" and its documents.
  { name: "graph-neighborhood-link", route: "/graph?focus=nb-renewal", state: "selected-link",
    ready: page => page.getByText("Renewal notice 2026: 1 links in 1 families.", { exact: false }), prepare: async page => {
      await graphReady(page);
      // On a phone the inspector opens with a selection: select the focus first.
      if (page.viewportSize().width < 768) {
        const canvas = page.locator("canvas").first();
        const box = await canvas.boundingBox();
        const zoom = await canvas.evaluate(element => element.__zoom);
        await page.mouse.click(box.x + zoom.x, box.y + zoom.y);
      }
      await page.getByRole("complementary", { name: "Evidence inspector" }).getByRole("button", { name: /^2014 Honda Fit/ }).click();
      await page.getByRole("region", { name: "Inspector: link" }).getByText("Stated in 1, inferred in 1", { exact: true }).waitFor();
      await page.getByRole("region", { name: "Inspector: link" }).locator("blockquote").first().waitFor();
    } },
  // A link between two neighbors opens the same evidence.
  { name: "graph-neighborhood-cross-link", route: "/graph?focus=nb-rowan&sel=nb-coverage-0~ISSUED_BY~nb-money-0", state: "selected-cross-link",
    ready: page => page.getByRole("region", { name: "Inspector: link" }).locator("blockquote").first() },
  { name: "graph-table-evidence", route: "/graph?focus=nb-rowan&view=table&sel=nb-rowan~EMPLOYED_BY~nb-parties-0", state: "selected-link",
    ready: page => page.getByRole("region", { name: "Inspector: link" }).locator("blockquote").first() },
  { name: "graph-table-empty", route: "/graph?focus=nb-leaf&view=table", state: "empty",
    ready: page => page.getByText("No direct links extracted. Open Focus to see this entity's documents.", { exact: true }) },
  { name: "graph-view-sheet", route: "/graph?focus=nb-rowan", state: "view-sheet", phoneOnly: true,
    ready: page => page.getByRole("button", { name: "View: Focus" }), prepare: async page => {
      await page.getByRole("button", { name: "View: Focus" }).click();
      await page.getByRole("dialog", { name: "View", exact: true }).waitFor();
    } },
  { name: "graph-path-owner", route: `${pathRoute}&owner=show`, state: "owner-included",
    ready: page => page.getByRole("list", { name: "Steps of the path" }).locator("blockquote").nth(1) },
  { name: "graph-leaf", route: "/graph?focus=nb-leaf", state: "leaf",
    ready: page => page.getByText("No direct links extracted. This entity appears in 2 documents.", { exact: true }) },
];

test.beforeEach(async ({ page, baseURL, request }, info) => {
  assert.equal(info.config.updateSnapshots, mode === "update" ? "all" : "none", "Use the manual CI job to update baselines");
  assert.ok(["127.0.0.1", "localhost"].includes(new URL(baseURL).hostname));
  assert.equal((await (await request.get("/api/_fixture")).json()).fixture, "paperless-accuracy-ui-v1");
  assert.ok((await request.post("/api/_fixture/reset")).ok());
  await page.clock.install({ time: fixedTime });
  await page.addInitScript(() => {
    const fillText = CanvasRenderingContext2D.prototype.fillText;
    CanvasRenderingContext2D.prototype.fillText = function (...args) {
      window.__visualGraphPainted = true;
      return fillText.apply(this, args);
    };
    // The physics renderer also uses randomness; give every capture the same seed.
    let seed = 112;
    Math.random = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296);
    localStorage.setItem("theme", "system");
  });
});

for (const state of [gallery, ...mainStates, ...extraStates]) test(`${state.name} ${state.state || "main"}`, async ({ page }, info) => {
  test.skip(state.phoneOnly && !info.project.name.startsWith("phone"), "The conversation sheet is phone-only");
  await state.seed?.(page);
  await page.goto(state.route);
  await state.ready(page).waitFor();
  await page.addStyleTag({ content: "*, *::before, *::after { animation: none !important; transition: none !important; backdrop-filter: none !important; scroll-behavior: auto !important; caret-color: transparent !important; }" });
  await state.prepare?.(page);
  if (state.state !== "tooltip") await page.mouse.move(0, 0);
  const graph = state.name.startsWith("graph-neighborhood") || state.name.startsWith("graph-filters");
  if (graph) await page.locator("canvas").first().waitFor({ state: "visible" });
  await page.evaluate(() => document.fonts.ready);
  // Finish count-up and graph cooldown on the fixed clock, then freeze all timers.
  if (graph) await page.waitForFunction(() => window.__visualGraphPainted === true);
  await page.clock.runFor(2500);
  await page.clock.pauseAt(new Date(fixedTime.getTime() + 120000));
  if (state.name === "gallery") await page.evaluate(() => {
    let element = document.querySelector("[data-gallery-content]");
    while (element) {
      element.style.height = "auto";
      element.style.overflow = "visible";
      element = element.parentElement;
    }
  });
  if (!state.state) {
    await page.locator("main").evaluate(main => {
      for (const element of [main, ...main.querySelectorAll("*")]) {
        if (element.scrollTop) element.scrollTop = 0;
      }
    });
    const heading = await page.getByRole("heading", { level: 1 }).boundingBox();
    expect(heading?.y, "Main captures start at the page heading").toBeGreaterThanOrEqual(0);
  }
  let capture;
  await expect(async () => {
    const previous = capture;
    capture = await page.screenshot({ fullPage: state.name === "gallery", animations: "disabled", caret: "hide" });
    expect(previous && capture.equals(previous), "Consecutive captures must match").toBe(true);
  }).toPass({ timeout: 5000 });
  await mkdir(info.outputDir, { recursive: true });
  const capturePath = info.outputPath("capture.png");
  await writeFile(capturePath, capture);
  await info.attach("capture", { path: capturePath, contentType: "image/png" });
  if (!state.state && mode !== "capture") await expect.soft(capture).toMatchSnapshot(`${state.name}.png`, { maxDiffPixels: 0, threshold: 0 });

  await page.clock.resume();
  await page.addScriptTag({ path: require.resolve("axe-core/axe.min.js") });
  const accessibility = await page.evaluate(async () => window.axe.run(document, {
    runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"] },
  }));
  const smallText = await page.locator("body *").evaluateAll(elements => elements.filter(element => {
    const style = getComputedStyle(element);
    return parseFloat(style.fontSize) < 12 && style.visibility !== "hidden" && element.getClientRects().length
      && [...element.childNodes].some(node => node.nodeType === Node.TEXT_NODE && node.textContent.trim());
  }).map(element => ({ target: [element.tagName.toLowerCase() + (element.className ? "." + String(element.className).trim().split(/\s+/).join(".") : "")], html: element.outerHTML })));
  const violations = [...accessibility.violations, ...(smallText.length ? [{ id: "font-size", nodes: smallText }] : [])];
  const report = JSON.stringify({ route: state.route, state: state.state || "main", violations, incomplete: accessibility.incomplete }, null, 2);
  const reportPath = info.outputPath("accessibility.json");
  await writeFile(reportPath, report);
  await info.attach("accessibility", { path: reportPath, contentType: "application/json" });
  expect.soft(unexpectedViolations(violations, exceptions, state.route, state.state || "main"), "New accessibility violations").toEqual([]);
});
