import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { chromium } from "playwright";
import { mkdir, writeFile } from "node:fs/promises";

const base = process.env.UI_TEST_BASE_URL || "http://127.0.0.1:3006";
const artifacts = process.env.UI_TEST_ARTIFACTS;
assert.ok(["127.0.0.1", "localhost"].includes(new URL(base).hostname));
let browser;
const results = [];
before(async () => {
  browser = await chromium.launch({ headless: true });
  if (artifacts) await mkdir(artifacts, { recursive: true });
});
after(async () => {
  await browser?.close();
  if (artifacts) await writeFile(`${artifacts}/results.json`, JSON.stringify(results, null, 2));
});
function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
}
async function fixturePage(t) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  t.after(() => context.close());
  const identity = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await identity.json()).fixture, "paperless-accuracy-ui-v1");
  await context.request.post(`${base}/api/_fixture/reset`);
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  return { context, page };
}
async function rendered(page) {
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
}
async function record(page, name) {
  results.push({ name, status: "passed" });
  if (artifacts) await page.screenshot({ path: `${artifacts}/${name}.png`, fullPage: true });
}

test("metadata-only freshness drift is explained and offers targeted repair", async t => {
  const { context, page } = await fixturePage(t);
  const status = await (await context.request.get(`${base}/api/status`)).json();
  status.freshness = { stale: true, paperless_documents: 303, indexed_documents: 303, missing_documents: 0,
    changed_since_index_documents: 1, modified_after_last_sync_documents: 0,
    drift: { changed_since_index: [{ id: 101, title: "Corrected source title" }] } };
  await page.route("**/api/status", route => route.fulfill({ json: status }));
  await page.goto(base);
  await page.getByText("Knowledge graph may be stale", { exact: true }).waitFor();
  assert.equal(await page.getByText("Changed since indexing:", { exact: true }).count(), 1);
  await page.getByText(/Corrected source title/).waitFor();
  await page.getByRole("button", { name: "Repair drift", exact: true }).click();
  await page.getByRole("dialog").waitFor();
  await record(page, "metadata-freshness-repair");
});

test("failed recovery from an old conversation cannot overwrite a new pending question", async t => {
  const { page } = await fixturePage(t);
  const recovery = deferred(), recoveryStarted = deferred(), next = deferred(), nextStarted = deferred();
  let oldId, newId, count = 0;
  await page.route("**/api/query/stream", async route => {
    const request = route.request().postDataJSON();
    if (++count === 1) {
      oldId = request.conversation_id;
      await route.fulfill({ contentType: "text/event-stream", body: 'data: {"type":"error","message":"Synthetic failure"}\n\n' });
    } else {
      newId = request.conversation_id; nextStarted.resolve(); await next.promise;
      await route.fulfill({ contentType: "text/event-stream", body: 'data: {"type":"complete","answer":"NEW_ANSWER","sources":[]}\n\n' });
    }
  });
  await page.route("**/api/conversations/*", async route => {
    if (route.request().method() === "GET" && route.request().url().endsWith(`/${oldId}`)) {
      recoveryStarted.resolve(); await recovery.promise;
      await route.fulfill({ status: 503, json: { detail: "Synthetic recovery failure" } });
    } else await route.continue();
  });
  t.after(() => { recovery.resolve(); next.resolve(); });
  await page.goto(`${base}/query`);
  const input = page.getByPlaceholder("Ask a question...");
  await input.fill("OLD_QUESTION"); await input.press("Enter");
  await recoveryStarted.promise;
  await page.getByRole("button", { name: "New Conversation", exact: true }).click();
  await input.fill("NEW_QUESTION"); await input.press("Enter");
  await nextStarted.promise;
  const response = page.waitForResponse(`**/api/conversations/${oldId}`);
  recovery.resolve(); await (await response).finished(); await rendered(page);
  assert.equal(await page.getByText("OLD_QUESTION", { exact: true }).count(), 0);
  assert.equal(await page.getByText("NEW_QUESTION", { exact: true }).count(), 1);
  assert.equal(await input.isDisabled(), true);
  assert.notEqual(oldId, newId);
  next.resolve(); await page.getByText("NEW_ANSWER", { exact: true }).waitFor();
  await record(page, "conversation-recovery-isolation");
});

test("conversation switching isolates delayed completion and subsequent questions", async t => {
  const { context, page } = await fixturePage(t);
  const b = await (await context.request.post(`${base}/api/conversations`, { data: { title: "Conversation B" } })).json();
  const held = deferred(), started = deferred();
  let firstId, nextId, count = 0;
  await page.route("**/api/query/stream", async route => {
    const request = route.request().postDataJSON();
    const first = ++count === 1;
    if (first) { firstId = request.conversation_id; started.resolve(); await held.promise; }
    else nextId = request.conversation_id;
    await route.fulfill({ status: 200, contentType: "text/event-stream", body: "data: " + JSON.stringify({
      type: "complete", answer: first ? "ANSWER_FROM_A" : "ANSWER_FROM_B", sources: [], confidence: 0,
      finalization: { disposition: "unsupported", complete: false },
    }) + "\n\n" });
  });
  t.after(() => held.resolve());
  await page.goto(`${base}/query`);
  await page.getByRole("button", { name: "Conversation B", exact: true }).waitFor();
  const input = page.getByPlaceholder("Ask a question...");
  await input.fill("QUESTION_FOR_A");
  await input.press("Enter");
  await started.promise;
  assert.notEqual(firstId, b.id);
  await page.getByRole("button", { name: "Conversation B", exact: true }).click();
  await page.getByText("QUESTION_FOR_A", { exact: true }).waitFor({ state: "hidden" });
  const response = page.waitForResponse("**/api/query/stream");
  held.resolve();
  await (await response).finished();
  await rendered(page);
  assert.equal(await page.getByText("ANSWER_FROM_A", { exact: true }).count(), 0);
  await input.fill("QUESTION_FOR_B");
  await input.press("Enter");
  await page.getByText("ANSWER_FROM_B", { exact: true }).waitFor();
  assert.equal(nextId, b.id);
  assert.equal(await page.getByText("ANSWER_FROM_A", { exact: true }).count(), 0);
  await record(page, "conversation-switch");
});

for (const batch of [false, true]) test(`${batch ? "batch" : "single"} reindex refreshes the current document view`, async t => {
  const { page } = await fixturePage(t);
  const held = deferred(), started = deferred();
  await page.route("**/api/reindex/101", route => route.fulfill({ json: { task_id: "held-reindex", status: "started" } }));
  await page.route("**/api/task/held-reindex", async route => {
    started.resolve(); await held.promise;
    await route.fulfill({ json: { status: "completed", result: { processed: 1, errors: 0 } } });
  });
  t.after(() => held.resolve());
  await page.goto(`${base}/documents`);
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  const row = page.getByRole("row").filter({ hasText: "January premium statement" });
  if (batch) {
    await row.getByRole("checkbox").click();
    await page.getByRole("button", { name: "Reindex 1", exact: true }).click();
  } else await row.getByRole("button").click();
  await started.promise;
  const input = page.getByRole("textbox", { name: "Search indexed documents" });
  await input.fill("Synthetic archive");
  await page.getByRole("button", { name: "Search documents", exact: true }).click();
  await page.getByText("301 matching indexed documents", { exact: true }).waitFor();
  await page.getByRole("button", { name: /invoice/ }).click();
  await page.getByText("151 matching indexed documents", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Next page", exact: true }).first().click();
  await page.getByText("2 / 7", { exact: true }).waitFor();
  const before = await page.getByRole("row").allTextContents();
  const refreshed = page.waitForResponse(response => new URL(response.url()).pathname === "/api/documents");
  held.resolve();
  const response = await refreshed;
  await response.finished(); await rendered(page);
  const url = new URL(response.url());
  assert.equal(url.searchParams.get("q"), "Synthetic archive");
  assert.equal(url.searchParams.get("doc_type"), "invoice");
  assert.equal(url.searchParams.get("offset"), "25");
  assert.deepEqual(await page.getByRole("row").allTextContents(), before);
  await record(page, `document-${batch ? "batch" : "single"}-refresh`);
});

for (const trigger of ["initial", "reset", "seed"]) test(`${trigger} graph arrival preserves a selected search node and expansion`, async t => {
  const { context, page } = await fixturePage(t);
  const initial = await (await context.request.get(`${base}/api/graph/initial?limit=100`)).json();
  if (trigger !== "initial") {
    await page.goto(`${base}/graph`);
    await page.getByText("4 nodes", { exact: false }).waitFor();
  }
  const held = deferred();
  await page.route("**/api/graph/initial?**", async route => {
    await held.promise; await route.fulfill({ json: initial });
  });
  t.after(() => held.resolve());
  if (trigger === "initial") await page.goto(`${base}/graph?q=Search%20Fixture`);
  else {
    if (trigger === "reset") await page.getByRole("button", { name: "Reset sample", exact: true }).click();
    else await page.getByLabel("Sample seed size", { exact: true }).selectOption("200");
    await page.getByLabel("Find an entity or document", { exact: true }).fill("Search Fixture");
    await page.getByRole("button", { name: "Search graph", exact: true }).click();
  }
  await page.getByRole("button", { name: /Search Fixture Entity 000/ }).click();
  const inspector = page.getByLabel("Evidence inspector", { exact: true });
  await inspector.waitFor();
  await inspector.getByRole("button", { name: "Load neighbors into graph", exact: true }).click();
  await page.getByText("5 nodes", { exact: false }).waitFor();
  const response = page.waitForResponse("**/api/graph/initial?**");
  held.resolve(); await (await response).finished(); await rendered(page);
  assert.equal(await inspector.count(), 1);
  await page.getByText("Synthetic pageable graph search fixture.", { exact: true }).waitFor();
  await page.getByText("5 nodes", { exact: false }).waitFor();
  await inspector.getByRole("button", { name: "Load neighbors into graph", exact: true }).click();
  await page.getByText("5 nodes", { exact: false }).waitFor();
  await record(page, `graph-${trigger}-selection`);
});

test("snapshot-rejected SSE answers do not display obsolete supported claims", async t => {
  const { page } = await fixturePage(t);
  const warning = "The document index changed. Retry after indexing finishes.";
  await page.route("**/api/query/stream", route => route.fulfill({
    status: 200,
    contentType: "text/event-stream",
    body: "data: " + JSON.stringify({
      type: "complete", answer: warning, confidence: 0, sources: [],
      verification: { status: "corpus_changed", supported_claims: [], missing_evidence: ["Stable source index required."] },
      finalization: { disposition: "corpus_changed", complete: false, cited_document_ids: [] },
      // Older persisted payloads can still contain a superseded diagnostic ledger.
      claim_ledger: { claims: [{ claim: "Obsolete premium is $321.", status: "supported" }] },
      source_summary: { verification_status: "corpus_changed", claim_summary: { supported: 1 } },
    }) + "\n\n",
  }));
  await page.goto(`${base}/query`);
  const input = page.getByPlaceholder("Ask a question...");
  await input.fill("Recorded premium during reindex?");
  await input.press("Enter");
  await page.getByText(warning, { exact: true }).waitFor();
  assert.equal(await page.getByText("Obsolete premium is $321.", { exact: true }).count(), 0);
  assert.equal(await page.getByText("supported", { exact: true }).count(), 0);
  assert.equal(await page.getByText("supported: 1", { exact: true }).count(), 0);
  await record(page, "snapshot-invalidated-claims");
});


test("failed catalog searches do not claim an empty result or reuse old totals", async t => {
  const { page } = await fixturePage(t);
  await page.goto(`${base}/documents`);
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  await page.getByRole("textbox", { name: "Search indexed documents" }).fill("fixture-error");
  await page.getByRole("button", { name: "Search documents", exact: true }).click();
  await page.getByRole("alert").filter({ hasText: "Synthetic document search failure" }).waitFor();
  assert.equal(await page.getByText(/303 matching indexed documents/).count(), 0);
  assert.equal(await page.getByRole("button", { name: "invoice 151", exact: true }).count(), 0);
  assert.equal(await page.getByText("No documents found", { exact: true }).count(), 0);
  assert.equal(await page.getByRole("button", { name: "Next page", exact: true }).count(), 0);
  await page.getByRole("textbox", { name: "Search indexed documents" }).fill("January");
  await page.getByRole("button", { name: "Search documents", exact: true }).click();
  await page.getByText("1 matching indexed documents", { exact: true }).waitFor();
  await page.getByRole("link", { name: "January premium statement", exact: true }).waitFor();
  await record(page, "catalog-search-error-recovery");
});


test("all navigation and theme controls remain visible on narrow phones", async t => {
  const { page } = await fixturePage(t);
  await page.setViewportSize({ width: 320, height: 740 });
  await page.goto(`${base}/query`);
  for (const name of ["Dashboard", "Query", "Graph", "Docs", "Review", "Hubs", "Debug", "Toggle theme"]) {
    const control = page.getByRole(name === "Toggle theme" ? "button" : "link", { name, exact: true });
    const box = await control.boundingBox();
    assert.ok(box && box.x >= 0 && box.x + box.width <= 320, `${name} must fit inside the viewport`);
  }
  const before = await page.locator("html").getAttribute("class");
  await page.getByRole("button", { name: "Toggle theme", exact: true }).click();
  await rendered(page);
  assert.notEqual(await page.locator("html").getAttribute("class"), before);
  await record(page, "narrow-phone-navigation");
});
