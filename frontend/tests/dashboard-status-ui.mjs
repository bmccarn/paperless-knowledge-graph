import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";

const base = process.env.UI_TEST_BASE_URL || "http://127.0.0.1:3006";
const artifacts = process.env.UI_TEST_ARTIFACTS;
assert.ok(["127.0.0.1", "localhost"].includes(new URL(base).hostname));
let browser;
before(async () => {
  browser = await chromium.launch({ headless: true });
  if (artifacts) await mkdir(artifacts, { recursive: true });
});
after(async () => { await browser?.close(); });

async function fixturePage(t, viewport = { width: 1440, height: 1000 }) {
  const context = await browser.newContext({ viewport });
  t.after(() => context.close());
  const identity = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await identity.json()).fixture, "paperless-accuracy-ui-v1");
  const healthy = await (await context.request.get(`${base}/api/status`)).json();
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.clock.install();
  return { page, healthy };
}

const summaryText = "The archive holds 303 documents and 122 entities.";
const freshness = {
  stale: true, paperless_documents: 310, indexed_documents: 303, missing_documents: 7,
  missing_embedding_documents: 2, modified_after_last_sync_documents: 1, last_sync: "2026-09-04T12:00:00+00:00",
  latest_paperless_title: "Synthetic late arrival", latest_paperless_modified: "2026-09-04T11:30:00+00:00",
  latest_paperless_id: 9400, last_failed_extraction: null,
  drift: {
    sample_limit: 20, extra_in_graph: [], extra_embeddings: [], missing_hashes: [], extra_hashes: [],
    missing_from_graph: [{ id: 9400, title: "Synthetic late arrival" }],
    missing_embeddings: [{ id: 9401, title: "Synthetic unembedded" }],
    modified_after_last_sync: [{ id: 9402, title: "Synthetic edit" }],
  },
};

/** The page scrolls inside its own container, not the window. */
const pageScroll = page => page.getByRole("heading", { level: 1 }).evaluate(element => {
  while (element && getComputedStyle(element).overflowY !== "auto") element = element.parentElement;
  return element ? { top: element.scrollTop, room: element.scrollHeight - element.clientHeight } : null;
});

for (const error of ["paperless down", ""]) test(`degraded first load shows ${error || "fallback"} and retry recovers`, async t => {
  const { page, healthy } = await fixturePage(t);
  let response = { status: "degraded", error };
  await page.route("**/api/status", route => route.fulfill({ json: response }));
  await page.goto(base);
  await page.getByText(error || "Backend degraded", { exact: true }).waitFor();
  assert.equal(await page.getByRole("heading", { name: "Status", exact: true }).count(), 1);
  response = healthy;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await page.getByRole("heading", { name: "Status", exact: true }).waitFor();
  await page.getByText("The status could not be loaded.", { exact: true }).waitFor({ state: "hidden" });
  assert.equal(await page.getByText(error || "Backend degraded", { exact: true }).count(), 0);
});

for (const failure of ["degraded", "http", "network"]) test(`${failure} poll warns about retained counts and the next healthy poll clears it`, async t => {
  const { page, healthy } = await fixturePage(t);
  let fail = false;
  await page.route("**/api/status", route => {
    if (!fail) return route.fulfill({ json: healthy });
    if (failure === "network") return route.abort("failed");
    if (failure === "http") return route.fulfill({ status: 503, json: { detail: "status unavailable" } });
    return route.fulfill({ json: { status: "degraded", error: "paperless down" } });
  });
  await page.goto(base);
  const summary = page.getByText(summaryText, { exact: false });
  await summary.waitFor();
  fail = true;
  await page.clock.fastForward(10000);
  const warning = page.getByRole("alert").filter({ hasText: "Showing the last successful status" });
  await warning.waitFor();
  assert.match(await warning.innerText(), /Showing the last successful status/);
  if (failure === "degraded") assert.match(await warning.innerText(), /paperless down/);
  if (failure === "http") assert.match(await warning.innerText(), /503 status unavailable/);
  assert.equal(await summary.isVisible(), true);
  if (artifacts) await page.screenshot({ path: `${artifacts}/${failure}-poll.png`, fullPage: true });
  fail = false;
  await page.clock.fastForward(10000);
  await warning.waitFor({ state: "hidden" });
  assert.equal(await summary.isVisible(), true);
});

test("a failed cancellation does not mark a healthy status as unavailable", async t => {
  const { page, healthy } = await fixturePage(t);
  healthy.active_tasks = { "running-sync": { status: "running", type: "sync" } };
  await page.route("**/api/status", route => route.fulfill({ json: healthy }));
  await page.route("**/api/task/running-sync", route => route.fulfill({ json: {
    status: "running", total_docs: 10, processed: 1, skipped: 0, errors: 0,
  } }));
  await page.route("**/api/task/running-sync/cancel", route => route.fulfill({
    status: 404, json: { detail: "Task no longer active" },
  }));
  await page.goto(base);
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await page.getByText("API error: 404 Task no longer active", { exact: true }).waitFor();
  assert.equal(await page.getByText("Showing the last successful status", { exact: false }).count(), 0);
});

test("on a phone, Sync new documents is inside the first screen even with the stale alert (#88)", async t => {
  const { page, healthy } = await fixturePage(t, { width: 390, height: 844 });
  await page.route("**/api/status", route => route.fulfill({ json: { ...healthy, freshness } }));
  await page.goto(base);
  await page.getByRole("alert").getByRole("button", { name: "Sync now", exact: true }).waitFor();
  const sync = await page.getByRole("button", { name: "Sync new documents", exact: true }).boundingBox();
  // The tab bar covers the bottom of the 844 px screen, so the button must end above it.
  const tabBar = await page.getByRole("navigation", { name: "Phone" }).boundingBox();
  assert.ok(sync.y >= 0 && sync.y + sync.height <= Math.min(844, tabBar.y), `Sync ends at ${sync.y + sync.height}`);
  if (artifacts) await page.screenshot({ path: `${artifacts}/phone-stale.png` });
});

test("a streaming task never scrolls the page, and its log follows only from the bottom", async t => {
  const { page, healthy } = await fixturePage(t, { width: 390, height: 844 });
  let polls = 0;
  await page.route("**/api/status", route => route.fulfill({ json: {
    ...healthy, active_tasks: { "streaming-sync": { status: "running", type: "sync" } },
  } }));
  await page.route("**/api/task/streaming-sync", route => {
    const done = ++polls * 6;
    return route.fulfill({ json: {
      status: "running", started: "2026-09-04T12:00:00+00:00", total_docs: 300, processed: done, skipped: 0, errors: 0,
      current_doc: `Synthetic archive ${done}`, elapsed_seconds: polls * 2, docs_per_minute: 180, estimated_remaining_seconds: 90,
      recent_results: Array.from({ length: done }, (_, index) => ({
        doc_id: 9000 + index, title: `Synthetic archive ${String(index).padStart(3, "0")}`, status: "processed", entities: 2,
      })),
    } });
  });
  await page.goto(base);
  await page.getByRole("progressbar").waitFor();
  const before = await pageScroll(page);
  assert.ok(before.room > 120, "The page must be taller than the screen for this check");
  await page.getByRole("heading", { level: 1 }).evaluate(element => {
    while (getComputedStyle(element).overflowY !== "auto") element = element.parentElement;
    element.scrollTop = 120;
  });
  for (let poll = 0; poll < 3; poll++) {
    const seen = polls;
    await page.clock.runFor(2000);
    await page.waitForFunction(count => document.querySelectorAll("[role=log] li").length >= count, (seen + 1) * 6);
  }
  await page.waitForTimeout(600);
  assert.equal((await pageScroll(page)).top, 120, "The page scrolled by itself");

  const log = page.getByRole("log", { name: "Sync log" });
  const position = () => log.evaluate(element => ({ top: element.scrollTop, bottom: element.scrollHeight - element.clientHeight }));
  const following = await position();
  assert.ok(following.bottom > 0 && Math.abs(following.top - following.bottom) <= 1, "The log follows new lines at the bottom");
  await log.evaluate(element => { element.scrollTop = 0; element.dispatchEvent(new Event("scroll")); });
  const seen = polls;
  await page.clock.runFor(2000);
  await page.waitForFunction(count => document.querySelectorAll("[role=log] li").length >= count, (seen + 1) * 6);
  assert.equal((await position()).top, 0, "The log moved while the reader was scrolled up");
  assert.equal((await pageScroll(page)).top, 120);
});

test("Full reindex opens a confirmation and starts nothing until confirmed", async t => {
  const { page } = await fixturePage(t);
  const posts = [];
  await page.route("**/api/reindex", route => { posts.push(route.request().url()); return route.fulfill({ json: { task_id: "", status: "noop" } }); });
  await page.goto(base);
  const reindex = page.getByRole("button", { name: "Full reindex", exact: true });
  await reindex.click();
  const dialog = page.getByRole("alertdialog", { name: "Reindex every document?" });
  await dialog.waitFor();
  assert.deepEqual(posts, []);
  await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  assert.deepEqual(posts, []);
  assert.equal(await reindex.evaluate(element => element === document.activeElement), true);
});

test("the summary shows the final counts at once, with no count-up", async t => {
  const { page } = await fixturePage(t);
  await page.addInitScript(() => {
    window.__counts = new Set();
    new MutationObserver(() => {
      for (const match of document.documentElement.innerText.matchAll(/holds ([\d,]+) documents and ([\d,]+) entities/g)) window.__counts.add(match[0]);
    }).observe(document, { subtree: true, childList: true, characterData: true });
  });
  await page.goto(base);
  await page.getByText(summaryText, { exact: false }).waitFor();
  await page.clock.runFor(2500);
  assert.deepEqual(await page.evaluate(() => [...window.__counts]), ["holds 303 documents and 122 entities"]);
});

for (const [term, path] of [["Alex Example", "/graph"], ["synthetic unmatched question", "/query"]]) test(`the search box opens ${path} for "${term}"`, async t => {
  const { page } = await fixturePage(t);
  await page.goto(base);
  const asked = path === "/query" ? page.waitForRequest(request => request.url().endsWith("/api/query/stream")) : null;
  await page.getByRole("textbox", { name: "Search the knowledge graph" }).fill(term);
  await page.getByRole("textbox", { name: "Search the knowledge graph" }).press("Enter");
  await page.waitForURL(url => url.pathname === path);
  if (asked) assert.equal((await asked).postDataJSON().question, term);
});

test("the three columns show each saved answer's state and question, the duplicate count and the archive by type", async t => {
  const { page } = await fixturePage(t);
  assert.ok((await page.request.post(`${base}/api/_fixture/reset`)).ok());
  assert.ok((await page.request.post(`${base}/api/_fixture/saved-answers`)).ok());
  // Later suites expect no saved conversations.
  t.after(() => fetch(`${base}/api/_fixture/reset`, { method: "POST" }));
  await page.goto(base);
  const answers = page.getByRole("region", { name: "Recent answers" }).getByRole("listitem");
  await answers.first().waitFor();
  assert.deepEqual(await answers.allInnerTexts(), [
    "Not checked\nWhat premium does the January statement list?",
    "Not checked\nWhat premium does the January statement list?",
    "Verified\nWhat premium does the January statement list?",
  ]);
  assert.match(await page.getByRole("region", { name: "Possible duplicates" }).innerText(), /3 pairs to review/);
  const types = await page.getByRole("region", { name: "Archive by type" }).getByRole("listitem").allInnerTexts();
  assert.deepEqual(types.map(row => row.replace(/\s+/g, " ")), ["Invoice 151", "Medical 150", "Financial statement 1", "Insurance 1"]);
});
