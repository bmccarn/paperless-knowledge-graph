import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { chromium } from "playwright";

const base = process.env.UI_TEST_BASE_URL;
assert.ok(["localhost", "127.0.0.1"].includes(new URL(base).hostname));
let browser;
before(async () => { browser = await chromium.launch(); });
after(async () => { await browser.close(); });

for (const width of [1440, 390]) test(`palette keyboard and navigation at ${width}px`, async t => {
  const context = await browser.newContext({ viewport: { width, height: 900 } });
  t.after(() => context.close());
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  await page.request.post(`${base}/api/_fixture/reset`);
  await page.request.post(`${base}/api/_fixture/saved-answers`);
  const writes = [];
  page.on("request", request => { if (request.method() !== "GET") writes.push(request.url()); });
  await page.goto(`${base}/query`);
  const opener = page.getByRole("button", { name: "Search Ctrl K, pages and archive", exact: true });
  await opener.click();
  const input = page.getByRole("combobox", { name: "Search pages and archive", exact: true });
  await input.fill("do");
  await page.getByRole("option", { name: "Documents", exact: true }).waitFor();
  await input.press("ArrowDown");
  assert.equal(await page.locator(`[id="${await input.getAttribute("aria-activedescendant")}"]`).getAttribute("aria-selected"), "true");
  await input.press("Escape");
  assert.equal(await opener.evaluate(el => el === document.activeElement), true);
  for (const shortcut of ["Control+k", "Meta+k"]) {
    await page.keyboard.press(shortcut);
    await input.waitFor();
    await input.press("Escape");
  }
  await opener.click();
  await input.fill("January");
  await page.getByRole("option", { name: "January premium statement", exact: true }).waitFor();
  await input.fill("January ");
  await page.getByRole("option", { name: "January premium statement", exact: true }).waitFor();
  await input.press("Enter");
  await page.waitForURL("**/documents/101");
  await page.keyboard.press("Control+k");
  await input.fill("Example");
  await page.getByRole("option", { name: "Example Utility", exact: true }).waitFor();
  assert.equal(await page.getByRole("group", { name: "Entities", exact: true }).getByRole("option").count(), 2);
  // Enter on an entity row opens that entity's neighborhood.
  for (let step = 0; step < 10 && await page.locator(`[id="${await input.getAttribute("aria-activedescendant")}"]`).innerText() !== "Example Utility"; step++)
    await input.press("ArrowDown");
  await input.press("Enter");
  await page.waitForURL("**/graph?focus=org-1");
  await page.keyboard.press("Control+k");
  await input.fill("Older verified");
  await page.getByRole("option", { name: "Older verified answer", exact: true }).waitFor();
  await input.press("Enter");
  await page.getByRole("article", { name: "Answer", exact: true }).waitFor();
  assert.equal(writes.length, 0, "Palette navigation never writes or submits questions");
  await page.goto(`${base}/`);
  await page.getByRole("search").getByRole("textbox").fill("Unmatched question from status");
  const questionSent = page.waitForResponse(response => response.url().includes("/query/stream"));
  await page.getByRole("search").getByRole("button", { name: "Search", exact: true }).click();
  await questionSent;
  assert.ok(writes.some(url => url.endsWith("/query/stream")), "Status keeps its question fall-through");
});

test("debounce, stale results, paging and retry", async t => {
  const page = await browser.newPage();
  t.after(() => page.close());
  page.setDefaultTimeout(5000);
  let searches = 0;
  let fail = false;
  await page.route("**/api/graph/search?**", async route => {
    searches++;
    const url = new URL(route.request().url());
    const q = url.searchParams.get("q");
    if (q === "slow" || q === "fast") await new Promise(resolve => setTimeout(resolve, 400));
    if (fail) return route.fulfill({ status: 503, json: { detail: "Synthetic failure" } });
    const offset = Number(url.searchParams.get("offset"));
    return route.fulfill({ json: { results: offset === 0
      ? [{ labels: ["Document"], properties: { title: "Exclude document" } }]
      : Array.from({ length: 6 }, (_, i) => ({ labels: ["Person"], properties: { name: `${q} person ${i < 2 ? 0 : i}`, uuid: `p-${i}` } })), has_more: offset === 0 } });
  });
  await page.route("**/api/conversations?**", route => {
    const offset = Number(new URL(route.request().url()).searchParams.get("offset"));
    return route.fulfill({ json: offset === 0 ? Array.from({ length: 50 }, (_, i) => ({ id: `old-${i}`, title: "Unrelated" })) : [{ id: "last", title: "fast saved conversation" }] });
  });
  await page.goto(`${base}/documents`);
  await page.clock.install();
  await page.clock.pauseAt(new Date(Date.now() + 10000));
  await page.getByRole("button", { name: "Search Ctrl K, pages and archive", exact: true }).click();
  const input = page.getByRole("combobox", { name: "Search pages and archive", exact: true });
  await input.fill("f");
  await page.clock.runFor(200);
  assert.equal(searches, 0);
  await input.fill("slow");
  await page.clock.runFor(149);
  assert.equal(searches, 0);
  const slowRequest = page.waitForRequest("**/api/graph/search?**");
  await page.clock.runFor(1);
  await slowRequest;
  await input.fill("fast");
  await page.clock.runFor(150);
  await page.getByRole("option", { name: "fast saved conversation", exact: true }).waitFor();
  await input.press("ArrowUp");
  await page.getByRole("option", { name: "fast person 0", exact: true }).first().waitFor();
  assert.equal(await page.getByRole("option", { name: "fast saved conversation", exact: true }).getAttribute("aria-selected"), "true");
  assert.equal(await page.getByRole("group", { name: "Entities", exact: true }).getByRole("option").count(), 5);
  await new Promise(resolve => setTimeout(resolve, 600));
  assert.equal(await page.getByRole("option", { name: /slow person/ }).count(), 0);
  const last = page.getByRole("option", { name: "fast saved conversation", exact: true });
  assert.equal(await last.getAttribute("aria-selected"), "true");
  await input.press("ArrowDown");
  const beforeDuplicate = await input.getAttribute("aria-activedescendant");
  await input.press("ArrowDown");
  assert.notEqual(await input.getAttribute("aria-activedescendant"), beforeDuplicate, "Duplicate names do not trap arrow selection");
  fail = true;
  await input.fill("failure");
  await page.clock.runFor(150);
  await page.getByRole("alert").waitFor();
  fail = false;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await page.clock.runFor(150);
  await page.getByRole("option", { name: "failure person 0", exact: true }).first().waitFor();
});

test("a search with no results shows one empty state, and an unnamed entity shows its type, not its UUID", async t => {
  const page = await browser.newPage();
  t.after(() => page.close());
  page.setDefaultTimeout(5000);
  await page.request.post(`${base}/api/_fixture/reset`);
  await page.goto(`${base}/documents`);
  await page.getByRole("button", { name: "Search Ctrl K, pages and archive", exact: true }).click();
  const input = page.getByRole("combobox", { name: "Search pages and archive", exact: true });
  await input.fill("qqzx no such thing");
  await page.getByRole("status").filter({ hasText: "0 results" }).waitFor();
  assert.equal(await page.getByText("No matches.", { exact: true }).count(), 1);
  assert.equal(await page.locator("#palette-results").getByText(/^(Pages|Documents|Entities|Conversations)$/).count(), 0,
    "empty groups print no headings");
  await page.route("**/api/graph/search?**", route => route.fulfill({ json: { results: [
    { labels: ["Person"], properties: { uuid: "9b1c77d2-0000-4000-8000-000000000002" } }], has_more: false } }));
  await input.fill("unnamed");
  await page.getByRole("option", { name: "Person 9b1c77", exact: true }).waitFor();
  assert.equal(await page.getByText("No matches.", { exact: true }).count(), 0);
});
