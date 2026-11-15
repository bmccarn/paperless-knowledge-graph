/** The documents table: keyboard expansion, visible sort state, readable types and the entity filter. */
import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { chromium } from "playwright";

const base = process.env.UI_TEST_BASE_URL || "http://127.0.0.1:3006";
assert.ok(["127.0.0.1", "localhost"].includes(new URL(base).hostname));
let browser;
before(async () => { browser = await chromium.launch({ headless: true }); });
after(async () => { await browser?.close(); });

async function documentsPage(t, path = "/documents", viewport = { width: 1440, height: 900 }) {
  const context = await browser.newContext({ viewport });
  t.after(() => context.close());
  assert.equal((await (await context.request.get(`${base}/api/_fixture`)).json()).fixture, "paperless-accuracy-ui-v1");
  await context.request.post(`${base}/api/_fixture/reset`);
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.goto(`${base}${path}`);
  return page;
}

test("Tab reaches a row's expand button and Enter shows readable details", async t => {
  const page = await documentsPage(t);
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  const expand = page.getByRole("button", { name: "Show details for January premium statement", exact: true });
  await page.getByRole("checkbox", { name: "Select document 101", exact: true }).focus();
  await page.keyboard.press("Tab");
  assert.equal(await expand.evaluate(element => element === document.activeElement), true);
  assert.equal(await expand.getAttribute("aria-expanded"), "false");
  await page.keyboard.press("Enter");
  assert.equal(await expand.getAttribute("aria-expanded"), "true");
  const details = page.locator(`#${await expand.getAttribute("aria-controls")}`);
  for (const label of ["Paperless ID", "Title", "Date", "Type"]) await details.getByText(label, { exact: true }).waitFor();
  await details.getByText("31 Jan 2026", { exact: true }).waitFor();
  assert.equal(await details.getByRole("link", { name: "Example Utility", exact: true }).getAttribute("href"), "/graph?focus=org-1");
  assert.equal(await details.getByText("paperless_id", { exact: true }).count(), 0);
  await page.keyboard.press("Enter");
  assert.equal(await expand.getAttribute("aria-expanded"), "false");
  assert.equal(await details.count(), 0);
});

test("activating Date sorts ascending and marks Date only", async t => {
  const page = await documentsPage(t);
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  const sorted = page.locator("th[aria-sort]");
  assert.deepEqual(await sorted.allTextContents(), ["Title"]);
  const loaded = page.waitForResponse(response => new URL(response.url()).searchParams.get("sort") === "date");
  await page.getByRole("button", { name: "Date", exact: true }).click();
  await loaded;
  await page.locator('th[aria-sort="ascending"]').filter({ hasText: "Date" }).waitFor();
  assert.deepEqual(await sorted.allTextContents(), ["Date"]);
  await page.getByRole("button", { name: "Date", exact: true }).click();
  await page.locator('th[aria-sort="descending"]').filter({ hasText: "Date" }).waitFor();
});

test("type chips and dots show readable labels and toggle with aria-pressed", async t => {
  const page = await documentsPage(t);
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  const chips = page.getByRole("group", { name: "Filters" }).getByRole("button");
  assert.deepEqual(await chips.allTextContents(), ["Invoice 151", "Medical 150", "Financial statement 1", "Insurance 1"]);
  const insurance = page.getByRole("button", { name: "Insurance 1", exact: true });
  assert.equal(await insurance.getAttribute("aria-pressed"), "false");
  await insurance.click();
  await page.getByText("1 matching indexed documents", { exact: true }).waitFor();
  assert.equal(await insurance.getAttribute("aria-pressed"), "true");
  const row = page.getByRole("row").filter({ hasText: "January premium statement" });
  assert.equal(await row.locator('[data-type="insurance"]').filter({ visible: true }).textContent(), "Insurance");
  assert.equal(await page.getByText("insurance", { exact: true }).count(), 0);
});

test("an entity link lists only that entity's documents with a removable chip", async t => {
  const page = await documentsPage(t, "/documents?entity=org-1");
  await page.getByText("2 matching indexed documents", { exact: true }).waitFor();
  assert.deepEqual(await page.getByRole("row").locator("a.doc-title").allTextContents(), ["January premium statement", "Utility statement — February"]);
  await page.getByRole("button", { name: "Remove entity filter Example Utility", exact: true }).click();
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  assert.equal(new URL(page.url()).search, "");
});

test("an empty result offers to clear the filters", async t => {
  const page = await documentsPage(t);
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  await page.getByRole("textbox", { name: "Search indexed documents" }).fill("no such synthetic title");
  await page.getByRole("button", { name: "Search documents", exact: true }).click();
  await page.getByText("No documents match these filters.", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Clear filters", exact: true }).click();
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  assert.equal(await page.getByRole("textbox", { name: "Search indexed documents" }).inputValue(), "");
});

test("on a phone each row shows type and date under the title", async t => {
  const page = await documentsPage(t, "/documents", { width: 390, height: 844 });
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  const row = page.getByRole("row").filter({ hasText: "January premium statement" });
  const title = await row.locator("a.doc-title").boundingBox();
  const meta = await row.locator('[data-type="insurance"]').filter({ visible: true }).boundingBox();
  assert.ok(meta.y >= title.y + title.height - 1, "type sits on the line under the title");
  await row.getByText("31 Jan 2026", { exact: true }).filter({ visible: true }).waitFor();
  await page.getByRole("button", { name: "Show details for January premium statement", exact: true }).click();
  await page.getByText("Paperless ID", { exact: true }).waitFor();
  // The full-width details row must not add the columns phones hide.
  const expandedTitle = await row.locator("a.doc-title").boundingBox();
  const reindex = await row.getByRole("button", { name: "Reindex document 101", exact: true }).boundingBox();
  assert.ok(expandedTitle.width >= title.width - 1, "the title keeps its width");
  assert.ok(expandedTitle.x + expandedTitle.width <= reindex.x, "the title does not run under the reindex button");
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
});

test("a reload after an error keeps the last rows, shows progress and locks selection", async t => {
  const page = await documentsPage(t);
  await page.getByText("303 indexed documents", { exact: true }).waitFor();
  const search = page.getByRole("textbox", { name: "Search indexed documents" });
  await search.fill("fixture-error");
  await page.getByRole("button", { name: "Search documents", exact: true }).click();
  await page.getByRole("alert").filter({ hasText: "Synthetic document search failure" }).waitFor();
  let release;
  const held = new Promise(resolve => { release = resolve; });
  await page.route("**/api/documents?**", async route => { await held; await route.continue(); });
  await search.fill("January");
  await page.getByRole("button", { name: "Search documents", exact: true }).click();
  await page.getByRole("progressbar", { name: "Loading documents" }).waitFor();
  const checkbox = page.getByRole("checkbox", { name: "Select document 101", exact: true });
  assert.equal(await checkbox.isDisabled(), true);
  release();
  await page.getByText("1 matching indexed documents", { exact: true }).waitFor();
  assert.equal(await checkbox.isDisabled(), false);
});
