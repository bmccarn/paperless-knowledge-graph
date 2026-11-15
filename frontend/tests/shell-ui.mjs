/** The app frame preserves route access and keyboard navigation in both themes. */
import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";

const base = process.env.UI_TEST_BASE_URL || "http://127.0.0.1:3006";
assert.ok(["127.0.0.1", "localhost"].includes(new URL(base).hostname));
const routes = [["/query", "Ask"], ["/documents", "Documents"], ["/graph", "Graph"], ["/entities/review", "Entities"], ["/", "Status"], ["/hubs", "Hubs"], ["/debug", "Logs"]];
let browser;
before(async () => { browser = await chromium.launch({ headless: true }); });
after(async () => { await browser?.close(); });

for (const theme of ["dark", "light"]) for (const width of [1440, 390]) {
  test(`app frame: ${theme}, ${width}px`, { timeout: 30000 }, async t => {
    const context = await browser.newContext({ viewport: { width, height: width === 390 ? 844 : 900 }, reducedMotion: "reduce" });
    t.after(() => context.close());
    await context.addInitScript(theme => localStorage.setItem("theme", theme), theme);
    const page = await context.newPage();
    page.setDefaultTimeout(5000);
    for (const [route, label] of routes) {
      const response = await page.goto(`${base}${route}`);
      assert.equal(response.status(), 200);
      await page.getByRole("heading", { name: label, exact: true, level: 1 }).waitFor();
      assert.equal(await page.locator("h1").count(), 1);
      await page.keyboard.press("Tab");
      assert.equal(await page.locator(":focus").textContent(), "Skip to content");
      await page.keyboard.press("Enter");
      assert.equal(await page.locator(":focus").getAttribute("id"), "main-content");
      await page.waitForFunction(() => document.querySelector("h1") && getComputedStyle(document.querySelector("h1")).fontWeight === "400");
      if (width === 1440) {
        const nav = page.getByRole("navigation", { name: "Main", exact: true });
        assert.deepEqual(await nav.getByRole("link").allTextContents(), routes.map(([, name]) => name));
        assert.equal(await nav.getByRole("link", { name: label, exact: true }).getAttribute("aria-current"), "page");
        assert.equal(await nav.locator('[aria-current="page"]').count(), 1);
        assert.equal((await page.getByRole("complementary", { name: "App navigation" }).boundingBox()).width, 200);
        assert.equal((await nav.getByRole("link").first().boundingBox()).height, 32);
        await page.getByText("Backend up", { exact: true }).waitFor();
      }
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${route} must fit`);
    }
    const response = await page.goto(`${base}/visual-missing-page`);
    assert.equal(response.status(), 404);
    await page.getByRole("heading", { name: "Page not found", level: 1 }).waitFor();
    assert.equal(await page.locator("h1").count(), 1);
    assert.equal(await page.locator("main").getByRole("link", { name: "Ask", exact: true }).getAttribute("href"), "/query");
    assert.equal(await page.locator("main").getByRole("link", { name: "Documents", exact: true }).getAttribute("href"), "/documents");
    if (process.env.UI_TEST_ARTIFACTS) {
      await mkdir(process.env.UI_TEST_ARTIFACTS, { recursive: true });
      await page.screenshot({ path: `${process.env.UI_TEST_ARTIFACTS}/not-found-${theme}-${width}.png` });
    }
    await page.goto(`${base}/gallery`);
    assert.equal(await page.getByRole("complementary", { name: "App navigation" }).count(), 0);
    assert.equal(await page.locator("main").evaluate(el => getComputedStyle(el).paddingBottom), "0px");
  });
}

test("backend health distinguishes degraded, failed and recovered polls", { timeout: 20000 }, async t => {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  t.after(() => context.close());
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  await page.clock.install();
  let status = "healthy";
  await page.route("**/api/status", route => status === "network" ? route.abort() : route.fulfill({ json: { status } }));
  await page.goto(`${base}/visual-missing-page`);
  await page.getByText("Backend up", { exact: true }).waitFor();
  for (status of ["degraded", "network", "healthy"]) {
    await page.clock.runFor(30000);
    await page.getByText(status === "healthy" ? "Backend up" : "Backend down", { exact: true }).waitFor();
  }
});

test("document failures keep a page heading without claiming the document is gone", async t => {
  const context = await browser.newContext();
  t.after(() => context.close());
  const page = await context.newPage();
  await page.route("**/api/document/101/detail", route => route.fulfill({ status: 503, json: { detail: "Unavailable" } }));
  await page.goto(`${base}/documents/101`);
  await page.getByRole("heading", { name: "Document unavailable", level: 1 }).waitFor();
  assert.equal(await page.locator("h1").count(), 1);
  assert.ok(await page.getByRole("button", { name: "Retry loading document" }).isVisible());
});
