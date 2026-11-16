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
      // The Graph tab opens a graph at once, and its search box does not take focus.
      if (route === "/graph") await page.waitForURL(url => url.searchParams.has("focus"));
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
        const health = await page.getByText("Backend up", { exact: true }).boundingBox();
        const toggle = await page.getByRole("button", { name: "Toggle theme", exact: true }).boundingBox();
        assert.ok(health.y > 800 && toggle.y > 800, "Sidebar health and theme controls belong at the foot");
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

test("document failures show the document not found state with retry", async t => {
  const context = await browser.newContext();
  t.after(() => context.close());
  const page = await context.newPage();
  await page.route("**/api/document/101/detail", route => route.fulfill({ status: 503, json: { detail: "Unavailable" } }));
  await page.goto(`${base}/documents/101`);
  await page.getByRole("heading", { name: "Document not found", level: 1 }).waitFor();
  assert.equal(await page.locator("h1").count(), 1);
  assert.ok(await page.getByRole("button", { name: "Retry loading document" }).isVisible());
});

for (const theme of ["dark", "light"]) test(`phone tabs and More sheet: ${theme}`, async t => {
  const context = await browser.newContext({ viewport: { width: 390, height: 844 }, reducedMotion: "reduce" });
  t.after(() => context.close());
  await context.addInitScript(theme => localStorage.setItem("theme", theme), theme);
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  await page.goto(`${base}/query`);
  const nav = page.getByRole("navigation", { name: "Phone", exact: true });
  assert.deepEqual(await nav.getByRole("link").allTextContents(), ["Ask", "Documents", "Graph", "Entities"]);
  assert.equal(await nav.evaluate(el => el.getBoundingClientRect().height), 56);
  for (const control of await nav.locator("a, button").all()) {
    const icon = await control.locator("svg").boundingBox();
    const label = await control.locator("span").boundingBox();
    const box = await control.boundingBox();
    assert.ok(icon.y + icon.height <= label.y, "The icon must not overlap its label");
    assert.ok(box.width >= 44 && box.height >= 44, "Phone controls need 44 px targets");
    assert.equal(await control.locator("span").evaluate(el => getComputedStyle(el).fontSize), "12px");
  }
  const more = nav.getByRole("button", { name: "More", exact: true });
  await more.click();
  const sheet = page.getByRole("dialog", { name: "More", exact: true });
  assert.deepEqual(await sheet.getByRole("link").allTextContents(), ["Status", "Hubs", "Logs"]);
  await sheet.getByText("Backend up", { exact: true }).waitFor();
  const toggle = sheet.getByRole("button", { name: "Toggle theme", exact: true });
  await toggle.click();
  assert.equal(await page.locator("html").evaluate(el => el.classList.contains("dark")), theme !== "dark");
  await page.keyboard.press("Escape");
  await sheet.waitFor({ state: "hidden" });
  assert.equal(await more.evaluate(el => el === document.activeElement), true);
  await more.click();
  await sheet.getByRole("link", { name: "Hubs", exact: true }).click();
  await page.waitForURL("**/hubs");
  await sheet.waitFor({ state: "hidden" });
  assert.equal(await more.getAttribute("data-active"), "true");
});

for (const theme of ["dark", "light"]) test(`Ask keyboard focus clears the composer and tabs: ${theme}`, async t => {
  const context = await browser.newContext({ viewport: { width: 390, height: 844 }, reducedMotion: "reduce" });
  t.after(() => context.close());
  await context.addInitScript(theme => localStorage.setItem("theme", theme), theme);
  const page = await context.newPage();
  page.setDefaultTimeout(5000);
  await page.goto(`${base}/query`);
  const input = page.getByPlaceholder("Ask a question...");
  await input.fill("What premium does the January statement list?");
  await input.press("Enter");
  await page.getByRole("button", { name: /^Claim table/ }).waitFor();
  // Exercise a long answer's controls, including controls at the end of its scroll region.
  await page.getByRole("link", { name: "Skip to content", exact: true }).focus();
  const visited = new Set();
  for (let i = 0; i < 100; i++) {
    await page.keyboard.press("Tab");
    const focused = page.locator(":focus");
    if (await focused.count() === 0) break;
    const info = await focused.evaluate(el => {
      const box = el.getBoundingClientRect();
      const scroll = el.closest(".app-scroll");
      const clip = scroll?.getBoundingClientRect();
      return { name: el.getAttribute("aria-label") || el.textContent || el.tagName, top: box.top, bottom: box.bottom,
        clipTop: clip?.top, clipBottom: clip?.bottom, padding: scroll && parseFloat(getComputedStyle(scroll).scrollPaddingBottom),
        composer: document.querySelector("textarea").closest("form").parentElement.parentElement.getBoundingClientRect().height };
    });
    if (info.name === "Skip to content") break;
    visited.add(info.name);
    assert.ok(info.top >= 0 && info.bottom <= 844, `${info.name} must remain in the viewport`);
    if (info.clipBottom !== undefined) {
      assert.ok(info.top >= info.clipTop && info.bottom <= info.clipBottom, `${info.name} must clear the composer`);
      assert.ok(info.padding >= info.composer + 56, "Scroll padding includes the composer and tab bar");
    }
    if (await focused.evaluate(el => el.closest("main") !== null)) {
      const bar = await page.getByRole("navigation", { name: "Phone", exact: true }).boundingBox();
      assert.ok(info.bottom <= bar.y, `${info.name} must clear the tab bar`);
    }
  }
  assert.ok(visited.has("More"));
  assert.ok(visited.has("Model"));
});
