/** URL selection, focus history and removed-entity recovery on the synthetic graph. */
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

const base = process.env.UI_TEST_BASE_URL;
assert.ok(base && ["127.0.0.1", "localhost"].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || ".browser-artifacts/graph-url-state-ui");
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch();
const errors = [];
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.on("pageerror", error => errors.push(error.message));
  await page.addInitScript(() => {
    window.__graphLabels = [];
    const fillText = CanvasRenderingContext2D.prototype.fillText;
    CanvasRenderingContext2D.prototype.fillText = function (text, ...args) {
      if (this.canvas.isConnected) window.__graphLabels.push(text);
      return fillText.call(this, text, ...args);
    };
  });
  const state = "focus=nb-rowan&view=table&fam=coverage&sel=nb-coverage-1";
  await page.goto(`${base}/graph?${state}`);
  const inspector = page.getByRole("complementary", { name: "Evidence inspector" });
  await inspector.waitFor();
  await inspector.getByRole("heading", { name: "Policy HO-5130 (home)", exact: true }).waitFor();
  const historyLength = await page.evaluate(() => history.length);
  // Focusing the selected neighbor pushes, clears the old selection and retains view/filter state.
  await inspector.getByRole("button", { name: "Focus this entity", exact: true }).click();
  await page.waitForURL(url => url.searchParams.get("focus") === "nb-coverage-1");
  assert.equal(await page.evaluate(() => history.length), historyLength + 1);
  assert.equal(new URL(page.url()).searchParams.get("sel"), null);
  assert.equal(new URL(page.url()).searchParams.get("fam"), "coverage");
  await page.goBack();
  await inspector.getByRole("heading", { name: "Policy HO-5130 (home)", exact: true }).waitFor();
  assert.equal(new URL(page.url()).searchParams.get("focus"), "nb-rowan");
  assert.equal(new URL(page.url()).searchParams.get("view"), "table");
  await page.goForward();
  await page.waitForURL(url => url.searchParams.get("focus") === "nb-coverage-1");

  // An older neighborhood link opens Focus. Unlisted view keys do nothing.
  await page.goto(`${base}/graph?focus=nb-rowan&view=neighborhood`);
  await page.getByText("Rowan Vale: 105 links in 9 families.", { exact: false }).waitFor();
  await page.locator("canvas").first().waitFor();
  assert.equal(await page.getByRole("toolbar", { name: "View" }).count(), 1);
  const keysUrl = page.url();
  const keysHistory = await page.evaluate(() => history.length);
  for (const key of ["1", "n", "2", "p"]) {
    await page.keyboard.press(key);
    await page.waitForTimeout(100);
    assert.equal(page.url(), keysUrl, `key ${key}`);
    assert.equal(await page.evaluate(() => history.length), keysHistory, `key ${key}`);
  }

  // A summary link key restores the renderer's selected link and its label after reload.
  const key = "nb-rowan~COVERS~nb-coverage-1";
  await page.goto(`${base}/graph?${new URLSearchParams({ focus: "nb-rowan", sel: key })}`);
  await page.waitForFunction(() => window.__graphLabels.includes("COVERS · 4 docs"));
  await page.reload();
  await page.waitForFunction(() => window.__graphLabels.includes("COVERS · 4 docs"));
  assert.equal(new URL(page.url()).searchParams.get("sel"), key);

  // Selection replaces the current entry. Closing the inspector also replaces the entry.
  await page.goto(`${base}/graph?focus=nb-rowan&sel=nb-coverage-1`);
  await inspector.waitFor();
  const beforeClose = await page.evaluate(() => history.length);
  await inspector.getByRole("button", { name: "Clear selection", exact: true }).click();
  await page.waitForURL(url => !url.searchParams.has("sel"));
  assert.equal(await page.evaluate(() => history.length), beforeClose);

  for (const [theme, viewport] of [
    ["dark", { width: 1440, height: 900 }], ["light", { width: 1440, height: 900 }],
    ["dark", { width: 390, height: 844 }], ["light", { width: 390, height: 844 }],
  ]) {
    await page.setViewportSize(viewport);
    await page.evaluate(value => localStorage.setItem("theme", value), theme);
    await page.goto(`${base}/graph?focus=removed-entity`);
    await page.getByText("This entity no longer exists. It may have been merged.", { exact: true }).waitFor();
    assert.equal(await page.locator("canvas").count(), 0);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(artifacts, `missing-${viewport.width}-${theme}.png`) });
  }
  await page.getByRole("textbox", { name: "Find an entity", exact: true }).fill("Harbor & Sons");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page.waitForURL(url => url.searchParams.get("q") === "Harbor & Sons");
  assert.equal(new URL(page.url()).searchParams.has("focus"), false);

  // A service failure is retryable and must not claim that the entity was removed.
  await page.route("**/api/graph/node/nb-rowan?summary=1", route => route.fulfill({ status: 503, json: { detail: "Synthetic outage" } }));
  await page.goto(`${base}/graph?focus=nb-rowan`);
  await page.getByRole("alert").filter({ hasText: "Could not load the neighborhood" }).waitFor();
  assert.equal(await page.getByText("This entity no longer exists. It may have been merged.", { exact: true }).count(), 0);
  await page.unroute("**/api/graph/node/nb-rowan?summary=1");
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await page.getByText("Rowan Vale: 105 links in 9 families.", { exact: false }).waitFor();
  assert.deepEqual(errors, []);
  console.log("PASS graph URL focus Back/Forward, stable link selection reload, replace selection, removed entity recovery, retryable service errors");
} finally {
  await browser.close();
}
