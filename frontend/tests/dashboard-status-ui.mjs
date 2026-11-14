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

async function fixturePage(t) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
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

for (const error of ["paperless down", ""]) test(`degraded first load shows ${error || "fallback"} and retry recovers`, async t => {
  const { page, healthy } = await fixturePage(t);
  let response = { status: "degraded", error };
  await page.route("**/api/status", route => route.fulfill({ json: response }));
  await page.goto(base);
  await page.getByText(error || "Backend degraded", { exact: true }).waitFor();
  assert.equal(await page.getByRole("heading", { name: "Dashboard", exact: true }).count(), 0);
  response = healthy;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await page.getByRole("heading", { name: "Dashboard", exact: true }).waitFor();
  assert.equal(await page.getByText("Connection Error", { exact: true }).count(), 0);
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
  await page.getByRole("heading", { name: "Dashboard", exact: true }).waitFor();
  fail = true;
  await page.clock.fastForward(10000);
  const warning = page.getByRole("alert").filter({ hasText: "Showing the last successful status" });
  await warning.waitFor();
  assert.match(await warning.innerText(), /Showing the last successful status/);
  if (failure === "degraded") assert.match(await warning.innerText(), /paperless down/);
  if (failure === "http") assert.match(await warning.innerText(), /503 status unavailable/);
  assert.equal(await page.getByText("Indexed documents", { exact: true }).isVisible(), true);
  await page.getByText("Unavailable", { exact: true }).waitFor();
  if (artifacts) await page.screenshot({ path: `${artifacts}/${failure}-poll.png`, fullPage: true });
  fail = false;
  await page.clock.fastForward(10000);
  await warning.waitFor({ state: "hidden" });
  await page.getByText("healthy", { exact: true }).waitFor();
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
  assert.equal(await page.getByText("Unavailable", { exact: true }).count(), 0);
  assert.equal(await page.getByText("healthy", { exact: true }).isVisible(), true);
});
