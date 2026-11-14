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
after(() => browser?.close());

for (const running of [false, true]) test(`auto-submit survives reload ${running ? "during" : "after"} the query without resubmitting`, async t => {
  const context = await browser.newContext();
  t.after(() => context.close());
  const identity = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await identity.json()).fixture, "paperless-accuracy-ui-v1");
  await context.request.post(`${base}/api/_fixture/reset`);
  let conversations = 0, queries = 0;
  context.on("request", request => {
    if (request.method() !== "POST") return;
    const pathname = new URL(request.url()).pathname;
    if (pathname === "/api/conversations") conversations++;
    if (pathname === "/api/query/stream") queries++;
  });
  let release;
  const held = new Promise(resolve => { release = resolve; });
  t.after(release);
  await context.route("**/api/query/stream", async route => {
    if (running) await held;
    await route.fulfill({ contentType: "text/event-stream", body: 'data: {"type":"complete","answer":"Synthetic answer","sources":[]}\n\n' }).catch(() => {});
  });
  const page = await context.newPage();
  page.setDefaultTimeout(15000);
  const started = page.waitForRequest("**/api/query/stream");
  await page.goto(`${base}/query?q=reload%20question&keep=yes#answer`);
  await started;
  if (!running) await page.getByText("Synthetic answer", { exact: true }).waitFor();
  await page.reload();
  // A held stream must remain pending until after the reload has mounted.
  await page.getByPlaceholder("Ask a question...").waitFor();
  release();
  await page.waitForLoadState("networkidle");
  assert.equal(conversations, 1, "Reload created another conversation");
  assert.equal(queries, 1, "Reload submitted the same question again");
  assert.equal(new URL(page.url()).searchParams.has("q"), false);
  assert.equal(new URL(page.url()).searchParams.get("keep"), "yes");
  assert.equal(new URL(page.url()).hash, "#answer");
  const consumedUrl = page.url();
  await page.goto(`${base}/documents`);
  await page.goBack();
  await page.waitForLoadState("networkidle");
  const reopened = await context.newPage();
  await reopened.goto(consumedUrl);
  await reopened.waitForLoadState("networkidle");
  assert.equal(conversations, 1, "History or a reopened tab created another conversation");
  assert.equal(queries, 1, "History or a reopened tab resubmitted the question");
  if (artifacts) await page.screenshot({ path: `${artifacts}/reload-${running ? "running" : "completed"}.png` });
});
