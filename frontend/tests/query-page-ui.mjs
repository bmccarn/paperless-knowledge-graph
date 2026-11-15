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

test("conversation scroll position stays unchanged while typing", async t => {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  t.after(() => context.close());
  const identity = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await identity.json()).fixture, "paperless-accuracy-ui-v1");
  const page = await context.newPage();
  page.setDefaultTimeout(15000);
  await page.route("**/api/conversations?*", route => route.fulfill({ json: Array.from({ length: 50 }, (_, index) => ({
    id: `conversation-${index}`, title: `Conversation ${index}`, message_count: 0,
  })) }));
  await page.goto(`${base}/query`);
  await page.getByRole("button", { name: "Conversation 49", exact: true }).waitFor();
  const scroll = page.locator("[data-conversation-scroll]").filter({ visible: true });
  const before = await scroll.evaluate(element => {
    element.scrollTop = element.scrollHeight;
    return element.scrollTop;
  });
  assert.ok(before > 0, "The conversation list must overflow");
  await page.getByPlaceholder("Ask a question...").pressSequentially("abc");
  assert.equal(await scroll.evaluate(element => element.scrollTop), before);
  if (artifacts) await page.screenshot({ path: `${artifacts}/conversation-scroll.png` });
});

test("Ask controls expose their labels and one selected mode", async t => {
  const context = await browser.newContext({ viewport: { width: 390, height: 844 } });
  t.after(() => context.close());
  const page = await context.newPage();
  await page.goto(`${base}/query`);
  await page.getByRole("textbox", { name: "Ask a question", exact: true }).waitFor();
  const modes = page.getByRole("group", { name: "Query mode" });
  for (const name of ["Fast", "Deep", "Timeline", "Strict"]) {
    const mode = modes.getByRole("button", { name, exact: true });
    await mode.click();
    assert.equal(await mode.getAttribute("aria-pressed"), "true");
    assert.equal(await modes.locator('[aria-pressed="true"]').count(), 1);
    await mode.click();
    assert.equal(await mode.getAttribute("aria-pressed"), "true", "A mode must remain selected");
  }
  const selected = page.waitForRequest(request => request.url().endsWith("/api/query/stream") && request.method() === "POST");
  const model = page.getByRole("combobox", { name: "Model", exact: true });
  await model.click();
  const selectedName = await page.getByRole("option").last().textContent();
  await page.getByRole("option").last().click();
  await page.getByRole("textbox", { name: "Ask a question", exact: true }).fill("Check selected controls");
  await page.getByRole("button", { name: "Send question", exact: true }).click();
  const payload = (await selected).postDataJSON();
  assert.equal(payload.mode, "strict");
  const response = await context.request.get(`${base}/api/models`);
  const models = (await response.json()).models;
  assert.equal(payload.model, models.find(model => selectedName.startsWith(model.name)).id);
  assert.equal(await page.getByRole("textbox", { name: "Ask a question", exact: true }).evaluate(el => getComputedStyle(el).fontSize), "16px");
});

for (const width of [1440, 390]) test(`conversation delete confirms, cancels, restores focus and reports failure at ${width}px`, async t => {
  const context = await browser.newContext({ viewport: { width, height: 844 } });
  t.after(() => context.close());
  const page = await context.newPage();
  let deleted = 0;
  let failDelete = false;
  let items = [{ id: "controls-test", title: "Test conversation", message_count: 0 }];
  await page.route("**/api/conversations?*", route => route.fulfill({ json: items }));
  await page.route("**/api/conversations/controls-test", async route => {
    assert.equal(route.request().method(), "DELETE");
    deleted++;
    if (!failDelete) items = [];
    await route.fulfill({ status: failDelete ? 500 : 200, json: {} });
  });
  await page.goto(`${base}/query`);
  if (width === 390) await page.getByRole("button", { name: "Open conversations", exact: true }).click();
  const trigger = page.getByRole("button", { name: "Delete Test conversation", exact: true });
  await trigger.click();
  const dialog = page.getByRole("alertdialog", { name: "Delete conversation?" });
  await dialog.waitFor();
  assert.equal(deleted, 0);
  await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  assert.equal(deleted, 0);
  assert.ok(await trigger.evaluate(el => el === document.activeElement));
  await trigger.click();
  failDelete = true;
  await dialog.getByRole("button", { name: "Delete conversation", exact: true }).click();
  await dialog.getByRole("alert").waitFor();
  assert.equal(deleted, 1);
  assert.ok(await dialog.isVisible());
  failDelete = false;
  await dialog.getByRole("button", { name: "Delete conversation", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  await page.getByText("No conversations yet.", { exact: true }).filter({ visible: true }).waitFor();
  assert.equal(deleted, 2);
  assert.ok(await page.getByRole("button", { name: "New conversation", exact: true }).filter({ visible: true }).evaluate(el => el === document.activeElement));
});

test("new phone answers keep the verdict visible and preserve markdown lists", async t => {
  const context = await browser.newContext({ viewport: { width: 390, height: 844 }, reducedMotion: "reduce" });
  t.after(() => context.close());
  const page = await context.newPage();
  await page.route("**/api/query/stream", route => route.fulfill({ contentType: "text/event-stream", body:
    `data: ${JSON.stringify({ type: "complete", answer: "- First item\n- Second item\n\n1. Ordered item\n2. Other item\n\n" + "A long paragraph of synthetic source text.\n\n".repeat(40), sources: [], finalization: { disposition: "unaudited" } })}\n\n` }));
  await page.goto(`${base}/query`);
  const input = page.getByRole("textbox", { name: "Ask a question", exact: true });
  await input.fill("Explain the synthetic list");
  await input.press("Enter");
  const article = page.getByRole("article", { name: "Answer" });
  await article.waitFor();
  await article.locator("ul li").first().waitFor();
  assert.equal(await article.locator("ul li").count(), 2);
  assert.equal(await article.locator("ol li").count(), 2);
  const verdict = await article.getByTestId("verdict").boundingBox();
  const viewport = await page.locator(".app-scroll").boundingBox();
  assert.ok(verdict.y >= viewport.y && verdict.y + verdict.height <= viewport.y + viewport.height, "The verdict must stay visible above the composer");
});

for (const width of [1440, 390]) test(`rename retains focus and deleting the active conversation has a focus target at ${width}px`, async t => {
  const context = await browser.newContext({ viewport: { width, height: 844 } });
  t.after(() => context.close());
  const page = await context.newPage();
  let exists = true;
  let title = "Rename example";
  await page.route("**/api/conversations?*", route => route.fulfill({ json: exists ? [{ id: "rename-test", title, message_count: 0 }] : [] }));
  await page.route("**/api/conversations/rename-test", async route => {
    if (route.request().method() === "PATCH") title = route.request().postDataJSON().title;
    if (route.request().method() === "DELETE") exists = false;
    await route.fulfill({ json: { id: "rename-test", title, messages: [] } });
  });
  await page.goto(`${base}/query`);
  const openHistory = async () => { if (width === 390) await page.getByRole("button", { name: "Open conversations", exact: true }).click(); };
  await openHistory();
  await page.getByRole("button", { name: "Rename Rename example", exact: true }).click();
  let input = page.getByRole("textbox", { name: "Conversation title", exact: true });
  await input.fill("Cancelled title");
  await input.press("Escape");
  if (width === 390) assert.equal(await page.getByRole("dialog", { name: "Conversations", exact: true }).getAttribute("data-state"), "open");
  assert.ok(await page.getByRole("button", { name: "Rename Rename example", exact: true }).evaluate(el => el === document.activeElement));
  await page.getByRole("button", { name: "Rename Rename example", exact: true }).click();
  input = page.getByRole("textbox", { name: "Conversation title", exact: true });
  await input.fill("Saved title");
  await input.press("Enter");
  await page.getByRole("button", { name: "Rename Saved title", exact: true }).waitFor();
  assert.ok(await page.getByRole("button", { name: "Rename Saved title", exact: true }).evaluate(el => el === document.activeElement));
  await page.getByRole("button", { name: "Saved title", exact: true }).click();
  await page.getByRole("textbox", { name: "Ask a question", exact: true }).waitFor();
  await openHistory();
  await page.getByRole("button", { name: "Delete Saved title", exact: true }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Delete conversation", exact: true }).click();
  await page.getByRole("alertdialog").waitFor({ state: "hidden" });
  assert.ok(await page.locator(":focus").evaluate(el => el !== document.body && el.isConnected && el.getBoundingClientRect().height > 0));
});
