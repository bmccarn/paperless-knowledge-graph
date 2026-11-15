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

const PERCENT = /\d\s*%/;
const DRAFT = "WITHHELD_DRAFT_MARKER";
const ABSTENTION = "I could not verify a complete answer";

async function fixturePage(t, options = {}) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, ...options });
  t.after(() => context.close());
  assert.equal((await (await context.request.get(`${base}/api/_fixture`)).json()).fixture, "paperless-accuracy-ui-v1");
  await context.request.post(`${base}/api/_fixture/reset`);
  const page = await context.newPage();
  page.setDefaultTimeout(15000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  return { context, page };
}
async function ask(page, question, mode) {
  await page.goto(`${base}/query`);
  if (mode) await page.getByRole("button", { name: mode, exact: true }).click();
  const input = page.getByPlaceholder("Ask a question...");
  await input.fill(question);
  await input.press("Enter");
  const answer = page.getByRole("article", { name: "Answer" }).last();
  await answer.waitFor();
  return answer;
}
async function expectState(page, answer, { word, reason, level, name }) {
  const verdict = answer.getByTestId("verdict");
  assert.equal(await verdict.locator("[data-status]").innerText(), word);
  assert.equal(await verdict.locator("[data-status] svg").count(), 1, "the state has its icon");
  assert.ok((await verdict.innerText()).includes(reason), `${word}: ${await verdict.innerText()}`);
  if (level) assert.equal(await verdict.getByRole("img", { name: `Evidence ${level}`, exact: true }).count(), 1);
  else assert.equal(await verdict.getByRole("img", { name: /^Evidence/ }).count(), 0);
  const body = await page.locator("body").innerText();
  assert.doesNotMatch(body, PERCENT, `${word}: no percentage on Ask`);
  assert.equal(body.includes("[Document"), false);
  if (artifacts) await page.screenshot({ path: `${artifacts}/${name}.png`, fullPage: true });
}

const PREMIUM = "The January statement lists a premium of $25.";
// The answer prose is the block the timeline links to as "the full answer".
const prose = answer => answer.locator('[id^="answer-"]');

test("Verified shows its state, reason, level and plain prose; the source sheet has no percentage", async t => {
  const { page } = await fixturePage(t);
  const answer = await ask(page, "What premium does the January statement list?");
  await expectState(page, answer, { word: "Verified", reason: "The claim matches a passage in 1 document.", level: "high", name: "verified" });
  assert.equal(await prose(answer).getByText(PREMIUM, { exact: true }).count(), 1);
  await page.getByRole("button", { name: /January premium statement/ }).last().click();
  await page.getByRole("dialog").waitFor();
  assert.doesNotMatch(await page.locator("body").innerText(), PERCENT);
  await page.getByRole("dialog").getByRole("link", { name: "Open", exact: true }).click();
  await page.waitForURL("**/documents/101");
});

test("Documented gives the newest source date and drops the appended notice", async t => {
  const { page } = await fixturePage(t);
  const answer = await ask(page, "Is the premium still current? state-documented");
  await expectState(page, answer, { word: "Documented", level: "medium", name: "documented",
    reason: "These facts are documented. The newest source is dated 28 Feb 2026. The sources do not show whether they are still true today." });
  assert.equal(await prose(answer).getByText(PREMIUM, { exact: true }).count(), 1);
  assert.equal(await page.getByText(/These are documented facts/).count(), 0);
});

test("Partial says how many claims were left out", async t => {
  const { page } = await fixturePage(t);
  const answer = await ask(page, "Show partial recorded history", "Timeline");
  await expectState(page, answer, { word: "Partial", level: "medium", name: "partial",
    reason: "1 of 2 claims was left out because it did not match a source passage." });
  assert.equal(await page.getByText(/Partial answer: some claims/).count(), 0);
});

for (const [disposition, reason] of [
  ["unsupported", "Not every material claim has validated source support."],
  ["incomplete", "Not every material claim has validated source support."],
  ["current_unresolved", "The audited claims do not establish current-world status or a sufficiently scoped source report."],
  ["timeout", "The source audit exceeded its time budget."],
  ["audit_failed", "The source audit was unavailable or returned invalid data."],
]) test(`Withheld (${disposition}) shows no answer text and offers Deep mode and the documents`, async t => {
  const { page } = await fixturePage(t);
  const question = `What premium is due? state-withheld-${disposition}`;
  const answer = await ask(page, question);
  await expectState(page, answer, { word: "Withheld", reason, level: "low", name: `withheld-${disposition}` });
  const body = await page.locator("body").innerText();
  assert.equal(body.includes(DRAFT), false, "draft claims stay hidden");
  assert.equal(body.includes(ABSTENTION), false, "no answer text");
  await answer.getByRole("button", { name: "How this was checked", exact: true }).click();
  assert.equal((await answer.innerHTML()).includes(DRAFT), false, "the open check section shows no draft claim");
  await answer.getByRole("button", { name: "Review the 2 documents", exact: true }).click();
  assert.ok(await page.evaluate(() => document.activeElement?.getAttribute("aria-label") === "Sources"));
  const request = page.waitForRequest("**/api/query/stream");
  await answer.getByRole("button", { name: "Ask again in Deep mode", exact: true }).click();
  const sent = (await request).postDataJSON();
  assert.equal(sent.mode, "deep");
  assert.equal(sent.question, question);
});

test("Fast answers are Not checked and drop the Fast prefix", async t => {
  const { page } = await fixturePage(t);
  const answer = await ask(page, "What premium does the January statement list?", "Fast");
  await expectState(page, answer, { word: "Not checked", level: "low", name: "not-checked-fast",
    reason: "Fast mode does not check claims. Verify against the sources before relying on this." });
  assert.equal(await prose(answer).getByText(PREMIUM, { exact: true }).count(), 1);
  assert.equal(await page.getByText(/Unaudited quick answer/).count(), 0);
});

test("An index change during the check is Retry needed with no answer text", async t => {
  const { page } = await fixturePage(t);
  const answer = await ask(page, "What premium is due? state-retry");
  await expectState(page, answer, { word: "Retry needed", level: "low", name: "retry",
    reason: "The document index changed during the check. Ask again after indexing finishes." });
  assert.equal(await page.getByText(/Retry after indexing finishes/).count(), 0);
});

for (const scenario of ["stream-error", "interrupt"]) test(`a ${scenario} is Failed with Retry and no level`, async t => {
  const { page } = await fixturePage(t);
  const question = `Run ${scenario} synthetic scenario`;
  const answer = await ask(page, question);
  await expectState(page, answer, { word: "Failed", reason: "The answer failed. Your question was not answered.", name: `failed-${scenario}` });
  assert.equal((await page.locator("body").innerText()).includes("DO_NOT_KEEP"), false);
  const request = page.waitForRequest("**/api/query/stream");
  await answer.getByRole("button", { name: "Retry", exact: true }).click();
  assert.equal((await request).postDataJSON().question, question);
});

for (const reducedMotion of ["no-preference", "reduce"]) test(`a running question shows only a status line (${reducedMotion} motion)`, async t => {
  const { page } = await fixturePage(t, { reducedMotion });
  await page.clock.install({ time: new Date("2026-09-04T12:00:00Z") });
  let release;
  const held = new Promise(resolve => { release = resolve; });
  t.after(() => release());
  await page.route("**/api/query/stream", async route => {
    await held;
    await route.fulfill({ contentType: "text/event-stream", body: 'data: {"type":"error","message":"held"}\n\n' }).catch(() => {});
  });
  await page.goto(`${base}/query`);
  const input = page.getByPlaceholder("Ask a question...");
  await input.fill("A held question");
  await input.press("Enter");
  const status = page.getByRole("status").filter({ hasText: /\d+ s/ });
  await status.waitFor();
  await page.clock.runFor(3000);
  assert.match(await status.innerText(), /Starting.*3 s/s);
  assert.doesNotMatch(await status.ariaSnapshot(), /\d+ s/, "the live region does not announce every second");
  const working = status.getByText("Working", { exact: true });
  assert.equal(await working.isVisible(), reducedMotion === "reduce");
  assert.equal(await status.getByTestId("typing-dots").isVisible(), reducedMotion !== "reduce");
  assert.equal(await page.getByRole("article", { name: "Answer" }).count(), 0, "no answer frame while running");
  if (artifacts) await page.screenshot({ path: `${artifacts}/running-${reducedMotion}.png` });
});

for (const [title, expected] of [
  ["Saved answer that fails re-verification", { word: "Not checked", level: "low",
    reason: "This saved answer could not be re-verified. Ask again to check it." }],
  ["Legacy answer without finalization", { word: "Not checked", level: "high",
    reason: "This answer was saved before the current source checks. Ask again to check it." }],
  ["Older verified answer", { word: "Verified", level: "high", reason: "The claim matches a passage in 1 document." }],
]) test(`a saved answer keeps a state and level: ${title}`, async t => {
  const { context, page } = await fixturePage(t);
  await context.request.post(`${base}/api/_fixture/saved-answers`);
  await page.goto(`${base}/query`);
  await page.getByRole("button", { name: title, exact: true }).click();
  const answer = page.getByRole("article", { name: "Answer" });
  await answer.waitFor();
  await expectState(page, answer, { ...expected, name: title.toLowerCase().replaceAll(" ", "-") });
  assert.equal(await prose(answer).getByText(PREMIUM, { exact: true }).count(), 1);
});
