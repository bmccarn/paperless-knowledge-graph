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

const QUESTION = "What does the policy say? margin-claims";

async function ask(t, width, question = QUESTION, mode) {
  const context = await browser.newContext({ viewport: { width, height: 900 } });
  t.after(() => context.close());
  await context.request.post(`${base}/api/_fixture/reset`);
  const page = await context.newPage();
  page.setDefaultTimeout(15000);
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  await page.goto(`${base}/query`);
  if (mode) await page.getByRole("button", { name: mode, exact: true }).click();
  const input = page.getByPlaceholder("Ask a question...");
  await input.fill(question);
  await input.press("Enter");
  const answer = page.getByRole("article", { name: "Answer" }).last();
  await answer.waitFor();
  await page.evaluate(() => document.fonts.ready);
  return { page, answer };
}
const noHorizontalScroll = page => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth
  && [...document.querySelectorAll("main *")].every(element => element.scrollWidth <= element.clientWidth + 1 || getComputedStyle(element).overflowX !== "auto"));

test("each Verified claim is underlined and numbered, with its note in the margin at 1440 px", async t => {
  const { page, answer } = await ask(t, 1440);
  const prose = answer.locator('[id^="answer-"]');
  for (const n of [1, 2, 3]) assert.equal(await answer.getByRole("button", { name: `Claim ${n}`, exact: true }).count(), 1);
  // Claim 2 follows an emoji; its text is the exact ledger slice.
  assert.equal(await answer.locator('[data-claim="2"]').last().innerText(), "Amount: The January statement lists a premium of $25.");
  assert.equal(await prose.getByRole("heading", { name: "Premium" }).locator('[data-claim="2"]').count(), 1);
  assert.equal(await prose.locator("strong").filter({ hasText: "Amount:" }).count(), 1);
  assert.equal((await prose.innerText()).includes("*"), false, "no literal asterisks");
  assert.equal((await page.locator("body").innerText()).includes("[Document"), false);
  // One snapshot: the page may still be scrolling to the new answer.
  const layout = await prose.evaluate(element => [1, 2, 3].map(n => ({
    prose: element.getBoundingClientRect(),
    claim: element.querySelector(`[data-claim="${n}"]`).getBoundingClientRect(),
    note: element.querySelector(`[aria-label="Claim ${n} source"]`).getBoundingClientRect(),
  })));
  layout.forEach(({ prose, claim, note }, i) => {
    assert.ok(note.top >= claim.top - 0.5, `note ${i + 1} top ${note.top} is level with or below its claim ${claim.top}`);
    assert.ok(note.left >= prose.right, `note ${i + 1} sits in the margin`);
  });
  assert.ok(await noHorizontalScroll(page), "no horizontal scroll");
  // A source without a date shows its title and no date text or placeholder.
  assert.equal(await answer.getByRole("note", { name: "Claim 1 source" }).locator("span.block").last().innerText(), "January premium statement · 31 Jan 2026");
  assert.equal(await answer.getByRole("note", { name: "Claim 3 source" }).locator("span.block").last().innerText(), "Utility statement — February");
  if (artifacts) await page.screenshot({ path: `${artifacts}/margin-1440.png`, fullPage: true });
});

test("selecting claim 2 marks the claim and its note; a claim number scrolls to its note without changing the URL", async t => {
  const { page, answer } = await ask(t, 1440);
  await answer.locator('[data-claim="2"]').last().click();
  for (const piece of await answer.locator('[data-claim="2"]').all()) assert.equal(await piece.getAttribute("aria-current"), "true");
  assert.equal(await answer.getByRole("note", { name: "Claim 2 source" }).getAttribute("aria-current"), "true");
  assert.equal(await answer.locator('[aria-current="true"][data-claim], [role="note"][aria-current="true"]').count(), 3);
  const url = page.url();
  await answer.getByRole("button", { name: "Claim 3", exact: true }).click();
  assert.equal(page.url(), url);
  const note = answer.getByRole("note", { name: "Claim 3 source" });
  assert.equal(await note.getAttribute("aria-current"), "true");
  assert.equal(await answer.getByRole("note", { name: "Claim 2 source" }).getAttribute("aria-current"), null);
  assert.ok(await note.evaluate(element => { const r = element.getBoundingClientRect(); return r.top >= 0 && r.bottom <= innerHeight; }));
});

test("at 1024 px the notes run inline under their claims", async t => {
  const { page, answer } = await ask(t, 1024);
  const layout = await answer.locator('[id^="answer-"]').evaluate(element => [1, 2, 3].map(n => ({
    prose: element.getBoundingClientRect(),
    claim: [...element.querySelectorAll(`[data-claim="${n}"]`)].pop().getBoundingClientRect(),
    note: element.querySelector(`[aria-label="Claim ${n} source"]`).getBoundingClientRect(),
  })));
  layout.forEach(({ prose, claim, note }, i) => {
    assert.ok(note.left >= prose.left && note.right <= prose.right + 0.5, `note ${i + 1} is inside the column`);
    assert.ok(note.top >= claim.bottom - 0.5, `note ${i + 1} is under its claim`);
  });
  assert.ok(await noHorizontalScroll(page), "no horizontal scroll");
  if (artifacts) await page.screenshot({ path: `${artifacts}/inline-1024.png`, fullPage: true });
});

test("at 390 px only the active note is visible, and a claim number opens it", async t => {
  const { page, answer } = await ask(t, 390);
  const visibleNotes = () => answer.getByRole("note").evaluateAll(notes => notes.filter(note => note.checkVisibility()).length);
  assert.equal(await visibleNotes(), 0);
  await answer.getByRole("button", { name: "Claim 2", exact: true }).click();
  assert.equal(await visibleNotes(), 1);
  assert.ok(await answer.getByRole("note", { name: "Claim 2 source" }).isVisible());
  assert.ok(await noHorizontalScroll(page), "no horizontal scroll");
  if (artifacts) await page.screenshot({ path: `${artifacts}/phone-390.png`, fullPage: true });
});

test("a Not checked answer shows no underlines and no notes", async t => {
  const { answer } = await ask(t, 1440, QUESTION, "Fast");
  assert.equal(await answer.getByTestId("verdict").locator("[data-status]").innerText(), "Not checked");
  assert.ok((await answer.innerText()).includes("The January statement lists a premium of $25."));
  assert.equal(await answer.locator("[data-claim]").count(), 0);
  assert.equal(await answer.getByRole("note").count(), 0);
});

test("an older saved Verified answer shows plain prose with no claims and no literal citation", async t => {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  t.after(() => context.close());
  await context.request.post(`${base}/api/_fixture/reset`);
  await context.request.post(`${base}/api/_fixture/saved-answers`);
  const page = await context.newPage();
  await page.goto(`${base}/query`);
  await page.getByRole("button", { name: "Older verified answer", exact: true }).click();
  const answer = page.getByRole("article", { name: "Answer" });
  await answer.waitFor();
  assert.equal(await answer.getByTestId("verdict").locator("[data-status]").innerText(), "Verified");
  assert.equal(await answer.locator("[data-claim]").count(), 0);
  assert.equal(await answer.getByRole("note").count(), 0);
  assert.equal((await page.locator("body").innerText()).includes("[Document"), false);
});
