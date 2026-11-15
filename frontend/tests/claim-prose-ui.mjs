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
  // A long quote shows three lines (14/21 type) until its note is selected.
  const quoteHeight = () => answer.getByRole("note", { name: "Claim 3 source" }).locator("q").evaluate(q => q.getBoundingClientRect().height);
  assert.equal(await quoteHeight(), 63);
  const url = page.url();
  await answer.getByRole("button", { name: "Claim 3", exact: true }).click();
  assert.equal(page.url(), url);
  const note = answer.getByRole("note", { name: "Claim 3 source" });
  assert.equal(await note.getAttribute("aria-current"), "true");
  assert.equal(await answer.getByRole("note", { name: "Claim 2 source" }).getAttribute("aria-current"), null);
  assert.ok(await quoteHeight() > 63, "the selected note shows the whole quote");
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
  const note = answer.getByRole("note", { name: "Claim 2 source" });
  assert.ok(await note.isVisible());
  assert.ok(await noHorizontalScroll(page), "no horizontal scroll");
  // Clicking the selected claim's number again scrolls back to its note.
  const inView = () => note.evaluate(element => { const r = element.getBoundingClientRect(); return r.top >= 0 && r.bottom <= innerHeight; });
  await page.setViewportSize({ width: 390, height: 600 });
  await note.evaluate(element => { element.closest(".overflow-y-auto").scrollTop = 1e6; });
  await page.waitForFunction(element => element.getBoundingClientRect().top < 0, await note.elementHandle(), { timeout: 3000 });
  // A DOM click: Playwright's click would scroll the button, and so the note, into view itself.
  await answer.getByRole("button", { name: "Claim 2", exact: true }).evaluate(button => button.click());
  await page.waitForFunction(element => element.getBoundingClientRect().top >= 0, await note.elementHandle(), { timeout: 3000 });
  assert.ok(await inView());
  if (artifacts) await page.screenshot({ path: `${artifacts}/phone-390.png`, fullPage: true });
});

test("a Not checked answer shows no underlines and no notes", async t => {
  const { answer } = await ask(t, 1440, QUESTION, "Fast");
  assert.equal(await answer.getByTestId("verdict").locator("[data-status]").innerText(), "Not checked");
  assert.ok((await answer.innerText()).includes("The January statement lists a premium of $25."));
  assert.equal(await answer.locator("[data-claim]").count(), 0);
  assert.equal(await answer.getByRole("note").count(), 0);
  assert.equal(await answer.getByRole("button", { name: /^Claim table/ }).count(), 0, "unchecked claims have no results to show");
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

const openClaimTable = async answer => {
  await answer.getByRole("button", { name: /^Claim table/ }).click();
  const table = answer.getByRole("table", { name: "Claims" });
  await table.waitFor();
  return table;
};

test("selecting a claim table row marks the same claim in the prose and its note, and back", async t => {
  const { answer } = await ask(t, 1440);
  const table = await openClaimTable(answer);
  const row = n => table.getByRole("row").filter({ has: answer.page().getByRole("button", { name: `Select claim ${n}`, exact: true }) });
  await table.getByRole("button", { name: "Select claim 2", exact: true }).click();
  assert.equal(await row(2).getAttribute("aria-current"), "true");
  for (const piece of await answer.locator('[data-claim="2"]').all()) assert.equal(await piece.getAttribute("aria-current"), "true");
  assert.equal(await answer.getByRole("note", { name: "Claim 2 source" }).getAttribute("aria-current"), "true");
  // Selecting in the prose moves the table's selection too.
  await answer.locator('[data-claim="3"]').last().click();
  assert.equal(await row(3).getAttribute("aria-current"), "true");
  assert.equal(await row(2).getAttribute("aria-current"), null);
  assert.equal(await row(3).locator("q").innerText(), (await answer.getByRole("note", { name: "Claim 3 source" }).locator("q").innerText()));
  assert.ok(await noHorizontalScroll(answer.page()), "no horizontal scroll");
});

test("each source row shows its serif title, type, the date when known, the claims it supports and both links", async t => {
  const { answer } = await ask(t, 1440);
  const sources = answer.getByRole("region", { name: "Sources" });
  const rows = sources.getByRole("listitem");
  assert.equal(await rows.count(), 2);
  const first = rows.nth(0), second = rows.nth(1);
  const title = first.getByRole("button", { name: "January premium statement", exact: true });
  assert.match(await title.evaluate(element => getComputedStyle(element).fontFamily), /Source Serif 4/);
  assert.ok((await first.innerText()).includes("Insurance"));
  assert.equal(await first.locator("[data-type] > span[aria-hidden]").count(), 1, "type dot");
  assert.ok((await first.innerText()).includes("31 Jan 2026"));
  assert.ok((await first.innerText()).includes("Supports claims 1 and 2"));
  assert.ok((await second.innerText()).includes("Supports claim 3"));
  // No date in source_dates: no date text and no placeholder.
  assert.doesNotMatch(await second.innerText(), /\d{4}|—\s*$|Unknown date/i);
  for (const row of [first, second]) {
    assert.equal(await row.getByRole("link", { name: /^Open\b(?! in Paperless)/ }).count(), 1);
    assert.equal(await row.getByRole("link", { name: /^Open in Paperless/ }).count(), 1);
  }
  assert.equal(await first.getByRole("link", { name: /^Open\b(?! in Paperless)/ }).getAttribute("href"), "/documents/101");
  await title.click();
  const sheet = answer.page().getByRole("dialog");
  assert.ok((await sheet.innerText()).includes("January statement: the premium is $25."));
  assert.match(await sheet.getByRole("heading", { name: "January premium statement" }).evaluate(element => getComputedStyle(element).fontFamily), /Source Serif 4/);
});

test("the Partial answer's claim table lists each left-out unit", async t => {
  const { answer } = await ask(t, 1440, "Show partial recorded history", "Timeline");
  const table = await openClaimTable(answer);
  const left = table.getByRole("row").filter({ hasText: "Left out" });
  assert.equal(await left.count(), 1);
  assert.ok((await left.innerText()).includes("The January statement lists a late fee of $5."));
  assert.equal(await left.locator("svg.lucide-circle-minus").count(), 1);
  assert.equal(await left.getByRole("button").count(), 0, "a left-out claim is not in the prose, so it cannot be selected");
});

test("at 390 px the claim table stacks without horizontal scroll, and selecting a row scrolls to its note", async t => {
  const { page, answer } = await ask(t, 390);
  const table = await openClaimTable(answer);
  assert.ok(await noHorizontalScroll(page), "no horizontal scroll");
  const note = answer.getByRole("note", { name: "Claim 2 source" });
  // A DOM click: Playwright's click would scroll the page itself.
  await table.getByRole("button", { name: "Select claim 2", exact: true }).scrollIntoViewIfNeeded();
  await table.getByRole("button", { name: "Select claim 2", exact: true }).evaluate(button => button.click());
  await page.waitForFunction(element => { const r = element.getBoundingClientRect(); return r.height > 0 && r.top >= 0 && r.bottom <= innerHeight; },
    await note.elementHandle(), { timeout: 3000 });
  if (artifacts) await page.screenshot({ path: `${artifacts}/claim-table-390.png`, fullPage: true });
});

// Long passages, a claim with six references and a list of four claims, as real answers have.
const REAL = "How did my auto insurance change at renewal? real-answer";
const twoFrames = page => page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
/** The window and every ancestor of the answer column; only the column itself may scroll. */
const frameShift = page => page.evaluate(() => {
  const shifted = [];
  for (let element = document.querySelector("article[aria-label=Answer]").closest(".overflow-y-auto").parentElement; element; element = element.parentElement)
    if (element.scrollTop) shifted.push(`${element.tagName} ${element.scrollTop}`);
  return { scrollY, shifted };
});

for (const width of [1440, 390]) test(`at ${width} px only the answer column scrolls, when the answer arrives and after a claim number click`, async t => {
  const { page, answer } = await ask(t, width, REAL);
  await twoFrames(page);
  assert.deepEqual(await frameShift(page), { scrollY: 0, shifted: [] });
  // A DOM click: Playwright's click scrolls the button into view itself.
  await answer.getByRole("button", { name: "Claim 4", exact: true }).evaluate(button => button.click());
  await twoFrames(page);
  assert.deepEqual(await frameShift(page), { scrollY: 0, shifted: [] });
  if (artifacts) await page.screenshot({ path: `${artifacts}/real-answer-${width}.png` });
});

/** Whether the emphasized value is inside the quote's visible (clamped) box. */
const showsValue = (quote, value) => quote.evaluate((q, value) => {
  const box = q.getBoundingClientRect();
  const match = [...q.querySelectorAll("strong")].find(strong => strong.textContent === value)?.getBoundingClientRect();
  return Boolean(match && match.height && match.top >= box.top - 0.5 && match.bottom <= box.bottom + 0.5);
}, value);

test("a margin quote and its claim table quote show the amount the claim states, past the passage header", async t => {
  const { answer } = await ask(t, 1440, REAL);
  for (const [n, amount] of [[1, "$612.00"], [2, "$655.00"], [3, "$500"]]) {
    const quote = answer.getByRole("note", { name: `Claim ${n} source` }).locator("q").first();
    assert.ok(await showsValue(quote, amount), `note ${n} shows ${amount}`);
    assert.ok((await quote.innerText()).startsWith("… "), `note ${n} marks the cut start`);
  }
  const table = await openClaimTable(answer);
  const row = table.getByRole("row").filter({ has: answer.page().getByRole("button", { name: "Select claim 3", exact: true }) });
  assert.ok(await showsValue(row.locator("q"), "$500"), "the table row shows $500");
});

test("a claim with six references shows the two that state its amount, and a control opens the other four", async t => {
  const { page, answer } = await ask(t, 1440, REAL);
  const note = answer.getByRole("note", { name: "Claim 1 source" });
  assert.equal(await note.locator("q").count(), 2);
  for (const quote of await note.locator("q").all()) assert.ok((await quote.innerText()).includes("$612.00"));
  const more = note.getByRole("button", { name: "and 4 more", exact: true });
  assert.equal(await more.getAttribute("aria-expanded"), "false");
  await more.click();
  assert.equal(await note.locator("q").count(), 6);
  assert.equal(await note.getByRole("button", { name: "Show fewer", exact: true }).getAttribute("aria-expanded"), "true");
  const table = await openClaimTable(answer);
  const row = table.getByRole("row").filter({ has: page.getByRole("button", { name: "Select claim 1", exact: true }) });
  assert.equal(await row.locator("q").count(), 6, "the claim table keeps every reference");
});

test("no block in the old style remains under the answer", async t => {
  const { page, answer } = await ask(t, 1440, REAL);
  await page.getByText("Suggested follow-ups", { exact: true }).waitFor();
  assert.equal(/Entities/i.test(await answer.innerText()), false, "no entities block");
  const allCaps = await page.locator("main").evaluate(main => [...main.querySelectorAll("*")]
    .filter(element => getComputedStyle(element).textTransform === "uppercase" && element.textContent.trim())
    .map(element => element.textContent.trim()));
  assert.deepEqual(allCaps, []);
  assert.equal(await page.getByRole("button", { name: "When is the next premium due?", exact: true }).count(), 1);
});

for (const width of [1440, 390]) test(`a claim number stays on the line of the claim's last word at ${width} px`, async t => {
  const { answer } = await ask(t, width);
  // Narrow the prose one pixel at a time so each claim's last word reaches the right edge at some width.
  const result = await answer.locator(".prose").first().evaluate(prose => {
    const lastCharRect = claim => {
      const walker = document.createTreeWalker(claim, NodeFilter.SHOW_TEXT);
      let last = null;
      for (let node = walker.nextNode(); node; node = walker.nextNode()) if (node.data.trim()) last = node;
      const end = last.data.trimEnd().length;
      const range = document.createRange();
      range.setStart(last, end - 1);
      range.setEnd(last, end);
      return range.getBoundingClientRect();
    };
    const start = prose.getBoundingClientRect().width;
    const split = [];
    let checks = 0;
    for (let width = start; width > start - 160 && width > 200; width--) {
      prose.style.width = `${width}px`;
      for (const number of prose.querySelectorAll("button.cite")) {
        const claim = number.previousElementSibling;
        const word = lastCharRect(claim), mark = number.getBoundingClientRect();
        checks++;
        if (mark.top >= word.bottom || mark.bottom <= word.top) split.push(`${number.getAttribute("aria-label")} at ${width}px`);
      }
    }
    prose.style.width = "";
    return { checks, split };
  });
  assert.ok(result.checks > 100);
  assert.deepEqual(result.split, []);
});
