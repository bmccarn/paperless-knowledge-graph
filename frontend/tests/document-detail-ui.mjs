/** Document inspection contracts against the synthetic fixture only. */
import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

const base = process.env.UI_TEST_BASE_URL;
assert.ok(base && ["127.0.0.1", "localhost"].includes(new URL(base).hostname));
let browser;
before(async () => { browser = await chromium.launch({ headless: true }); });
after(async () => { await browser?.close(); });

for (const theme of ["dark", "light"]) for (const width of [1440, 390]) {
  test(`document detail ${theme} ${width}`, async t => {
    const context = await browser.newContext({ viewport: { width, height: width === 390 ? 844 : 900 }, colorScheme: theme });
    t.after(() => context.close());
    await context.addInitScript(theme => localStorage.setItem("theme", theme), theme);
    const page = await context.newPage();
    assert.equal((await (await context.request.get(`${base}/api/_fixture`)).json()).fixture, "paperless-accuracy-ui-v1");
    assert.ok((await context.request.post(`${base}/api/_fixture/reset`)).ok());
    await page.goto(`${base}/documents/101`);
    await page.getByRole("heading", { name: "January premium statement", exact: true }).waitFor();
    const headings = await page.locator("main h2").allTextContents();
    assert.equal(headings[0], "Extracted from this document");
    assert.equal(headings[1], "Indexed chunks");
    const extracted = page.getByRole("region", { name: "Extracted from this document" });
    assert.equal(await extracted.getByRole("link", { name: "Alex Example", exact: true }).getAttribute("href"), "/graph?focus=person-1");
    assert.equal(await extracted.getByRole("link", { name: "Example Utility", exact: true }).getAttribute("href"), "/graph?focus=org-1");
    assert.match(await extracted.innerText(), /Issued by Example Utility/);
    for (const state of ["Supported", "Inferred", "Stated in 1, inferred in 1", "No support record"]) await extracted.getByText(state, { exact: true }).waitFor();
    const heading = await page.locator("h1").evaluate(element => ({ size: getComputedStyle(element).fontSize, height: getComputedStyle(element).lineHeight, family: getComputedStyle(element).fontFamily }));
    assert.equal(heading.size, "22px");
    assert.equal(heading.height, "28px");
    assert.match(heading.family, /Source Serif 4/);
    assert.equal(await page.locator("main a button, main button a").count(), 0);
    const toggle = page.getByRole("button", { name: "Original OCR text" });
    assert.equal(await toggle.getAttribute("aria-expanded"), "false");
    assert.equal(await page.getByRole("region", { name: "Original OCR text" }).count(), 0);
    if (process.env.UI_TEST_ARTIFACTS) {
      await mkdir(process.env.UI_TEST_ARTIFACTS, { recursive: true });
      await page.evaluate(() => document.fonts.ready);
      await page.screenshot({ path: path.join(process.env.UI_TEST_ARTIFACTS, `${width}-${theme}.png`) });
    }
    await toggle.click();
    const ocr = page.getByRole("region", { name: "Original OCR text" });
    assert.equal(await ocr.getAttribute("tabindex"), "0");
    await ocr.focus();
    assert.equal(await ocr.evaluate(element => element === document.activeElement), true);
    assert.match(await ocr.innerText(), /January statement: the premium is \$25/);
    assert.equal(await page.locator("main").evaluate(el => el.scrollWidth > el.clientWidth), false);
    await page.getByRole("textbox", { name: "Extraction review report" }).fill("Check the original premium.");
    await page.getByRole("button", { name: "Mark extraction wrong", exact: true }).click();
    const resolution = page.getByRole("combobox", { name: "Resolution", exact: true });
    await resolution.click();
    await page.keyboard.press("Escape");
    assert.ok(await resolution.evaluate(element => element === document.activeElement));
    await resolution.click();
    await page.getByRole("option", { name: "Checked original extraction; no correction needed" }).click();
    await page.getByRole("textbox", { name: "Review note for report 1" }).fill("The original premium matches the extracted value.");
    await page.getByRole("button", { name: "Resolve reviewed report", exact: true }).click();
    await page.getByText("Dismissed after review:", { exact: false }).waitFor();
    await page.goto(`${base}/documents/102`);
    await page.getByText("Correspondent ID: 7", { exact: true }).waitFor();
    await page.goto(`${base}/documents/999999`);
    await page.getByRole("heading", { name: "Document not found", exact: true }).waitFor();
    assert.equal(await page.locator("main").getByRole("link", { name: "Back to Documents", exact: true }).getAttribute("href"), "/documents");
  });
}
