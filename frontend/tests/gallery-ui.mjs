/** Shared controls use the same behavior and geometry in both themes. */
import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { chromium } from "playwright";

const base = process.env.UI_TEST_BASE_URL || "http://127.0.0.1:3006";
assert.ok(["127.0.0.1", "localhost"].includes(new URL(base).hostname));
let browser;
before(async () => { browser = await chromium.launch({ headless: true }); });
after(async () => { await browser?.close(); });

for (const theme of ["dark", "light"]) for (const width of [1440, 390]) {
  test(`gallery controls: ${theme}, ${width}px`, async t => {
    const context = await browser.newContext({ viewport: { width, height: width === 390 ? 844 : 900 }, reducedMotion: "reduce" });
    t.after(() => context.close());
    await context.addInitScript(theme => localStorage.setItem("theme", theme), theme);
    const page = await context.newPage();
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    await page.goto(`${base}/gallery`);
    await page.getByRole("heading", { name: "Component gallery", exact: true }).waitFor();
    await page.evaluate(() => document.fonts.ready);
    await page.addStyleTag({ content: "*, *::before, *::after { animation: none !important; transition: none !important; }" });
    for (const name of ["Buttons", "Fields and selection", "Cards and badges", "Table and pager", "Progress, skeleton and scroll area", "Dialog, sheet and tooltip"]) {
      assert.equal(await page.getByRole("heading", { name, exact: true }).count(), 1);
    }
    assert.ok(await page.locator("[data-slot=button]").evaluateAll(buttons => buttons.every(button => button.textContent.trim() || button.getAttribute("aria-label"))));
    const save = page.getByTestId("loading-button");
    const sizedButtons = page.getByRole("button", { name: /^Size / });
    const beforeSizes = await sizedButtons.evaluateAll(buttons => buttons.map(button => button.getBoundingClientRect().width));
    const before = await save.boundingBox();
    await page.getByRole("button", { name: "Toggle loading", exact: true }).click();
    assert.equal(await save.getAttribute("aria-busy"), "true");
    assert.ok(await save.isDisabled());
    assert.deepEqual(await sizedButtons.evaluateAll(buttons => buttons.map(button => button.getBoundingClientRect().width)), beforeSizes);
    const during = await save.boundingBox();
    assert.equal(before.width, during.width, "loading must preserve label width");
    assert.equal(before.height, during.height);
    assert.equal(await save.textContent(), "Save document");
    await page.getByRole("button", { name: "Toggle loading", exact: true }).click();
    assert.ok(await save.isEnabled());
    assert.equal(await page.getByLabel("Required title", { exact: true }).getAttribute("aria-invalid"), "true");
    assert.equal(await page.getByLabel("Required note", { exact: true }).getAttribute("aria-describedby"), "note-error");
    await page.getByRole("checkbox", { name: "Unchecked", exact: true }).check();
    assert.ok(await page.getByRole("checkbox", { name: "Unchecked", exact: true }).isChecked());
    assert.equal(await page.getByRole("checkbox", { name: "Mixed", exact: true }).getAttribute("aria-checked"), "mixed");
    await page.getByRole("button", { name: "Title", exact: true }).click();
    assert.equal(await page.getByRole("columnheader", { name: "Title" }).getAttribute("aria-sort"), "descending");
    const row = page.getByRole("row", { name: "Service agreement 2", exact: true });
    await row.focus();
    await row.press("Enter");
    assert.equal(await row.getAttribute("aria-selected"), "true");
    const rowHeight = (await row.boundingBox()).height;
    assert.equal((await page.getByRole("row", { name: "Loading document", exact: true }).boundingBox()).height, rowHeight);
    const pager = page.getByRole("navigation", { name: "page navigation", exact: true });
    await pager.getByRole("button", { name: "Next page", exact: true }).click();
    assert.match(await pager.textContent(), /11–20 of 23/);
    for (const kind of ["dialog", "sheet"]) {
      const trigger = page.getByRole("button", { name: `Open ${kind}`, exact: true });
      await trigger.click();
      const dialog = page.getByRole("dialog");
      await dialog.waitFor();
      if (kind === "sheet") {
        const box = await dialog.boundingBox();
        assert.ok(box.width <= 460 && box.x >= 0 && box.x + box.width <= width);
        if (width === 390) assert.equal(Math.round(box.y + box.height), 844);
        else assert.equal(Math.round(box.x + box.width), 1440);
      }
      await page.keyboard.press("Escape");
      await dialog.waitFor({ state: "hidden" });
      assert.ok(await trigger.evaluate(element => element === document.activeElement), "overlay restores focus");
    }
    await page.getByRole("button", { name: "Add to archive", exact: true }).scrollIntoViewIfNeeded();
    await page.getByRole("button", { name: "Add to archive", exact: true }).hover();
    await page.getByRole("tooltip").waitFor();
    assert.equal(await page.getByRole("tooltip").textContent(), "Add a document for review");
    const input = page.getByLabel("Document title", { exact: true });
    if (width === 390) {
      assert.equal(await input.evaluate(element => getComputedStyle(element).fontSize), "16px");
      assert.equal(await page.getByRole("checkbox", { name: "Unchecked", exact: true }).evaluate(element => getComputedStyle(element, "::after").height), "44px");
    } else {
      for (const size of [28, 32, 36, 44]) assert.equal((await page.getByRole("button", { name: `Size ${size}`, exact: true }).boundingBox()).height, size);
    }
    assert.ok(await page.locator("[data-gallery-scroll]").evaluate(element => element.scrollWidth <= element.clientWidth), "no horizontal scrolling");
    assert.deepEqual(errors, []);
  });
}
