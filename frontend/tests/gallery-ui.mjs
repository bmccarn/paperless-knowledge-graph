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
    for (const name of ["Buttons", "Fields and selection", "Cards and badges", "Table and pager", "Progress, skeleton and scroll area", "Dialogs, sheet, tooltip and toast"]) {
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
    const pager = page.getByRole("region", { name: "Table and pager", exact: true }).getByRole("navigation", { name: "page navigation", exact: true });
    await pager.getByRole("button", { name: "Next page", exact: true }).click();
    assert.match(await pager.textContent(), /11–20 of 23/);
    for (const kind of ["dialog", "sheet", "alert dialog"]) {
      const trigger = page.getByRole("button", { name: `Open ${kind}`, exact: true });
      await trigger.click();
      const dialog = page.getByRole(kind === "alert dialog" ? "alertdialog" : "dialog");
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

test("graph view switcher: one Tab stop, arrow keys, aria-pressed, announcements and the phone sheet", async t => {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: "reduce" });
  t.after(() => context.close());
  const page = await context.newPage();
  await page.goto(`${base}/gallery`);
  const section = page.getByRole("region", { name: "Graph view switcher", exact: true });
  const [toolbar, noFocus] = [section.getByRole("toolbar", { name: "View" }).first(), section.getByRole("toolbar", { name: "View" }).nth(1)];
  await toolbar.waitFor();
  const pressed = async bar => bar.locator("[aria-pressed=true]").textContent();
  const focused = () => page.evaluate(() => document.activeElement?.textContent);
  assert.deepEqual(await toolbar.getByRole("button").allTextContents(), ["Focus", "Explore", "Timeline", "Table", "Paths"]);
  assert.equal(await toolbar.getByRole("button", { name: "Focus" }).getAttribute("title"), "Focus, key 1");
  for (const button of await toolbar.getByRole("button").all()) assert.ok(["true", "false"].includes(await button.getAttribute("aria-pressed")));
  // One Tab stop: Tab enters on the pressed button and the next Tab leaves the toolbar.
  await toolbar.getByRole("button", { name: "Focus" }).focus();
  await page.keyboard.press("Shift+Tab");
  assert.equal(await toolbar.evaluate(bar => bar.contains(document.activeElement)), false);
  await page.keyboard.press("Tab");
  assert.equal(await focused(), "Focus");
  await page.keyboard.press("Tab");
  assert.equal(await toolbar.evaluate(bar => bar.contains(document.activeElement)), false);
  await page.keyboard.press("Shift+Tab");
  assert.equal(await focused(), "Focus");
  // Left and Right move without switching; Home and End jump; Enter switches and announces.
  await page.keyboard.press("ArrowRight");
  assert.equal(await focused(), "Explore");
  assert.equal(await pressed(toolbar), "Focus");
  await page.keyboard.press("End");
  assert.equal(await focused(), "Paths");
  await page.keyboard.press("ArrowRight");
  assert.equal(await focused(), "Focus");
  await page.keyboard.press("ArrowLeft");
  assert.equal(await focused(), "Paths");
  await page.keyboard.press("Home");
  await page.keyboard.press("ArrowRight");
  await page.keyboard.press("Enter");
  assert.equal(await pressed(toolbar), "Explore");
  assert.equal(await focused(), "Explore", "keyboard focus stays on the switcher");
  await section.getByRole("status").filter({ hasText: "Explore of Policy HM-4471 (auto)" }).first().waitFor();
  await page.keyboard.press("Tab");
  await page.keyboard.press("Shift+Tab");
  assert.equal(await focused(), "Explore", "the Tab stop follows the pressed button");
  // Without a focus entity, the views that need one are unavailable and say why.
  assert.deepEqual(await noFocus.locator("[aria-disabled=true]").allTextContents(), ["Focus", "Explore", "Timeline", "Table"]);
  assert.equal(await noFocus.locator("[aria-disabled=true]").count(), 4);
  await noFocus.getByRole("button", { name: "Timeline" }).focus();
  await page.keyboard.press("Enter");
  await section.getByRole("status").filter({ hasText: "Pick an entity on the map or search first." }).waitFor();
  assert.equal(await pressed(noFocus), "Overview");

  // At 390 px the View button opens a sheet with 56 px rows.
  await page.setViewportSize({ width: 390, height: 844 });
  const trigger = section.getByRole("button", { name: "View: Explore" });
  assert.equal(Math.round((await trigger.boundingBox()).height), 44);
  await trigger.click();
  const sheet = page.getByRole("dialog", { name: "View", exact: true });
  await sheet.waitFor();
  const rows = sheet.locator("li > button");
  assert.equal(await rows.count(), 5);
  const heights = await rows.evaluateAll(buttons => buttons.map(button => button.getBoundingClientRect().height));
  assert.ok(heights.every(height => height >= 56), `rows: ${heights}`);
  assert.equal(heights[3], 56, "a one-line question gives a 56 px row");
  assert.equal(await rows.nth(1).getAttribute("aria-pressed"), "true");
  await rows.nth(2).click();
  await sheet.waitFor({ state: "detached" });
  await section.getByRole("button", { name: "View: Timeline" }).waitFor();
  assert.equal(await pressed(toolbar), "Timeline");
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
});
