import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { test, expect } from "playwright/test";
import { unexpectedViolations, visualMode } from "./visual-policy.mjs";

const require = createRequire(import.meta.url);
const exceptions = JSON.parse(await readFile(new URL("./accessibility-exceptions.json", import.meta.url)));
const mode = visualMode();
const fixedTime = new Date("2026-09-04T12:00:00Z");
const mainStates = [
  { name: "ask", route: "/query", ready: page => page.getByPlaceholder("Ask a question...") },
  { name: "status", route: "/", ready: page => page.getByRole("heading", { name: "Dashboard", exact: true }) },
  { name: "documents", route: "/documents", ready: page => page.getByText("303 indexed documents", { exact: true }) },
  { name: "document", route: "/documents/101", ready: page => page.getByRole("heading", { name: "January premium statement", exact: true }) },
  { name: "entities", route: "/entities/review", ready: page => page.getByText("Cedar Lab 1", { exact: true }) },
  { name: "graph", route: "/graph", ready: page => page.getByText("4 nodes", { exact: false }).last() },
  { name: "hubs", route: "/hubs", ready: page => page.getByText("303 matching indexed documents", { exact: true }) },
  { name: "logs", route: "/debug", ready: page => page.getByText("Connected", { exact: true }) },
  { name: "not-found", route: "/visual-missing-page", ready: page => page.getByText("This page could not be found.", { exact: true }) },
];
const extraStates = [
  { ...mainStates[0], name: "ask-history", state: "history-sheet", phoneOnly: true, prepare: async page => {
    await page.getByRole("button", { name: "Open conversations", exact: true }).click();
    await page.getByRole("dialog").waitFor();
  } },
  { ...mainStates[4], name: "entities-dialog", state: "merge-dialog", prepare: async page => {
    await page.getByRole("button", { name: "Merge right into left", exact: true }).first().click();
    await page.getByRole("dialog").waitFor();
  } },
  { ...mainStates[2], name: "documents-expanded", state: "expanded", prepare: async page => {
    await page.getByRole("row").filter({ has: page.getByRole("link", { name: "January premium statement", exact: true }) }).locator("td").nth(2).click({ position: { x: 2, y: 2 } });
    await page.getByText("paperless_id", { exact: true }).waitFor();
  } },
  { ...mainStates[0], name: "ask-verified", state: "verified", prepare: async page => {
    await page.getByPlaceholder("Ask a question...").fill("What premium does the January statement list?");
    await page.getByPlaceholder("Ask a question...").press("Enter");
    await page.getByText("Claim ledger (1)", { exact: true }).waitFor();
  } },
];

test.beforeEach(async ({ page, baseURL, request }, info) => {
  assert.equal(info.config.updateSnapshots, mode === "update" ? "all" : "none", "Use the manual CI job to update baselines");
  assert.ok(["127.0.0.1", "localhost"].includes(new URL(baseURL).hostname));
  assert.equal((await (await request.get("/api/_fixture")).json()).fixture, "paperless-accuracy-ui-v1");
  assert.ok((await request.post("/api/_fixture/reset")).ok());
  await page.clock.install({ time: fixedTime });
  await page.clock.setFixedTime(fixedTime);
  await page.addInitScript(() => {
    // The physics renderer also uses randomness; give every capture the same seed.
    let seed = 112;
    Math.random = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296);
    localStorage.setItem("theme", "system");
  });
});

// #113 adds the gallery here once its route exists.
for (const state of [...mainStates, ...extraStates]) test(`${state.name} ${state.state || "main"}`, async ({ page }, info) => {
  test.skip(state.phoneOnly && !info.project.name.startsWith("phone"), "The conversation sheet is phone-only");
  await page.goto(state.route);
  await state.ready(page).waitFor();
  await page.addStyleTag({ content: "*, *::before, *::after { animation: none !important; transition: none !important; scroll-behavior: auto !important; caret-color: transparent !important; }" });
  await state.prepare?.(page);
  await page.mouse.move(0, 0);
  if (state.name === "graph") await page.locator("canvas").first().waitFor({ state: "visible" });
  await page.evaluate(() => document.fonts.ready);
  // Finish count-up and graph cooldown on the fixed clock, then freeze all timers.
  await page.clock.runFor(2500);
  await page.clock.pauseAt(new Date(fixedTime.getTime() + 10000));
  let capture;
  await expect(async () => {
    const previous = capture;
    capture = await page.screenshot({ animations: "disabled", caret: "hide" });
    expect(previous && capture.equals(previous), "Consecutive captures must match").toBe(true);
  }).toPass({ timeout: 5000 });
  await mkdir(info.outputDir, { recursive: true });
  const capturePath = info.outputPath("capture.png");
  await writeFile(capturePath, capture);
  await info.attach("capture", { path: capturePath, contentType: "image/png" });
  if (!state.state && mode !== "capture") await expect.soft(capture).toMatchSnapshot(`${state.name}.png`, { maxDiffPixels: 0, threshold: 0 });

  await page.clock.resume();
  await page.addScriptTag({ path: require.resolve("axe-core/axe.min.js") });
  const accessibility = await page.evaluate(async () => window.axe.run(document, {
    runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"] },
  }));
  const smallText = await page.locator("body *").evaluateAll(elements => elements.filter(element => {
    const style = getComputedStyle(element);
    return parseFloat(style.fontSize) < 12 && style.visibility !== "hidden" && element.getClientRects().length
      && [...element.childNodes].some(node => node.nodeType === Node.TEXT_NODE && node.textContent.trim());
  }).map(element => ({ target: [element.tagName.toLowerCase() + (element.className ? "." + String(element.className).trim().split(/\s+/).join(".") : "")], html: element.outerHTML })));
  const violations = [...accessibility.violations, ...(smallText.length ? [{ id: "font-size", nodes: smallText }] : [])];
  const report = JSON.stringify({ route: state.route, state: state.state || "main", violations, incomplete: accessibility.incomplete }, null, 2);
  const reportPath = info.outputPath("accessibility.json");
  await writeFile(reportPath, report);
  await info.attach("accessibility", { path: reportPath, contentType: "application/json" });
  expect.soft(unexpectedViolations(violations, exceptions, state.route, state.state || "main"), "New accessibility violations").toEqual([]);
});
