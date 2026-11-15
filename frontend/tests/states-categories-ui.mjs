/** Check the state and category components on their unlisted gallery route in both themes. */
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { test, before, after } from "node:test";
import { chromium } from "playwright";

const require = createRequire(import.meta.url);
const base = process.env.UI_TEST_BASE_URL || "http://127.0.0.1:3006";
const artifacts = process.env.UI_TEST_ARTIFACTS;
assert.ok(["127.0.0.1", "localhost"].includes(new URL(base).hostname));
const statuses = {
  verified: ["Verified", "badge-check"], documented: ["Documented", "clock"], partial: ["Partial", "contrast"],
  withheld: ["Withheld", "lock"], not_checked: ["Not checked", "circle-dashed"], retry: ["Retry needed", "refresh-cw"],
  failed: ["Failed", "triangle-alert"], supported: ["Supported", "badge-check"], unsupported: ["Unsupported", "lock"],
  conflicting: ["Conflicting", "triangle-alert"], left_out: ["Left out", "circle-minus"],
};
const kinds = {
  "Person": "people", "Organization": "org", "Location": "place", "Address": "place", "Financial item": "money",
  "Contract": "agree", "Insurance policy": "agree", "Product": "thing", "System": "thing", "Condition": "health",
  "Medical result": "health", "Event": "time", "Date event": "time", "Document": "docs", "Document ref": "docs", "Vehicle": "unknown",
};
const types = ["Invoice", "Tax", "Home", "Insurance", "Military", "Legal", "Personal", "Work", "Medical", "Vehicle registration"];

let browser;
before(async () => {
  browser = await chromium.launch({ headless: true });
  if (artifacts) await mkdir(artifacts, { recursive: true });
});
after(async () => { await browser?.close(); });

for (const theme of ["dark", "light"]) test(`${theme}: every state, level, kind and type has its word in the accessibility tree`, async t => {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  t.after(() => context.close());
  await context.addInitScript(theme => localStorage.setItem("theme", theme), theme);
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto(`${base}/gallery`);
  await page.getByRole("heading", { name: "Component gallery", exact: true }).waitFor();
  assert.ok(await page.evaluate(theme => document.documentElement.classList.contains(theme), theme));
  const tree = await page.locator("main").ariaSnapshot();

  for (const [status, [word, icon]] of Object.entries(statuses)) {
    // The verdict lines, claims and claim table sections repeat states inside larger components.
    const labels = page.locator(`section:not([aria-labelledby="verdict-lines"]):not([aria-labelledby="claims-notes"]):not([aria-labelledby="claim-table-sources"]) [data-status="${status}"]`);
    assert.equal(await labels.count(), 2, `${status} renders in both sizes`);
    for (const label of await labels.all()) {
      assert.equal((await label.innerText()).trim(), word);
      assert.equal(await label.locator(`svg.lucide-${icon}[aria-hidden="true"]`).count(), 1, `${status} uses ${icon}`);
    }
    assert.ok(tree.includes(word), `${word} is missing from the accessibility tree`);
    // The word is colored by its status token, resolved in the current theme.
    const [color, token] = await labels.first().evaluate((element, status) => {
      const probe = document.createElement("span");
      probe.style.color = `var(--${{ documented: "qualified", not_checked: "withheld", retry: "partial", failed: "error",
        supported: "verified", unsupported: "withheld", conflicting: "error", left_out: "partial" }[status] || status})`;
      document.body.append(probe);
      const value = getComputedStyle(probe).color;
      probe.remove();
      return [getComputedStyle(element).color, value];
    }, status);
    assert.equal(color, token, `${status} color`);
  }

  for (const level of ["high", "medium", "low"]) {
    const meter = page.getByRole("region", { name: "Evidence levels", exact: true }).getByRole("img", { name: `Evidence ${level}`, exact: true });
    assert.equal(await meter.count(), 1);
    assert.equal(await meter.locator("[aria-hidden] > span.bg-fg-muted").count(), { high: 3, medium: 2, low: 1 }[level]);
  }
  assert.ok(!/\d+\s*%/.test(await page.getByRole("region", { name: "Evidence levels", exact: true }).innerText()), "no percentage appears");

  for (const [label, kind] of Object.entries(kinds)) {
    assert.equal(await page.locator(`[data-kind="${kind}"]`, { hasText: new RegExp(`^${label}$`) }).count(), 3, `${label} in shape, icon and faded rows`);
    assert.ok(tree.includes(label), `${label} is missing from the accessibility tree`);
  }
  // Outlined types keep the family color as a stroke over a --bg fill; faded nodes keep their shape and change only color.
  const address = await page.locator('[data-kind="place"][data-outlined]').first().locator("svg path").evaluate(path => {
    const style = getComputedStyle(path);
    return { fill: style.fill, stroke: style.stroke, bg: getComputedStyle(document.body).backgroundColor };
  });
  assert.equal(address.fill, address.bg);
  assert.notEqual(address.stroke, "none");
  const faded = await page.locator('[data-kind="people"]').nth(2).evaluate(element => ({
    opacity: getComputedStyle(element).opacity, fill: getComputedStyle(element.querySelector("path")).fill,
  }));
  assert.equal(faded.opacity, "1");
  const fadedToken = await page.evaluate(() => {
    const probe = document.createElement("span");
    probe.style.color = "var(--graph-faded)";
    document.body.append(probe);
    const value = getComputedStyle(probe).color;
    probe.remove();
    return value;
  });
  assert.equal(faded.fill, fadedToken);

  const typeTree = await page.getByRole("region", { name: "Document types", exact: true }).ariaSnapshot();
  for (const label of types) assert.ok(typeTree.includes(label), `${label} is missing from the accessibility tree`);

  await page.addScriptTag({ path: require.resolve("axe-core/axe.min.js") });
  const violations = await page.evaluate(async () => (await window.axe.run(document, {
    runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"] },
  })).violations.map(violation => `${violation.id}: ${violation.nodes.map(node => node.target.join(" ")).join(", ")}`));
  assert.deepEqual(violations, []);
  if (artifacts) await page.screenshot({ path: path.join(artifacts, `states-${theme}.png`), fullPage: true });
  assert.deepEqual(errors, []);
});
