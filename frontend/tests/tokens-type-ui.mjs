/** Check the direction C tokens, type roles, focus ring and motion in the built app. */
import assert from "node:assert/strict";
import { test, before, after } from "node:test";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

const base = process.env.UI_TEST_BASE_URL || "http://127.0.0.1:3006";
const artifacts = process.env.UI_TEST_ARTIFACTS;
assert.ok(["127.0.0.1", "localhost"].includes(new URL(base).hostname));
const routes = ["/", "/query", "/documents", "/documents/101", "/entities/review", "/graph", "/hubs", "/debug"];
const surfaces = ["bg", "surface", "raised", "overlay", "sunken"];
const statuses = ["verified", "qualified", "partial", "withheld", "error"];
const nodes = ["people", "org", "place", "money", "agree", "thing", "health", "time", "docs"].map(kind => `cat-${kind}`);
// The rules of the redesign issue: text 4.5:1 on every surface and its own tint; rules and node colors 3:1.
const pairs = [
  ...surfaces.flatMap(surface => ["fg", "fg-muted", "fg-subtle", "accent", ...statuses].map(text => [text, surface, 4.5])),
  ...statuses.map(status => [status, `${status}-bg`, 4.5]),
  ["fg", "accent-bg", 4.5], ["on-accent", "accent-solid", 4.5], ["on-mark", "mark", 4.5],
  ...["bg", "surface"].flatMap(surface =>
    ["border-strong", "ring", "mark-line", ...statuses.map(status => `${status}-line`)].map(line => [line, surface, 3])),
  ...surfaces.flatMap(surface => nodes.map(node => [node, surface, 3])),
];
let browser;
before(async () => {
  browser = await chromium.launch({ headless: true });
  if (artifacts) await mkdir(artifacts, { recursive: true });
});
after(async () => { await browser?.close(); });

async function open(t, { theme = "dark", width = 1440, height = 900, reducedMotion = "no-preference" } = {}) {
  const context = await browser.newContext({ viewport: { width, height }, reducedMotion });
  t.after(() => context.close());
  await context.addInitScript(theme => localStorage.setItem("theme", theme), theme);
  const page = await context.newPage();
  page.setDefaultTimeout(10000);
  const errors = [];
  const requests = [];
  page.on("pageerror", error => errors.push(error.message));
  page.on("request", request => requests.push(request));
  t.after(() => assert.deepEqual(errors, []));
  return { page, requests };
}

async function visit(page, route) {
  await page.goto(`${base}${route}`);
  await page.locator("main").waitFor();
  await page.waitForLoadState("networkidle").catch(() => {});
  await page.evaluate(() => document.fonts.ready);
}

for (const theme of ["dark", "light"]) test(`${theme} token pairs reach their contrast minimum`, async t => {
  const { page } = await open(t, { theme });
  await visit(page, "/");
  assert.ok(await page.evaluate(theme => document.documentElement.classList.contains(theme), theme));
  const results = await page.evaluate(pairs => {
    const canvas = document.createElement("canvas");
    canvas.width = canvas.height = 1;
    const context = canvas.getContext("2d", { willReadFrequently: true });
    // Let the browser convert the resolved color to sRGB.
    const srgb = color => {
      context.clearRect(0, 0, 1, 1);
      context.fillStyle = "#000";
      context.fillStyle = color;
      context.fillRect(0, 0, 1, 1);
      return [...context.getImageData(0, 0, 1, 1).data].slice(0, 3);
    };
    const luminance = rgb => {
      const [r, g, b] = rgb.map(value => value / 255).map(value =>
        value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4);
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    };
    const white = srgb("oklch(1 0 0)");
    return { white, pairs: pairs.map(([front, back, minimum]) => {
      const element = document.createElement("span");
      element.textContent = "Aa";
      element.style.color = `var(--${front})`;
      element.style.backgroundColor = `var(--${back})`;
      document.body.append(element);
      const style = getComputedStyle(element);
      const [a, b] = [luminance(srgb(style.color)), luminance(srgb(style.backgroundColor))];
      element.remove();
      return { front, back, minimum, transparent: style.color === "rgba(0, 0, 0, 0)" || style.backgroundColor === "rgba(0, 0, 0, 0)",
        ratio: (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05) };
    }) };
  }, pairs);
  assert.deepEqual(results.white, [255, 255, 255], "the canvas did not resolve an oklch color");
  const failures = results.pairs.filter(pair => pair.transparent || pair.ratio < pair.minimum)
    .map(pair => `--${pair.front} on --${pair.back}: ${pair.ratio.toFixed(2)}:1, needs ${pair.minimum}:1`);
  assert.deepEqual(failures, []);
});

test("light raised surfaces differ from cards", async t => {
  const { page } = await open(t, { theme: "light" });
  await visit(page, "/");
  const tokens = await page.evaluate(() => {
    const style = getComputedStyle(document.documentElement);
    return [style.getPropertyValue("--raised").trim(), style.getPropertyValue("--surface").trim()];
  });
  assert.notEqual(tokens[0], tokens[1]);
});

test("--sunken differs between the themes", async t => {
  const values = [];
  for (const theme of ["dark", "light"]) {
    const { page } = await open(t, { theme });
    await visit(page, "/");
    values.push(await page.evaluate(() => getComputedStyle(document.documentElement).getPropertyValue("--sunken").trim()));
  }
  assert.ok(values.every(Boolean), "--sunken is not defined");
  assert.notEqual(values[0], values[1]);
});

test("type roles use the three faces, served by the app itself", async t => {
  const { page, requests } = await open(t);
  await visit(page, "/");
  const faces = await page.evaluate(async () => {
    const roles = ["page-title", "question", "prose", "quote", "doc-title"];
    for (const role of roles) {
      const element = document.createElement("p");
      element.className = role;
      element.dataset.role = role;
      element.textContent = "Probe text";
      document.body.append(element);
    }
    await document.fonts.ready;
    const variable = name => getComputedStyle(document.body).getPropertyValue(name).trim();
    const first = family => family.split(",")[0].trim().replace(/["']/g, "");
    const family = element => first(getComputedStyle(element).fontFamily);
    const loaded = name => [...document.fonts].some(font => font.family.replace(/["']/g, "") === name && font.status === "loaded");
    const result = {
      serif: first(variable("--font-geist-serif-regular")),
      text: first(variable("--font-source-serif")),
      sans: first(variable("--font-geist-sans")),
      roles: Object.fromEntries(roles.map(role => [role, family(document.querySelector(`[data-role="${role}"]`))])),
      heading: family(document.querySelector("h1")),
      navigation: family(document.querySelector("nav a, a[href='/query']")),
    };
    result.loaded = [result.serif, result.text].map(loaded);
    return result;
  });
  assert.ok(faces.serif && faces.text && faces.sans, "a font variable is missing");
  assert.deepEqual(faces.roles, { "page-title": faces.serif, question: faces.serif, prose: faces.text, quote: faces.text, "doc-title": faces.text });
  assert.equal(faces.heading, faces.serif);
  assert.equal(faces.navigation, faces.sans);
  assert.deepEqual(faces.loaded, [true, true], "Geist Serif or Source Serif 4 did not load");
  const outside = requests.map(request => request.url()).filter(url => /^https?:/.test(url) && new URL(url).origin !== new URL(base).origin);
  assert.deepEqual(outside, []);
  assert.ok(requests.filter(request => request.resourceType() === "font").length >= 2);
});

test("components keep rounded corners on the new radius scale", async t => {
  const { page } = await open(t);
  await visit(page, "/query");
  const radius = await page.evaluate(() => {
    const scale = Object.fromEntries(["sm", "md", "lg", "xl"].map(size => {
      const element = document.createElement("div");
      element.className = `rounded-${size}`;
      document.body.append(element);
      return [size, getComputedStyle(element).borderTopLeftRadius];
    }));
    const of = selector => getComputedStyle(document.querySelector(selector)).borderTopLeftRadius;
    return { scale, button: of("button[data-slot='button'], button"), textarea: of("textarea") };
  });
  assert.deepEqual(radius.scale, { sm: "4px", md: "6px", lg: "10px", xl: "12px" });
  assert.notEqual(radius.button, "0px");
  assert.notEqual(radius.textarea, "0px");
});

test("Tab shows a solid 2 px outline with 2 px offset on every page", async t => {
  const { page } = await open(t);
  const failures = [];
  for (const route of routes) {
    await visit(page, route);
    for (let press = 0; press < 10; press++) {
      await page.keyboard.press("Tab");
      const focus = await page.evaluate(async () => {
        const element = document.activeElement;
        if (!element || element === document.body) return null;
        await Promise.all(element.getAnimations().map(animation => animation.finished));
        const style = getComputedStyle(element);
        return { name: element.outerHTML.slice(0, 80), outline: `${style.outlineStyle} ${style.outlineWidth} ${style.outlineOffset}` };
      });
      if (!focus) failures.push(`${route}: Tab ${press + 1} focused nothing`);
      else if (focus.outline !== "solid 2px 2px") failures.push(`${route}: ${focus.name} has ${focus.outline}`);
    }
  }
  assert.deepEqual(failures, []);
});

test("with reduced motion no route transition or count-up plays", async t => {
  const { page } = await open(t, { reducedMotion: "reduce" });
  for (const route of ["/", "/documents"]) {
    await page.goto(`${base}${route}`);
    await page.locator("h1").first().waitFor();
    const running = await page.evaluate(() => document.getAnimations()
      .filter(animation => Number(animation.effect?.getTiming().duration) > 1)
      .map(animation => animation.animationName || animation.transitionProperty || "animation"));
    assert.deepEqual(running, [], `${route} still animates`);
  }
});

test("no route transition plays on the page content without reduced motion", async t => {
  const { page } = await open(t);
  await page.goto(base);
  const heading = page.getByRole("heading", { name: "Dashboard", exact: true });
  await heading.waitFor();
  const animated = await heading.evaluate(element => document.getAnimations()
    .filter(animation => animation.effect?.target?.contains(element)).map(animation => animation.animationName));
  assert.deepEqual(animated, []);
});

if (artifacts) test("capture every route in both themes at desktop and phone width", async t => {
  for (const theme of ["dark", "light"]) for (const [width, height] of [[1440, 900], [390, 844]]) {
    const { page } = await open(t, { theme, width, height });
    for (const route of routes) {
      await visit(page, route);
      const name = route === "/" ? "dashboard" : route.slice(1).replaceAll("/", "-");
      await page.screenshot({ path: path.join(artifacts, `${name}-${theme}-${width}x${height}.png`) });
    }
  }
});
