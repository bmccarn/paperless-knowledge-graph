/** The capped neighborhood at /graph?focus=<id>, measured from canvas paint calls on synthetic data. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3002';
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-neighborhood-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
const errors = [];
const hubUrl = `${base}/graph?focus=nb-rowan`;
const families = ['Parties and roles', 'Money and accounts', 'Coverage and obligations', 'Health',
  'Service and government', 'Places', 'Time', 'Loose association', 'Other'];

// Each frame of the visible canvas. Labels and pill text are drawn with the context moved to their node's
// center in screen pixels, so the transform origin is the node's position whatever the label placement.
// Link strokes record where they end, their dash and width in screen pixels, and their color.
await context.addInitScript(() => {
  window.__frame = {};
  window.__strokes = [];
  window.__minFont = Infinity;
  const proto = CanvasRenderingContext2D.prototype;
  const ends = new WeakMap();
  const screen = (ctx, x, y) => {
    const point = new DOMPoint(x, y).matrixTransform(ctx.getTransform());
    return { x: point.x / devicePixelRatio, y: point.y / devicePixelRatio };
  };
  const clearRect = proto.clearRect;
  proto.clearRect = function (...args) {
    if (this.canvas.isConnected) { window.__frame = {}; window.__strokes = []; }
    return clearRect.apply(this, args);
  };
  for (const name of ['lineTo', 'quadraticCurveTo']) {
    const original = proto[name];
    proto[name] = function (...args) {
      ends.set(this, screen(this, args.at(-2), args.at(-1)));
      return original.apply(this, args);
    };
  }
  const stroke = proto.stroke;
  proto.stroke = function (...args) {
    if (this.canvas.isConnected && !args.length && ends.has(this)) {
      const scale = this.getTransform().a / devicePixelRatio;
      window.__strokes.push({ end: ends.get(this), dash: this.getLineDash().map(value => Math.round(value * scale * 100) / 100),
        width: this.lineWidth * scale, color: this.strokeStyle });
    }
    return stroke.apply(this, args);
  };
  const fillText = proto.fillText;
  proto.fillText = function (text, x, y, ...rest) {
    if (this.canvas.isConnected) {
      const font = Math.round(parseFloat(this.font.match(/([\d.]+)px/)[1]) * this.getTransform().a / devicePixelRatio * 100) / 100;
      window.__minFont = Math.min(window.__minFont, font);
      window.__frame[text] = { ...screen(this, 0, 0), font };
    }
    return fillText.call(this, text, x, y, ...rest);
  };
});

/** The frame with world positions from the camera transform of the visible canvas. */
const frameNow = page => page.evaluate(() => {
  const zoom = document.querySelector('canvas').__zoom;
  return Object.fromEntries(Object.entries(window.__frame).map(([text, at]) => [text,
    { ...at, worldX: Math.round((at.x - zoom.x) / zoom.k * 100) / 100, worldY: Math.round((at.y - zoom.y) / zoom.k * 100) / 100 }]));
});

async function open(page, url = hubUrl, ready = 'Rowan Vale: 105 links in 9 families. Large hub: the top 2 of each family are shown.') {
  await page.goto(url);
  await page.getByText(ready, { exact: true }).waitFor();
  await page.locator('canvas').first().waitFor({ state: 'visible' });
  // The first layout is fitted once; wait until the camera and the paint stay still.
  await page.waitForFunction(() => {
    const zoom = document.querySelector('canvas')?.__zoom;
    if (!zoom || zoom.k === 1 || !Object.keys(window.__frame).length) return false;
    const signature = JSON.stringify([zoom, Object.keys(window.__frame).length]);
    if (window.__still?.signature !== signature) window.__still = { signature, since: Date.now() };
    return Date.now() - window.__still.since > 300;
  });
  return frameNow(page);
}
const pillsOf = frame => Object.keys(frame).filter(text => text.startsWith('+')).sort();
const neighborsOf = frame => Object.keys(frame).filter(text => !text.startsWith('+') && text !== 'Rowan Vale').sort();
const world = frame => JSON.stringify(Object.entries(frame).map(([text, at]) => [text, at.worldX, at.worldY]).sort());
/** Strokes that end on a node's center. */
const strokesTo = (strokes, at) => strokes.filter(item => Math.hypot(item.end.x - at.x, item.end.y - at.y) < 1);
/** A CSS color as sRGB bytes, through a 1 x 1 canvas. */
const resolve = (page, color) => page.evaluate(color => {
  const probe = document.createElement('div');
  probe.style.color = color;
  document.body.append(probe);
  const context = Object.assign(document.createElement('canvas'), { width: 1, height: 1 }).getContext('2d');
  context.fillStyle = getComputedStyle(probe).color;
  probe.remove();
  context.fillRect(0, 0, 1, 1);
  return [...context.getImageData(0, 0, 1, 1).data].join(',');
}, color);

try {
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  const marker = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await marker.json()).fixture, 'paperless-accuracy-ui-v1');

  // Caps: 2 neighbors per family on a hub, one pill per family with hidden links, no documents.
  const first = await open(page);
  await page.screenshot({ path: path.join(artifacts, 'graph-neighborhood-hub.png') });
  const neighbors = neighborsOf(first);
  assert.equal(neighbors.length, 18, `2 neighbors in each of 9 families: ${neighbors}`);
  assert.ok(!neighbors.some(name => /^[A-Z][a-z]+ fixture \d+$/.test(name)), 'Only the top-ranked neighbors are drawn');
  assert.ok(!Object.keys(first).some(text => text.startsWith('Synthetic archive')), 'No Document node is drawn');
  const pills = pillsOf(first);
  assert.deepEqual(pills.map(text => text.replace(/^\+\d+ /, '')).sort(), [...families].sort());
  const hidden = pills.reduce((sum, text) => sum + Number(text.match(/^\+(\d+) /)[1]), 0);
  assert.equal(hidden + neighbors.length, 105, 'Pill counts plus drawn links equal the link total');
  assert.ok(first['Harbor Mutual Insurance'], 'The two-family neighbor is drawn once, in money');
  assert.ok(first['+20 Money and accounts'] && first['+23 Parties and roles']);

  // Encoding: the inferred link is dashed 6 on, 4 off at every zoom; the mixed link is solid; no text under 12 px.
  const strokes = await page.evaluate(() => window.__strokes);
  assert.deepEqual(strokesTo(strokes, first['Avery Brooks']).map(item => item.dash), [[6, 4]], 'The inferred link is dashed');
  assert.deepEqual(strokesTo(strokes, first['Policy HM-2208 (auto)']).map(item => item.dash), [[]], 'The mixed link is solid');
  assert.ok(await page.evaluate(() => window.__minFont) >= 12, 'No canvas text is smaller than 12 px');

  // Same URL, same positions.
  const second = await open(page);
  assert.equal(world(second), world(first), 'Two loads of the same URL give identical node positions');

  // Dragging a node does not move it.
  const target = second['Marlow Freight Co.'];
  const box = await page.locator('canvas').first().boundingBox();
  await page.mouse.move(box.x + target.x, box.y + target.y);
  await page.mouse.down();
  await page.mouse.move(box.x + target.x + 80, box.y + target.y + 60, { steps: 8 });
  await page.mouse.up();
  await page.waitForTimeout(300);
  const dragged = await frameNow(page);
  assert.equal(world(dragged), world(second), 'Node drag is off');

  // An early gesture owns the camera: the initial fit does not run again.
  const zoomed = await page.locator('canvas').first().evaluate(element => element.__zoom.k);
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.wheel(0, -1200);
  await page.waitForTimeout(600);
  const closeZoom = await page.locator('canvas').first().evaluate(element => element.__zoom.k);
  assert.ok(closeZoom > zoomed * 1.5, `Wheel zooms in: ${zoomed} -> ${closeZoom}`);
  await page.waitForTimeout(800);
  assert.equal(await page.locator('canvas').first().evaluate(element => element.__zoom.k), closeZoom);
  // Labels keep their screen size at every zoom.
  const close = await frameNow(page);
  for (const frame of [second, close]) assert.equal(frame['Rowan Vale'].font, 12, `label ${frame['Rowan Vale'].font}px`);
  // Fit view returns from the close-up.
  await page.getByRole('button', { name: 'Fit view', exact: true }).click();
  await page.waitForTimeout(600);
  const fitted = await page.locator('canvas').first().evaluate(element => element.__zoom.k);
  assert.ok(Math.abs(fitted - zoomed) / zoomed < 0.1, `Fit view: ${fitted} vs initial fit ${zoomed}`);

  // A Shift+click on a pill adds up to 25 neighbors of its family at the end of its sector; other sectors keep their places.
  const before = await open(page);
  const pill = before['+23 Parties and roles'];
  // The renderer refreshes its hit canvas at most every 800 ms; the tooltip shows the pill is under the pointer.
  await page.waitForTimeout(900);
  await page.mouse.move(box.x + pill.x, box.y + pill.y);
  await page.getByText('+23 Parties and roles', { exact: true }).waitFor({ state: 'visible' });
  await page.keyboard.down('Shift');
  await page.mouse.click(box.x + pill.x, box.y + pill.y);
  await page.keyboard.up('Shift');
  // Over 40 nodes, labels that would overlap are skipped; the pill count shows the 22 added neighbors.
  await page.waitForFunction(() => window.__frame['+1 Parties and roles']);
  await page.waitForTimeout(300);
  const after = await frameNow(page);
  // Harbor Mutual's parties link stays folded: it is drawn once, in money.
  assert.ok(after['+1 Parties and roles'] && !after['+23 Parties and roles'], 'The press adds the 22 hidden parties neighbors');
  for (const name of ['Harbor Mutual Insurance', 'Hypertension', '+20 Money and accounts'])
    assert.deepEqual([after[name].worldX, after[name].worldY], [before[name].worldX, before[name].worldY], `${name} keeps its place`);
  await page.screenshot({ path: path.join(artifacts, 'graph-neighborhood-more.png') });

  // A node selected at the right edge stays visible beside the inspector.
  // Pan the node next to the right edge with a background drag.
  const start = after['Policy HO-5130 (home)'];
  await page.mouse.move(box.x + 30, box.y + 30);
  await page.mouse.down();
  await page.mouse.move(box.x + 30 + box.width - 40 - start.x, box.y + 30, { steps: 8 });
  await page.mouse.up();
  await page.waitForTimeout(900);
  const edge = (await page.evaluate(() => window.__frame))['Policy HO-5130 (home)'];
  assert.ok(edge.x > box.width - 60, `Node panned to the edge: ${edge.x}`);
  await page.mouse.move(box.x + edge.x, box.y + edge.y);
  await page.getByText('Policy HO-5130 (home) · Insurance policy', { exact: true }).waitFor({ state: 'visible' });
  await page.mouse.click(box.x + edge.x, box.y + edge.y);
  await page.getByRole('complementary', { name: 'Evidence inspector' }).getByRole('heading', { name: 'Policy HO-5130 (home)' }).waitFor();
  await page.waitForTimeout(300);
  const selected = (await page.evaluate(() => window.__frame))['Policy HO-5130 (home)'];
  const narrowed = await page.locator('canvas').first().boundingBox();
  assert.ok(selected.x > 10 && selected.x < narrowed.width - 10, `Selected node stays in view: ${selected.x} of ${narrowed.width}`);
  // The selected node's links carry their type and document count, still at 12 px or more.
  assert.ok((await frameNow(page))['COVERS · 4 docs'], 'The selected link is labeled');
  assert.ok(await page.evaluate(() => window.__minFont) >= 12, 'No canvas text is smaller than 12 px');

  // Names are drawn as literal text; a node without a name shows its kind and a short id, never the id.
  const desk = await open(page, `${base}/graph?focus=nb-desk`, 'Front desk: 3 links in 3 families.');
  const markup = '<b>Acme</b> & "Sons" <i>';
  assert.ok(desk[markup], `Literal markup label: ${Object.keys(desk)}`);
  assert.ok(desk['Organization nb-unn'] && !Object.keys(desk).includes('nb-unnamed'), `Unnamed label: ${Object.keys(desk)}`);
  assert.ok(desk['Loaner gadget'], 'A label outside the kinds is drawn');
  const deskBox = await page.locator('canvas').first().boundingBox();
  await page.waitForTimeout(900);
  await page.mouse.move(deskBox.x + desk[markup].x, deskBox.y + desk[markup].y);
  await page.getByText(`${markup} · Organization`, { exact: true }).waitFor({ state: 'visible' });
  assert.equal(await page.locator('b, i').filter({ hasText: 'Acme' }).count(), 0, 'No markup element is created');

  // In the light theme the canvas background equals --bg, and a theme change re-reads the tokens.
  const light = await context.newPage();
  light.on('pageerror', error => errors.push(error.message));
  await light.addInitScript(() => localStorage.setItem('theme', 'light'));
  await open(light);
  const background = async () => resolve(light, await light.locator('canvas').first().evaluate(canvas => getComputedStyle(canvas).backgroundColor));
  const lightBg = await resolve(light, 'var(--bg)');
  assert.equal(await background(), lightBg, 'Light canvas background equals --bg');
  await light.getByRole('button', { name: 'Toggle theme', exact: true }).click();
  await light.waitForFunction(() => document.documentElement.classList.contains('dark'));
  const darkBg = await resolve(light, 'var(--bg)');
  assert.notEqual(darkBg, lightBg);
  await light.waitForTimeout(300);
  assert.equal(await background(), darkBg, 'Dark canvas background equals --bg after the theme change');
  assert.deepEqual(errors, []);
  console.log('PASS graph_neighborhood caps, pills, fixed positions, no drag, initial fit, Fit view, +N more, link dash, 12 px text, literal names and token canvas colors');
} finally {
  await browser.close();
}
