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

// Each frame of the visible canvas: every label or pill text with its world and screen position.
await context.addInitScript(() => {
  window.__frame = {};
  const proto = CanvasRenderingContext2D.prototype;
  const centers = new WeakMap();
  const clearRect = proto.clearRect;
  proto.clearRect = function (...args) {
    if (this.canvas.isConnected) window.__frame = {};
    return clearRect.apply(this, args);
  };
  const arc = proto.arc;
  proto.arc = function (...args) {
    centers.set(this, args);
    return arc.apply(this, args);
  };
  const fillText = proto.fillText;
  proto.fillText = function (text, x, y, ...rest) {
    if (this.canvas.isConnected) {
      const pill = text.startsWith('+');
      const [worldX, worldY] = pill ? [x, y] : centers.get(this) || [x, y];
      const transform = this.getTransform();
      const point = new DOMPoint(worldX, worldY).matrixTransform(transform);
      window.__frame[text] = { worldX, worldY, x: point.x / devicePixelRatio, y: point.y / devicePixelRatio,
        font: parseFloat(this.font) * transform.a / devicePixelRatio };
    }
    return fillText.call(this, text, x, y, ...rest);
  };
});

async function open(page) {
  await page.goto(hubUrl);
  await page.getByText('Rowan Vale: 105 links in 9 families. Large hub: the top 2 of each family are shown.', { exact: true }).waitFor();
  await page.locator('canvas').first().waitFor({ state: 'visible' });
  // The first layout is fitted once; wait until the camera and the paint stay still.
  await page.waitForFunction(() => {
    const zoom = document.querySelector('canvas')?.__zoom;
    if (!zoom || zoom.k === 1 || !window.__frame['Rowan Vale']) return false;
    const signature = JSON.stringify([zoom, Object.keys(window.__frame).length]);
    if (window.__still?.signature !== signature) window.__still = { signature, since: Date.now() };
    return Date.now() - window.__still.since > 300;
  });
  return page.evaluate(() => window.__frame);
}
const pillsOf = frame => Object.keys(frame).filter(text => text.startsWith('+')).sort();
const neighborsOf = frame => Object.keys(frame).filter(text => !text.startsWith('+') && text !== 'Rowan Vale').sort();
const world = frame => JSON.stringify(Object.entries(frame).map(([text, at]) => [text, at.worldX, at.worldY]).sort());

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
  const dragged = await page.evaluate(() => window.__frame);
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
  const close = await page.evaluate(() => window.__frame);
  for (const frame of [second, close]) assert.ok(Math.abs(frame['Rowan Vale'].font - 11) < 0.2, `label ${frame['Rowan Vale'].font}px`);
  // Fit view returns from the close-up.
  await page.getByRole('button', { name: 'Fit view', exact: true }).click();
  await page.waitForTimeout(600);
  const fitted = await page.locator('canvas').first().evaluate(element => element.__zoom.k);
  assert.ok(Math.abs(fitted - zoomed) / zoomed < 0.1, `Fit view: ${fitted} vs initial fit ${zoomed}`);

  // A pill press adds up to 25 neighbors of its family at the end of its sector; other sectors keep their places.
  const before = await open(page);
  const pill = before['+23 Parties and roles'];
  // The renderer refreshes its hit canvas at most every 800 ms; the tooltip shows the pill is under the pointer.
  await page.waitForTimeout(900);
  await page.mouse.move(box.x + pill.x, box.y + pill.y);
  await page.getByText('+23 Parties and roles', { exact: true }).waitFor({ state: 'visible' });
  await page.mouse.click(box.x + pill.x, box.y + pill.y);
  await page.waitForFunction(() => Object.keys(window.__frame).filter(text => /^Parties fixture/.test(text)).length > 0);
  await page.waitForTimeout(300);
  const after = await page.evaluate(() => window.__frame);
  assert.equal(Object.keys(after).filter(text => /^Parties fixture/.test(text)).length, 22, 'The press adds the 22 hidden parties neighbors');
  // Harbor Mutual's parties link stays folded: it is drawn once, in money.
  assert.ok(after['+1 Parties and roles'] && !after['+23 Parties and roles']);
  for (const name of ['Harbor Mutual Insurance', 'Hypertension', '+20 Money and accounts'])
    assert.deepEqual([after[name].worldX, after[name].worldY], [before[name].worldX, before[name].worldY], `${name} keeps its place`);
  await page.screenshot({ path: path.join(artifacts, 'graph-neighborhood-more.png') });
  assert.deepEqual(errors, []);
  console.log('PASS graph_neighborhood caps, pills, fixed positions, no drag, initial fit, Fit view and +N more');
} finally {
  await browser.close();
}
