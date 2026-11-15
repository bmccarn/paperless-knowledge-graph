/** The focus canvas at 390 x 844 and its accessible alternative: name, keyboard zoom, bottom sheet, no overlaps. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3002';
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-phone-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 390, height: 844 } });
const errors = [];

// Each frame of the visible canvas, in screen pixels: the ink box of each text, and the center of each filled
// shape (nodes and pills fill a Path2D with the context moved to their center).
await context.addInitScript(() => {
  window.__frame = [];
  window.__shapes = [];
  const proto = CanvasRenderingContext2D.prototype;
  const screen = (ctx, x, y) => {
    const point = new DOMPoint(x, y).matrixTransform(ctx.getTransform());
    return { x: point.x / devicePixelRatio, y: point.y / devicePixelRatio };
  };
  const clearRect = proto.clearRect;
  proto.clearRect = function (...args) {
    if (this.canvas.isConnected) { window.__frame = []; window.__shapes = []; }
    return clearRect.apply(this, args);
  };
  const fill = proto.fill;
  proto.fill = function (...args) {
    if (this.canvas.isConnected && args[0] instanceof Path2D) window.__shapes.push(screen(this, 0, 0));
    return fill.apply(this, args);
  };
  const fillText = proto.fillText;
  proto.fillText = function (text, x, y, ...rest) {
    if (this.canvas.isConnected) {
      const ink = this.measureText(text);
      const [a, b] = [screen(this, x - ink.actualBoundingBoxLeft, y - ink.actualBoundingBoxAscent),
        screen(this, x + ink.actualBoundingBoxRight, y + ink.actualBoundingBoxDescent)];
      window.__frame.push([text, { ...screen(this, 0, 0), box: { left: a.x, top: a.y, right: b.x, bottom: b.y } }]);
    }
    return fillText.call(this, text, x, y, ...rest);
  };
});

const zoomOf = page => page.locator('canvas').first().evaluate(element => element.__zoom.k);
async function open(page, url) {
  await page.goto(url);
  await page.locator('canvas').first().waitFor({ state: 'visible' });
  await page.waitForFunction(() => {
    const zoom = document.querySelector('canvas')?.__zoom;
    if (!zoom || zoom.k === 1 || !window.__frame.length) return false;
    const signature = JSON.stringify([zoom, window.__frame.length]);
    if (window.__still?.signature !== signature) window.__still = { signature, since: Date.now() };
    return Date.now() - window.__still.since > 300;
  });
  return page.evaluate(() => ({ frame: window.__frame, shapes: window.__shapes }));
}
const hits = (a, b) => a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;
const pillBox = at => ({ left: at.x - (at.box.right - at.box.left) / 2 - 12, right: at.x + (at.box.right - at.box.left) / 2 + 12,
  top: at.y - 14, bottom: at.y + 14 });
// A node is at most 18 px in radius; its 3 px ring and halo stay within 20 px.
const nodeBox = at => ({ left: at.x - 20, right: at.x + 20, top: at.y - 20, bottom: at.y + 20 });

try {
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  const marker = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await marker.json()).fixture, 'paperless-accuracy-ui-v1');

  // The canvas is an image named by its summary.
  const { frame, shapes } = await open(page, `${base}/graph?focus=nb-rowan`);
  await page.screenshot({ path: path.join(artifacts, 'graph-phone-hub.png') });
  await page.getByRole('img', { name: 'Neighborhood of Rowan Vale: 105 links in 9 families, 1 inferred', exact: true }).waitFor();

  // At the fit zoom on a hub, pills clear the nodes and each other and stay inside the canvas; labels never overlap.
  const canvas = await page.locator('canvas').first().boundingBox();
  const pills = frame.filter(([text]) => /^\+\d+/.test(text));
  assert.equal(pills.length, 9, `One pill per family: ${pills.map(([text]) => text)}`);
  const pillCenters = new Set(pills.map(([, at]) => `${Math.round(at.x)},${Math.round(at.y)}`));
  const nodes = shapes.filter(at => !pillCenters.has(`${Math.round(at.x)},${Math.round(at.y)}`));
  for (const [index, [text, at]] of pills.entries()) {
    const box = pillBox(at);
    assert.ok(box.left >= 0 && box.right <= canvas.width && box.top >= 0 && box.bottom <= canvas.height,
      `${text} is inside the ${canvas.width} x ${canvas.height} canvas: ${JSON.stringify(box)}`);
    const nodesUnder = nodes.filter(node => hits(box, nodeBox(node)));
    assert.deepEqual(nodesUnder, [], `${text} covers no node`);
    for (const [other, at2] of pills.slice(index + 1)) assert.ok(!hits(box, pillBox(at2)), `${text} and ${other} do not overlap`);
  }
  // Labels never overlap one another or a pill, at the fit zoom and zoomed out, where the ring is crowded.
  const labelsClear = items => {
    const labels = items.filter(([text]) => !/^\+\d+/.test(text));
    const pillBoxes = items.filter(([text]) => /^\+\d+/.test(text)).map(([, at]) => pillBox(at));
    for (const [i, [text, at]] of labels.entries()) {
      for (const [other, at2] of labels.slice(i + 1)) assert.ok(!hits(at.box, at2.box), `Labels ${text} and ${other} do not overlap`);
      assert.ok(!pillBoxes.some(box => hits(at.box, box)), `Label ${text} covers no pill`);
    }
    return labels.length;
  };
  assert.ok(labelsClear(frame) >= 8, 'At least 8 labels at the fit zoom');

  // No node, pill or label lies under the Legend button or the zoom buttons.
  for (const control of [page.getByRole('button', { name: 'Legend', exact: true }), page.getByRole('group', { name: 'Zoom', exact: true })]) {
    const at = await control.boundingBox();
    const area = { left: at.x - canvas.x, right: at.x + at.width - canvas.x, top: at.y - canvas.y, bottom: at.y + at.height - canvas.y };
    const under = [...nodes.filter(node => hits(area, nodeBox(node))).map(node => `node at ${node.x},${node.y}`),
      ...frame.filter(([, item]) => hits(area, item.box)).map(([text]) => text)];
    assert.deepEqual(under, [], 'Nothing lies under the canvas controls');
  }

  // Zoom in, Zoom out and Fit view work from the keyboard.
  const fitted = await zoomOf(page);
  await page.getByRole('button', { name: 'Zoom in', exact: true }).focus();
  await page.keyboard.press('Enter');
  await page.waitForTimeout(500);
  assert.ok(Math.abs(await zoomOf(page) - fitted * 1.5) < 1e-6, 'Enter on Zoom in zooms in by 1.5');
  await page.keyboard.press('Tab');
  await page.keyboard.press('Space');
  await page.keyboard.press('Space');
  await page.waitForTimeout(500);
  assert.ok(Math.abs(await zoomOf(page) - fitted / 1.5) < 1e-6, 'Tab, then Space twice on Zoom out zooms out twice');
  labelsClear(await page.evaluate(() => window.__frame));
  await page.keyboard.press('Tab');
  await page.keyboard.press('Enter');
  await page.waitForTimeout(500);
  assert.ok(Math.abs(await zoomOf(page) - fitted) < 1e-6, 'Enter on Fit view returns to the fit zoom');

  // Selecting a node opens the inspector as a bottom sheet with a 300 px peek; the node stays visible above it.
  await page.waitForTimeout(900);
  const label = async name => (await page.evaluate(() => window.__frame)).find(([text]) => text === name)[1];
  const juniper = await label('Juniper Okafor');
  await page.mouse.click(canvas.x + juniper.x, canvas.y + juniper.y);
  const sheet = page.getByRole('complementary', { name: 'Evidence inspector' });
  await sheet.getByRole('heading', { name: 'Juniper Okafor' }).waitFor();
  await page.waitForTimeout(300);
  const visible = async () => {
    const node = await label('Juniper Okafor');
    const area = await page.locator('canvas').first().boundingBox();
    const panel = await sheet.boundingBox();
    assert.ok(area.y + area.height <= panel.y + 1, 'The canvas ends where the sheet starts');
    assert.ok(node.y > 0 && node.y < area.height && node.x > 0 && node.x < area.width,
      `The selected node stays above the sheet: ${node.x},${node.y} in ${area.width} x ${area.height}`);
    return panel;
  };
  assert.equal(Math.round((await visible()).height), 300, 'The sheet peeks 300 px');
  await page.screenshot({ path: path.join(artifacts, 'graph-phone-sheet.png') });
  const handle = page.getByRole('button', { name: 'Expand inspector', exact: true });
  assert.equal(await handle.getAttribute('aria-expanded'), 'false');
  await handle.click();
  await page.getByRole('button', { name: 'Collapse inspector', exact: true }).waitFor();
  await page.waitForTimeout(300);
  assert.ok((await visible()).height > 400, 'The expanded sheet is taller than its peek');
  await page.screenshot({ path: path.join(artifacts, 'graph-phone-sheet-expanded.png') });
  await page.getByRole('button', { name: 'Collapse inspector', exact: true }).click();
  assert.equal(Math.round((await visible()).height), 300, 'Collapsing returns to the peek');

  assert.deepEqual(errors, []);
  console.log('PASS graph_phone canvas name, keyboard zoom, bottom sheet peek, pills and labels clear at 390 px');
} finally {
  await browser.close();
}
