/** Real wheel gestures and canvas paint measurements against synthetic data only. */
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3002';
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-zoom-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
const page = await context.newPage();
const errors = [];
page.on('pageerror', error => errors.push(error.message));
try {
  const marker = await context.request.get(`${base}/api/_fixture`);
  assert.equal(marker.status(), 200);
  assert.equal((await marker.json()).fixture, 'paperless-accuracy-ui-v1');
  // Observe actual screen geometry without exposing a production test API or
  // changing the renderer's data, camera, paint calls or zoom transform.
  // A node label is drawn in screen pixels with the context moved to its node's center, so the transform
  // origin is the node's position. The text middle sits 18 px below a 6 px node: 4 px gap plus half the 16 px line.
  await page.addInitScript(() => {
    window.__graphPaint = {};
    const proto = CanvasRenderingContext2D.prototype;
    const fillText = proto.fillText;
    proto.fillText = function (text, x, y, ...rest) {
      if (text === 'Alex Example') {
        const transform = this.getTransform();
        const scale = transform.a / devicePixelRatio;
        const point = new DOMPoint(0, 0).matrixTransform(transform);
        const zoom = this.canvas.__zoom || { k: 1, x: 0, y: 0 };
        const at = { x: point.x / devicePixelRatio, y: point.y / devicePixelRatio };
        window.__graphPaint[text] = {
          scale, font: parseFloat(this.font.match(/([\d.]+)px/)[1]) * scale, gap: y * scale, ...at,
          worldX: (at.x - zoom.x) / zoom.k, worldY: (at.y - zoom.y) / zoom.k,
        };
      }
      return fillText.call(this, text, x, y, ...rest);
    };
  });
  // Interact before simulation cooldown: automatic initial fitting must yield.
  await page.goto(`${base}/graph`);
  const earlyCanvas = page.locator('canvas').first();
  await earlyCanvas.waitFor({ state: 'visible' });
  const earlyBounds = await earlyCanvas.boundingBox();
  await page.mouse.move(earlyBounds.x + earlyBounds.width / 2, earlyBounds.y + earlyBounds.height / 2);
  await page.mouse.wheel(0, -1600);
  await page.waitForTimeout(250);
  const earlyZoom = await earlyCanvas.evaluate(element => element.__zoom.k);
  await page.waitForTimeout(2200);
  const settledEarlyZoom = await earlyCanvas.evaluate(element => element.__zoom.k);
  await page.screenshot({ path: path.join(artifacts, 'graph-early-zoom.png'), fullPage: true });
  assert.ok(earlyZoom > 1, 'Early wheel gesture must actually zoom');
  assert.ok(Math.abs(settledEarlyZoom - earlyZoom) < 0.001,
            `Initial fit must not overwrite early user zoom: ${earlyZoom} -> ${settledEarlyZoom}`);
  // Pointer-driven pan also takes ownership before simulation cooldown.
  await page.goto(`${base}/graph`);
  const panCanvas = page.locator('canvas').first();
  await panCanvas.waitFor({ state: 'visible' });
  const panBounds = await panCanvas.boundingBox();
  await page.mouse.move(panBounds.x + 20, panBounds.y + 20);
  await page.mouse.down();
  await page.mouse.move(panBounds.x + 100, panBounds.y + 60, { steps: 5 });
  await page.mouse.up();
  const earlyPan = await panCanvas.evaluate(element => ({ ...element.__zoom }));
  await page.waitForTimeout(2400);
  const settledPan = await panCanvas.evaluate(element => ({ ...element.__zoom }));
  assert.deepEqual(settledPan, earlyPan, 'Initial fit must not overwrite an early pan');

  // A gesture as automatic fitting starts must not fight a queued fit animation.
  await page.goto(`${base}/graph`);
  const boundaryCanvas = page.locator('canvas').first();
  await boundaryCanvas.waitFor({ state: 'visible' });
  const boundaryInitial = await boundaryCanvas.evaluate(element => element.__zoom.k);
  await page.waitForFunction(initial => document.querySelector('canvas')?.__zoom.k !== initial, boundaryInitial);
  const boundaryBounds = await boundaryCanvas.boundingBox();
  await page.mouse.move(boundaryBounds.x + boundaryBounds.width / 2, boundaryBounds.y + boundaryBounds.height / 2);
  await page.mouse.wheel(0, -800);
  await page.waitForTimeout(250);
  const boundaryZoom = await boundaryCanvas.evaluate(element => element.__zoom.k);
  await page.waitForTimeout(1000);
  assert.ok(Math.abs(await boundaryCanvas.evaluate(element => element.__zoom.k) - boundaryZoom) < 0.001,
            'Automatic fit must not continue animating over a boundary gesture');
  // A fresh untouched renderer still gets its automatic fit (checked below).
  await page.goto(`${base}/graph`);
  const canvas = page.locator('canvas').first();
  await canvas.waitFor({ state: 'visible' });
  await page.waitForFunction(() => window.__graphPaint['Alex Example']);
  // Wait for the initial automatic fit and a stable camera before user input.
  await page.waitForFunction(() => {
    const paint = window.__graphPaint['Alex Example'];
    const zoom = document.querySelector('canvas')?.__zoom;
    if (!paint || !zoom || zoom.k === 1) return false;
    const signature = JSON.stringify([zoom.k, zoom.x, zoom.y, paint.x, paint.y]);
    if (window.__lastGraphView?.signature !== signature) {
      window.__lastGraphView = { signature, since: Date.now() };
    }
    return Date.now() - window.__lastGraphView.since > 300;
  });
  const initial = await page.evaluate(() => window.__graphPaint['Alex Example']);
  const bounds = await canvas.boundingBox();
  assert.ok(bounds);
  await page.mouse.move(bounds.x + initial.x, bounds.y + initial.y);
  await page.mouse.wheel(0, -1800);
  await page.waitForTimeout(400);
  await page.mouse.wheel(0, -1800);
  await page.waitForTimeout(400);
  const closeup = await page.evaluate(() => window.__graphPaint['Alex Example']);
  const zoom = await canvas.evaluate(element => element.__zoom.k);
  await page.screenshot({ path: path.join(artifacts, 'graph-closeup.png'), fullPage: true });
  await writeFile(path.join(artifacts, 'graph-zoom-measurements.json'), JSON.stringify({ initial, closeup, zoom, errors }, null, 2) + '\n');
  const failures = [];
  if (!(zoom > 16)) failures.push(`Wheel zoom stopped at ${zoom}; expected beyond 16x`);
  for (const [phase, paint] of Object.entries({ initial, closeup })) {
    for (const [metric, expected] of Object.entries({ font: 12, gap: 18 })) {
      if (Math.abs(paint[metric] - expected) > 0.2) failures.push(`${phase} ${metric}: ${paint[metric]}, expected ${expected} screen pixels`);
    }
  }
  assert.deepEqual(failures, []);
  // Place the selected node near the edge that the inspector will occupy.
  // Real background gestures reproduce viewport clipping without camera APIs.
  await page.mouse.move(bounds.x + 30, bounds.y + 30);
  await page.mouse.down();
  await page.mouse.move(bounds.x + 30 + bounds.width - 60 - closeup.x,
                        bounds.y + 30 + bounds.height / 2 - closeup.y, { steps: 10 });
  await page.mouse.up();
  const edge = await page.evaluate(() => window.__graphPaint['Alex Example']);
  assert.ok(edge.x > bounds.width - 90 && edge.x < bounds.width, 'Target is near the inspector edge');
  // Select the visibly zoomed node through its canvas hit area.
  await page.mouse.move(bounds.x + edge.x, bounds.y + edge.y);
  await page.getByText('Alex Example · Person', { exact: true }).waitFor({ state: 'visible' });
  await page.mouse.click(bounds.x + edge.x, bounds.y + edge.y);
  await page.locator('summary').filter({ hasText: 'Evidence from document #101' }).waitFor({ state: 'visible' });
  await page.waitForFunction(width => document.querySelector('canvas').clientWidth < width, bounds.width);
  await page.waitForTimeout(300);
  await page.screenshot({ path: path.join(artifacts, 'graph-inspector-closeup.png'), fullPage: true });
  async function assertSelectedVisible(phase) {
    const box = await canvas.boundingBox();
    const point = await page.evaluate(() => window.__graphPaint['Alex Example']);
    assert.ok(point.x > 10 && point.x < box.width - 10 && point.y > 10 && point.y < box.height - 10,
              `${phase}: selected node must remain visible: ${JSON.stringify({point,box})}`);
    assert.ok(await canvas.evaluate((element, point) => {
      const box = element.getBoundingClientRect();
      return document.elementFromPoint(box.x + point.x, box.y + point.y) === element;
    }, point), `${phase}: inspector must not cover the selected node`);
    assert.ok(Math.abs(await canvas.evaluate(element => element.__zoom.k) - zoom) < 0.001,
              `${phase}: keep the user's zoom`);
  }
  await assertSelectedVisible('Inspector open');
  await page.setViewportSize({ width: 1200, height: 850 });
  await page.waitForTimeout(300);
  await assertSelectedVisible('Viewport resized');
  await page.screenshot({ path: path.join(artifacts, 'graph-resized-closeup.png'), fullPage: true });
  for (const width of [1000, 768, 640]) {
    await page.setViewportSize({ width, height: 850 });
    await page.waitForTimeout(300);
    await assertSelectedVisible(`Viewport ${width}`);
  }
  await page.screenshot({ path: path.join(artifacts, 'graph-tablet-closeup.png'), fullPage: true });
  await page.setViewportSize({ width: 390, height: 850 });
  await page.waitForTimeout(300);
  await page.getByRole('complementary', { name: 'Evidence inspector' }).waitFor({ state: 'visible' });
  await page.screenshot({ path: path.join(artifacts, 'graph-phone-inspector.png'), fullPage: true });
  await page.getByRole('button', { name: 'Close node inspector', exact: true }).click();
  await page.getByRole('complementary', { name: 'Evidence inspector' }).waitFor({ state: 'hidden' });
  await assertSelectedVisible('Phone inspector closed');
  await page.screenshot({ path: path.join(artifacts, 'graph-phone-restored.png'), fullPage: true });
  await page.setViewportSize({ width: 1200, height: 850 });
  await page.waitForTimeout(300);
  const beforePan = await canvas.evaluate(element => ({ ...element.__zoom }));
  const resized = await canvas.boundingBox();
  await page.mouse.move(resized.x + 35, resized.y + 35);
  await page.mouse.down();
  await page.mouse.move(resized.x + 115, resized.y + 75, { steps: 5 });
  await page.mouse.up();
  const afterPan = await canvas.evaluate(element => ({ ...element.__zoom }));
  assert.equal(afterPan.k, beforePan.k);
  assert.ok(Math.abs(afterPan.x - beforePan.x - 80) < 1, 'Dragging the background pans horizontally');
  assert.ok(Math.abs(afterPan.y - beforePan.y - 40) < 1, 'Dragging the background pans vertically');
  await page.getByRole('button', { name: 'Fit view', exact: true }).click();
  await page.waitForTimeout(500);
  const fittedZoom = await canvas.evaluate(element => element.__zoom.k);
  assert.ok(fittedZoom < zoom / 2, `Fit view must leave close-up: ${fittedZoom} vs ${zoom}`);
  await page.screenshot({ path: path.join(artifacts, 'graph-fit.png'), fullPage: true });
  // A dragged node stays pinned when expansion replaces the renderer data.
  const pinBounds = await canvas.boundingBox();
  const pinStart = await page.evaluate(() => window.__graphPaint['Alex Example']);
  await page.mouse.move(pinBounds.x + pinStart.x, pinBounds.y + pinStart.y);
  await page.mouse.down();
  await page.mouse.move(pinBounds.x + pinStart.x + 25, pinBounds.y + pinStart.y + 25, { steps: 8 });
  await page.mouse.up();
  await page.waitForTimeout(300);
  const pinned = await page.evaluate(() => window.__graphPaint['Alex Example']);
  await page.getByRole('button', { name: /Alex Example/ }).click();
  const neighbors = page.waitForResponse(response => new URL(response.url()).pathname.startsWith('/api/graph/neighbors/'));
  await page.getByRole('button', { name: 'Load neighbors into graph', exact: true }).click();
  await neighbors;
  await page.waitForFunction(() => [...document.querySelectorAll('button')].some(button =>
    button.textContent.includes('Load neighbors into graph') && !button.disabled));
  await page.waitForTimeout(1000);
  const expanded = await page.evaluate(() => window.__graphPaint['Alex Example']);
  assert.ok(Math.abs(expanded.worldX - pinned.worldX) < 0.01, 'Expansion preserves the pinned x coordinate');
  assert.ok(Math.abs(expanded.worldY - pinned.worldY) < 0.01, 'Expansion preserves the pinned y coordinate');
  assert.deepEqual(errors, []);
  console.log(`PASS graph_closeup_zoom (${zoom.toFixed(1)}x), constant label geometry, visible selection after inspector/resize, panning and Fit view`);
} finally {
  await browser.close();
}
