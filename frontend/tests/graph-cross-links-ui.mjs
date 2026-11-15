/** Links between neighbors in Focus at /graph?focus=<id>: the curves, the cap, the switch, the inspector group and the states. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3002';
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-cross-links-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
const errors = [];
const hubUrl = `${base}/graph?focus=nb-rowan`;

// Each frame of the visible canvas: label positions (labels are drawn with the context at their node's center)
// and every link stroke with its first and last point in screen pixels.
await context.addInitScript(() => {
  window.__frame = {};
  window.__strokes = [];
  const proto = CanvasRenderingContext2D.prototype;
  const path = new WeakMap();
  const screen = (ctx, x, y) => {
    const point = new DOMPoint(x, y).matrixTransform(ctx.getTransform());
    return { x: point.x / devicePixelRatio, y: point.y / devicePixelRatio };
  };
  const clearRect = proto.clearRect;
  proto.clearRect = function (...args) {
    if (this.canvas.isConnected) { window.__frame = {}; window.__strokes = []; }
    return clearRect.apply(this, args);
  };
  const moveTo = proto.moveTo;
  proto.moveTo = function (x, y) {
    path.set(this, { start: screen(this, x, y) });
    return moveTo.call(this, x, y);
  };
  for (const name of ['lineTo', 'quadraticCurveTo']) {
    const original = proto[name];
    proto[name] = function (...args) {
      const current = path.get(this);
      if (current) current.end = screen(this, args.at(-2), args.at(-1));
      if (current && name === 'quadraticCurveTo') current.control = screen(this, args[0], args[1]);
      return original.apply(this, args);
    };
  }
  const stroke = proto.stroke;
  proto.stroke = function (...args) {
    const current = path.get(this);
    if (this.canvas.isConnected && !args.length && current?.end) window.__strokes.push({ ...current, dash: this.getLineDash().length > 0 });
    return stroke.apply(this, args);
  };
  const fillText = proto.fillText;
  proto.fillText = function (text, ...rest) {
    if (this.canvas.isConnected) window.__frame[text] = screen(this, 0, 0);
    return fillText.call(this, text, ...rest);
  };
});

/** Wait until the caption (a string or pattern for the cross-link status, or a locator) shows and the camera and paint stay still. */
async function open(page, url, caption) {
  await page.goto(url);
  await (typeof caption === 'function' ? caption(page) : page.getByRole('status').filter({ hasText: caption }).first()).waitFor();
  await page.locator('canvas').first().waitFor({ state: 'visible' });
  await page.waitForFunction(() => {
    const zoom = document.querySelector('canvas')?.__zoom;
    if (!zoom || zoom.k === 1 || !Object.keys(window.__frame).length) return false;
    const signature = JSON.stringify([zoom, window.__strokes.length]);
    if (window.__still?.signature !== signature) window.__still = { signature, since: Date.now() };
    return Date.now() - window.__still.since > 300;
  });
  return page.evaluate(() => ({ frame: window.__frame, strokes: window.__strokes }));
}
const near = (a, b) => Math.hypot(a.x - b.x, a.y - b.y) < 1;
/** Strokes that join two neighbors: neither end is the focus. */
const crossStrokes = ({ frame, strokes }) => strokes.filter(item => !near(item.start, frame['Rowan Vale']) && !near(item.end, frame['Rowan Vale']));
const round = strokes => JSON.stringify(strokes.map(item => [item.start, item.control, item.end].map(point => point && [Math.round(point.x * 10), Math.round(point.y * 10)])));
const canvasName = page => page.getByRole('img').first().getAttribute('aria-label');

const FIXTURE = [
  ['Juniper Okafor', 'Marlow Freight Co.'], ['Juniper Okafor', 'Marlow Freight Co.'],
  ['Policy HM-2208 (auto)', 'Harbor Mutual Insurance'], ['Checking account 4417', 'Harbor Mutual Insurance'],
  ['Policy HM-2208 (auto)', '2019 Subaru Outback'], ['Policy HM-2208 (auto)', '1 Apr 2026'], ['Policy HM-2208 (auto)', '1 Oct 2026'],
  ['14 Larkspur Ct', 'Port Ellery'], ['County Tax Office', 'Port Ellery'], ['Port Ellery Library', 'Port Ellery'],
  ['Telematics plug TX-2', '2019 Subaru Outback'], ['Lipid panel, March 2026', 'Hypertension'], ['Avery Brooks', 'Riverside 10K'],
];

try {
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  const requested = [];
  page.on('request', request => {
    if (request.url().includes('/api/graph/subgraph')) requested.push(new URL(request.url()).searchParams.get('ids').split(','));
  });
  const marker = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await marker.json()).fixture, 'paperless-accuracy-ui-v1');

  // Each fixture link between two drawn neighbors is drawn once, bowed, under the focus links; no pill member is requested.
  const first = await open(page, hubUrl, '13 links between neighbors.');
  await page.screenshot({ path: path.join(artifacts, 'cross-links-hub.png') });
  assert.equal(requested.length, 1);
  assert.equal(requested[0].length, 18, 'The 18 drawn neighbors are sent');
  assert.ok(!requested[0].some(id => id.includes('filler')), `No pill member is sent: ${requested[0]}`);
  const cross = crossStrokes(first);
  assert.equal(cross.length, FIXTURE.length, `${cross.length} curves`);
  for (const [source, target] of FIXTURE) {
    const matches = cross.filter(item => near(item.start, first.frame[source]) && near(item.end, first.frame[target]));
    assert.equal(matches.length, FIXTURE.filter(pair => pair[0] === source && pair[1] === target).length, `${source} to ${target}`);
  }
  assert.ok(cross.every(item => item.control), 'Every link between neighbors is a curve');
  const focusAt = first.frame['Rowan Vale'];
  // The parallel pair (Juniper Okafor and Marlow Freight Co.) curves at +/-0.15 around that, as any parallel pair does.
  const single = cross.filter(item => !near(item.start, first.frame['Juniper Okafor']));
  assert.equal(single.length, FIXTURE.length - 2);
  for (const item of single) {
    const middle = { x: (item.start.x + item.end.x) / 2, y: (item.start.y + item.end.y) / 2 };
    assert.ok(Math.hypot(item.control.x - focusAt.x, item.control.y - focusAt.y) > Math.hypot(middle.x - focusAt.x, middle.y - focusAt.y) - 0.5,
      'Each curve bows away from the focus');
  }
  assert.equal(cross.filter(item => item.dash).length, 2, 'The two inferred links are dashed');
  assert.ok(first.strokes.indexOf(cross.at(-1)) < first.strokes.findIndex(item => near(item.start, focusAt) || near(item.end, focusAt)),
    'Links between neighbors are drawn before the focus links');
  assert.equal(await canvasName(page),
    'Neighborhood of Rowan Vale: 105 links in 9 families, 1 inferred; 13 links between neighbors.');

  // Reloading gives identical positions and curves.
  const second = await open(page, hubUrl, '13 links between neighbors.');
  assert.equal(round(crossStrokes(second)), round(cross));
  assert.deepEqual(second.frame['Juniper Okafor'], first.frame['Juniper Okafor']);

  // Selecting a neighbor: its links between neighbors carry labels; the inspector lists them, the keyboard route to them.
  const box = await page.locator('canvas').first().boundingBox();
  await page.waitForTimeout(900);
  const policy = second.frame['Policy HM-2208 (auto)'];
  await page.mouse.move(box.x + policy.x, box.y + policy.y);
  await page.getByText('Policy HM-2208 (auto) · Insurance policy', { exact: true }).waitFor({ state: 'visible' });
  await page.mouse.click(box.x + policy.x, box.y + policy.y);
  const inspector = page.getByRole('complementary', { name: 'Evidence inspector' });
  const group = inspector.getByRole('list', { name: 'Linked to other neighbors' });
  await group.waitFor();
  assert.deepEqual(await group.getByRole('button').allInnerTexts().then(rows => rows.map(row => row.split('\n')[0])),
    ['Harbor Mutual Insurance', '2019 Subaru Outback', '1 Apr 2026', '1 Oct 2026'], 'Strongest first');
  // Labels on a selection are optional where they would overlap, as for the focus links.
  await page.waitForFunction(() => ['ISSUED_BY · 5 docs', 'COVERS · 3 docs', 'EFFECTIVE_FROM · 2 docs', 'EXPIRES_ON · 1 doc']
    .filter(text => window.__frame[text]).length >= 2);
  await page.screenshot({ path: path.join(artifacts, 'cross-links-selected-neighbor.png') });
  await group.getByRole('button').first().focus();
  await page.keyboard.press('Enter');
  await page.waitForURL(url => url.searchParams.get('sel') === 'nb-coverage-0~ISSUED_BY~nb-money-0');
  const linkHeading = inspector.getByRole('heading', { level: 2 });
  const title = 'Policy HM-2208 (auto) issued by Harbor Mutual Insurance';
  await linkHeading.filter({ hasText: 'issued by' }).waitFor();
  assert.equal((await linkHeading.innerText()).replace(/\s+/g, ' '), title);
  await inspector.getByText('Synthetic archive 217', { exact: true }).waitFor();
  await page.reload();
  await inspector.getByText('Synthetic archive 217', { exact: true }).waitFor();
  assert.equal((await linkHeading.innerText()).replace(/\s+/g, ' '), title, 'The selected link survives a reload');
  await page.screenshot({ path: path.join(artifacts, 'cross-links-selected-link.png') });

  // The switch: off writes cross=0 in place of the current entry and draws no curve; on brings them back.
  await open(page, hubUrl, '13 links between neighbors.');
  const history = await page.evaluate(() => history.length);
  const toggle = page.getByRole('button', { name: 'Links between neighbors', exact: true });
  assert.equal(await toggle.getAttribute('aria-pressed'), 'true');
  await toggle.click();
  await page.waitForURL(url => url.searchParams.get('cross') === '0');
  assert.equal(await toggle.getAttribute('aria-pressed'), 'false');
  assert.equal(await page.evaluate(() => history.length), history, 'The switch replaces the history entry');
  await page.waitForTimeout(400);
  assert.equal(crossStrokes(await page.evaluate(() => ({ frame: window.__frame, strokes: window.__strokes }))).length, 0);
  assert.equal(await canvasName(page), 'Neighborhood of Rowan Vale: 105 links in 9 families, 1 inferred.');
  const off = await open(page, `${hubUrl}&cross=0`, page => page.getByText('Rowan Vale: 105 links in 9 families. Large hub: the top 2 of each family are shown.', { exact: true }));
  assert.equal(crossStrokes(off).length, 0, 'cross=0 from the URL draws no curve');
  await toggle.click();
  await page.getByText('13 links between neighbors.', { exact: true }).waitFor();
  assert.equal(new URL(page.url()).searchParams.has('cross'), false);

  // Over the cap: 60 drawn, strongest first, and the caption names the total. On phone, 30.
  const many = async (route, count) => {
    const ids = new URL(route.request().url()).searchParams.get('ids').split(',');
    const links = [];
    for (let i = 0; i < ids.length && links.length < count; i++) for (let j = i + 1; j < ids.length && links.length < count; j++)
      links.push({ key: `${ids[i]}~RELATED_TO~${ids[j]}`, type: 'RELATED_TO', family: 'loose', source: ids[i], target: ids[j],
        support_state: 'supported', stated_count: 1, inferred_count: 0, records: [{ doc_id: 9000 + links.length, inferred: false, confidence: 0.9 }] });
    await route.fulfill({ json: { links, documents: {}, total: links.length, truncated: false } });
  };
  await page.route('**/api/graph/subgraph?**', route => many(route, 64));
  const capped = await open(page, hubUrl, 'Showing the 60 strongest of 64 links between neighbors.');
  assert.equal(crossStrokes(capped).length, 60);
  assert.match(await canvasName(page), /; 60 links between neighbors\.$/);
  await page.screenshot({ path: path.join(artifacts, 'cross-links-capped.png') });
  const phone = await context.newPage();
  await phone.setViewportSize({ width: 390, height: 844 });
  await phone.route('**/api/graph/subgraph?**', route => many(route, 64));
  await phone.goto(hubUrl);
  await phone.getByText('Showing the 30 strongest of 64 links between neighbors.', { exact: true }).waitFor();
  await phone.getByRole('button', { name: 'Links between neighbors', exact: true }).waitFor();
  await phone.close();

  // None, error with Retry, and the status line after 1 s.
  await page.unroute('**/api/graph/subgraph?**');
  await page.route('**/api/graph/subgraph?**', route => route.fulfill({ json: { links: [], documents: {}, total: 0, truncated: false } }));
  await open(page, hubUrl, 'No links between these neighbors.');
  assert.match(await canvasName(page), /; 0 links between neighbors\.$/);
  await page.unroute('**/api/graph/subgraph?**');
  await page.route('**/api/graph/subgraph?**', route => route.fulfill({ status: 500, json: { detail: 'Synthetic failure' } }));
  await open(page, hubUrl, /^Links between neighbors did not load\. Retry$/);
  assert.equal(await canvasName(page), 'Neighborhood of Rowan Vale: 105 links in 9 families, 1 inferred.');
  await page.unroute('**/api/graph/subgraph?**');
  await page.route('**/api/graph/subgraph?**', async route => { await new Promise(resolve => setTimeout(resolve, 1600)); await route.continue(); });
  await page.getByRole('button', { name: 'Retry', exact: true }).click();
  await page.getByText('Loading links between neighbors', { exact: true }).waitFor();
  await page.getByText('13 links between neighbors.', { exact: true }).waitFor();
  await page.unroute('**/api/graph/subgraph?**');

  // A focus with one neighbor sends no request, which the route would refuse.
  requested.length = 0;
  await page.goto(`${base}/graph?focus=nb-renewal`);
  await page.getByText('Renewal notice 2026: 1 links in 1 families.', { exact: true }).waitFor();
  await page.waitForTimeout(500);
  assert.equal(requested.length, 0);
  assert.match(await canvasName(page), /; 0 links between neighbors\.$/);
  assert.deepEqual(errors, []);
  console.log('PASS graph_cross_links drawn once, curved, no pill member, 60 and 30 caps, fixed curves, switch and cross=0, inspector group, link selection, canvas name, loading, none and error');
} finally {
  await browser.close();
}
