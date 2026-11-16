/** Explore at /graph?focus=<id>&view=explore: the start ring, the handle, expansion, Back, collapse, hop 3, the limits and the states. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3002';
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-explore-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch({ headless: true });
const errors = [];
const root = `${base}/graph?focus=ex-policy&view=explore`;

/** A page that records where each canvas text is drawn: labels at their node, the handle at its own center. */
async function newPage(viewport = { width: 1440, height: 900 }) {
  const context = await browser.newContext({ viewport });
  await context.addInitScript(() => {
    window.__frame = {};
    const proto = CanvasRenderingContext2D.prototype;
    const clearRect = proto.clearRect;
    proto.clearRect = function (...args) {
      if (this.canvas.isConnected) window.__frame = {};
      return clearRect.apply(this, args);
    };
    const fillText = proto.fillText;
    proto.fillText = function (text, ...rest) {
      if (this.canvas.isConnected) {
        const point = new DOMPoint(0, 0).matrixTransform(this.getTransform());
        window.__frame[text] = { x: point.x / devicePixelRatio, y: point.y / devicePixelRatio };
      }
      return fillText.call(this, text, ...rest);
    };
  });
  const page = await context.newPage();
  page.on('console', message => { if (message.type() === 'error' && !message.text().includes('503')) errors.push(message.text()); });
  page.on('pageerror', error => errors.push(error.message));
  return page;
}
const caption = page => page.getByRole('status').filter({ hasText: /^Explore from / }).first();
/** Wait for the caption, then for the camera and the painted texts to stay still. */
async function settle(page, text) {
  await caption(page).filter({ hasText: text }).waitFor({ state: 'attached' });
  await page.waitForFunction(() => {
    const zoom = document.querySelector('canvas')?.__zoom;
    if (!zoom || zoom.k === 1 || !Object.keys(window.__frame).length) return false;
    const signature = JSON.stringify([zoom, Object.keys(window.__frame).sort()]);
    if (window.__still?.signature !== signature) window.__still = { signature, since: Date.now() };
    return Date.now() - window.__still.since > 400;
  });
  return page.evaluate(() => window.__frame);
}
const params = page => new URL(page.url()).searchParams;
/** Click a point recorded on the canvas, in canvas pixels, once force-graph's hit map has caught up (it repaints at most every 800 ms). */
async function clickAt(page, point) {
  await page.waitForTimeout(900);
  const box = await page.locator('canvas').first().boundingBox();
  await page.mouse.click(box.x + point.x, box.y + point.y);
}
const inspector = page => page.getByRole('complementary', { name: 'Evidence inspector' });
const trail = page => page.getByRole('region', { name: 'Explored from Policy KT-9030 (auto)' });

try {
  const page = await newPage();
  // The start ring: the 8 strongest of 10 links, plus 3 links among them that only the subgraph route returns.
  await page.goto(root);
  let frame = await settle(page, 'Explore from Policy KT-9030 (auto): 9 entities, 11 links, 1 hop.');
  assert.equal(await page.getByRole('img').first().getAttribute('aria-label'), 'Explore from Policy KT-9030 (auto): 9 entities, 11 links, 1 hop.');
  assert.equal(await page.getByRole('toolbar', { name: 'View' }).getByRole('button', { pressed: true }).textContent(), 'Explore');
  for (const name of ['Bayview Mutual', 'Lena Ortiz', '2021 Mazda CX-5', 'Calder Bay', 'Policy KT-8841 (2025)']) assert.ok(frame[name], `${name} is on the ring`);
  for (const name of ['Owen Pratt', 'Tidewater Credit Union', 'Car loan CL-7710']) assert.equal(frame[name], undefined, `${name} is not in view yet`);
  assert.equal(await trail(page).getByRole('listitem').count(), 1);
  assert.equal(await inspector(page).count(), 0, 'no inspector until something is selected');

  // Selecting a node shows its handle with the entities it could add; the handle expands it.
  const length = await page.evaluate(() => history.length);
  await clickAt(page, frame['2021 Mazda CX-5']);
  await page.waitForURL(url => url.searchParams.get('sel') === 'ex-car');
  frame = await settle(page, '9 entities');
  assert.ok(frame['+8'], 'the car has 8 entities not in view');
  await inspector(page).getByRole('button', { name: 'Show 6 more links from here' }).waitFor();
  await clickAt(page, frame['+8']);
  // Move off the canvas: a hovered node's handle replaces the selected node's.
  await page.mouse.move(0, 0);
  await page.waitForURL(url => url.searchParams.get('x') === 'ex-car');
  frame = await settle(page, 'Explore from Policy KT-9030 (auto): 15 entities, 21 links, 2 hops.');
  assert.equal(await page.evaluate(() => history.length), length + 1, 'an expansion is one history entry');
  for (const name of ['Car loan CL-7710', 'Bayline Auto Body', 'Tidewater Credit Union', 'DMV, Calder Bay office', 'Recall notice, 2025']) assert.ok(frame[name], name);
  assert.equal(frame['Northshore Mazda'], undefined, 'at most 6 new entities');
  assert.ok(frame['+2'], 'two entities remain');
  assert.match(await trail(page).getByRole('listitem').nth(1).textContent(), /2021 Mazda CX-5\+6/);
  // Its links to entities already in view are drawn: the holder (a loop to the start ring) and the root.
  const linked = await inspector(page).getByRole('list', { name: 'Linked in this view' }).getByRole('listitem').allTextContents();
  assert.ok(linked.some(text => text.startsWith('Lena Ortiz') && text.includes('Owner of')), linked.join(' | '));
  assert.ok(linked.some(text => text.startsWith('Policy KT-9030 (auto)')));
  assert.equal(linked.length, 8);

  // Back restores the previous view and URL; Forward returns.
  await page.goBack();
  await page.waitForURL(url => !url.searchParams.has('x'));
  await settle(page, ': 9 entities, 11 links, 1 hop.');
  assert.equal(params(page).get('sel'), 'ex-car');
  assert.equal(params(page).get('view'), 'explore');
  await page.goForward();
  await settle(page, ': 15 entities');

  // - collapses the selected node and + grows it again; a second expansion adds what is left, then nothing remains.
  await page.keyboard.press('-');
  await page.waitForURL(url => !url.searchParams.has('x'));
  await settle(page, ': 9 entities');
  await page.keyboard.press('+');
  await page.waitForURL(url => url.searchParams.get('x') === 'ex-car');
  await settle(page, ': 15 entities');
  await inspector(page).getByRole('button', { name: 'Show 2 more links from here' }).click();
  await page.waitForURL(url => url.searchParams.get('x') === 'ex-car,ex-car');
  frame = await settle(page, ': 17 entities');
  assert.ok(frame['All links shown']);
  assert.equal(await inspector(page).getByRole('button', { name: 'All links shown' }).isDisabled(), true);

  // Collapse in the trail removes the branch and everything added through it.
  await page.goto(`${root}&x=ex-car,ex-loan`);
  await settle(page, ': 18 entities, 24 links, 3 hops.');
  await trail(page).getByRole('button', { name: 'Collapse 2021 Mazda CX-5' }).click();
  await page.waitForURL(url => !url.searchParams.has('x'));
  await settle(page, ': 9 entities');

  // A hop-3 node offers Open in Focus instead of expanding, on its handle, in the inspector and on +.
  await page.goto(`${root}&x=ex-car,ex-loan&sel=ex-servicer`);
  frame = await settle(page, ': 18 entities, 24 links, 3 hops.');
  assert.ok(frame['Open in Focus'], 'the handle reads Open in Focus');
  assert.equal(await inspector(page).getByRole('button', { name: /^Show / }).count(), 0);
  await inspector(page).getByRole('button', { name: 'Open in Focus' }).waitFor();
  await page.keyboard.press('+');
  await page.waitForURL(url => url.searchParams.get('focus') === 'ex-servicer');
  assert.equal(params(page).has('view'), false);
  assert.equal(params(page).has('x'), false);

  // At 20 expansions, + explains the limit and changes nothing.
  const full = ['ex-car', ...Array.from({ length: 19 }, (_, index) => `gone-${index}`)].join(',');
  await page.goto(`${root}&x=${full}&sel=ex-loan`);
  await settle(page, ': 15 entities');
  const before = page.url();
  await page.keyboard.press('+');
  await page.getByText('This view holds at most 20 expansions. Collapse a branch or open one entity in Focus.').waitFor();
  assert.equal(page.url(), before);

  // A summary that fails leaves the view as it was and offers Retry in the trail.
  let fail = true;
  await page.route('**/api/graph/node/ex-loan?summary=1', route => (fail ? route.fulfill({ status: 503, json: { detail: 'Synthetic failure' } }) : route.fallback()));
  await page.goto(`${root}&x=ex-car,ex-loan`);
  await trail(page).getByRole('alert').filter({ hasText: 'Links of Car loan CL-7710 did not load.' }).waitFor();
  frame = await settle(page, ': 15 entities');
  assert.equal(frame['Saltmarsh Lending'], undefined);
  fail = false;
  await trail(page).getByRole('button', { name: 'Retry' }).click();
  await settle(page, ': 18 entities, 24 links, 3 hops.');

  // Sparse and empty roots.
  await page.goto(`${base}/graph?focus=ex-hq&view=explore`);
  await settle(page, 'Explore from 12 Wharf Rd: 2 entities, 1 link, 1 hop.');
  await page.goto(`${base}/graph?focus=nb-leaf&view=explore`);
  await page.getByText('No direct links extracted. This entity appears in 2 documents.').waitFor();
  await page.screenshot({ path: path.join(artifacts, 'explore-desktop.png') });

  // Phone: 6 starting links and 4 per expansion; the trail is a sheet behind a 56 px peek.
  const phone = await newPage({ width: 390, height: 844 });
  await phone.goto(root);
  await settle(phone, ': 7 entities');
  const peek = phone.getByRole('button', { name: /^Explored from Policy KT-9030 \(auto\)/ });
  assert.equal((await peek.boundingBox()).height, 56);
  await peek.click();
  await phone.getByRole('dialog').getByRole('region', { name: 'Explored from Policy KT-9030 (auto)' }).waitFor();
  await phone.keyboard.press('Escape');
  await phone.goto(`${root}&sel=ex-car`);
  await settle(phone, ': 7 entities');
  const more = inspector(phone).getByRole('button', { name: 'Show more links' });
  assert.equal((await more.boundingBox()).height, 44);
  await more.click();
  await phone.waitForURL(url => url.searchParams.get('x') === 'ex-car');
  await settle(phone, ': 11 entities');
  assert.equal(await inspector(phone).getByRole('button', { name: 'Collapse' }).evaluate(element => element.getBoundingClientRect().height), 44);
  await phone.screenshot({ path: path.join(artifacts, 'explore-phone.png') });

  assert.deepEqual(errors, []);
  console.log('Explore browser checks passed');
} finally {
  await browser.close();
}
