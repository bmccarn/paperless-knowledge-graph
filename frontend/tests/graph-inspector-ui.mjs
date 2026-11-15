/** The evidence shelf, the entity inspector, the link summary, the leaf state and the legend at /graph?focus=<id>. */
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { chromium } from 'playwright';

const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3002';
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
let browser;
before(async () => { browser = await chromium.launch(); });
after(async () => { await browser.close(); });

/** A page that records canvas text positions, and each link stroke's end point and color, in screen pixels. */
async function newPage(t, viewport = { width: 1440, height: 900 }) {
  const context = await browser.newContext({ viewport });
  t.after(() => context.close());
  await context.addInitScript(() => {
    window.__frame = {};
    window.__strokes = [];
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
      if (this.canvas.isConnected && !args.length && ends.has(this)) window.__strokes.push({ end: ends.get(this), color: this.strokeStyle });
      return stroke.apply(this, args);
    };
    const fillText = proto.fillText;
    proto.fillText = function (text, ...rest) {
      if (this.canvas.isConnected) window.__frame[text] = screen(this, 0, 0);
      return fillText.call(this, text, ...rest);
    };
  });
  const page = await context.newPage();
  page.setDefaultTimeout(10000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  t.after(() => assert.deepEqual(errors, []));
  return page;
}

/** Any CSS color as "r,g,b", resolved by the browser through a 1 x 1 canvas. */
const rgb = (page, color) => page.evaluate(color => {
  const probe = document.createElement('div');
  probe.style.color = color;
  document.body.append(probe);
  const context = Object.assign(document.createElement('canvas'), { width: 1, height: 1 }).getContext('2d');
  context.fillStyle = getComputedStyle(probe).color;
  probe.remove();
  context.fillRect(0, 0, 1, 1);
  return [...context.getImageData(0, 0, 1, 1).data].slice(0, 3).join(',');
}, color);

async function open(page, focus, ready) {
  await page.goto(`${base}/graph?focus=${focus}`);
  await page.getByText(ready, { exact: false }).first().waitFor();
}
/** Wait until the first fit has run and the paint stays still. */
async function settled(page) {
  await page.locator('canvas').first().waitFor({ state: 'visible' });
  await page.waitForFunction(() => {
    const zoom = document.querySelector('canvas')?.__zoom;
    if (!zoom || zoom.k === 1 || !Object.keys(window.__frame).length) return false;
    const signature = JSON.stringify([zoom, Object.keys(window.__frame).length]);
    if (window.__still?.signature !== signature) window.__still = { signature, since: Date.now() };
    return Date.now() - window.__still.since > 300;
  });
}
const inspector = page => page.getByRole('complementary', { name: 'Evidence inspector' });
const shelfOf = page => page.getByRole('region', { name: /^Evidence/ });
const summaryOf = async (page, focus) => (await page.request.get(`${base}/api/graph/node/${focus}?summary=1`)).json();
const hubReady = 'Rowan Vale: 105 links in 9 families.';
const deskReady = 'Front desk: 3 links in 3 families.';

test('hovering or focusing a shelf chip marks the links that document supports', async t => {
  const page = await newPage(t);
  assert.equal((await (await page.request.get(`${base}/api/_fixture`)).json()).fixture, 'paperless-accuracy-ui-v1');
  await open(page, 'nb-desk', deskReady);
  await settled(page);
  const shelf = shelfOf(page);
  assert.equal(await shelf.getByRole('heading').innerText(), 'Evidence · 6 documents');
  const mark = await rgb(page, 'var(--mark)');
  /** The labels of nodes where a stroke in the --mark color ends. */
  const marked = async () => {
    const frame = await page.evaluate(() => window.__frame);
    const strokes = await page.evaluate(() => window.__strokes);
    const colors = await Promise.all(strokes.map(item => rgb(page, item.color)));
    return Object.entries(frame).filter(([, at]) => strokes.some((item, index) => colors[index] === mark
      && Math.hypot(item.end.x - at.x, item.end.y - at.y) < 1)).map(([text]) => text);
  };
  assert.deepEqual(await marked(), [], 'No link is marked before a chip is hovered');
  const desk = await summaryOf(page, 'nb-desk');
  const chip = shelf.getByRole('link').first();
  const docId = Number((await chip.getAttribute('href')).split('/').pop());
  const supported = desk.links.filter(link => link.records.some(record => record.doc_id === docId)).map(link => link.neighbor.name);
  assert.equal(supported.length, 1);
  await chip.hover();
  await page.waitForTimeout(300);
  assert.deepEqual(await marked(), supported, 'Hovering a chip marks the link it supports');
  await page.mouse.move(700, 300);
  await page.waitForTimeout(300);
  assert.deepEqual(await marked(), []);
  await chip.focus();
  await page.waitForTimeout(300);
  assert.deepEqual(await marked(), supported, 'Focusing a chip marks the link it supports');
});

test('the inspector lists documents by title and date, never "Document #"', async t => {
  const page = await newPage(t);
  await open(page, 'nb-rowan', hubReady);
  const panel = inspector(page);
  const mentioned = panel.getByRole('heading', { name: 'Mentioned in' }).locator('xpath=following-sibling::ul[1]');
  assert.deepEqual((await mentioned.getByRole('listitem').allInnerTexts()).map(text => text.replace(/\s+/g, ' ')).slice(0, 2),
    ['Synthetic archive 000 Invoice 1 Sep 2026', 'Synthetic archive 001 Medical 1 Sep 2026']);
  // A selected neighbor and a selected link also name documents and entities, not ids.
  await panel.getByRole('button', { name: /^Marlow Freight Co\./ }).click();
  await panel.getByText('Link in Parties and roles', { exact: true }).waitFor();
  assert.match(await panel.getByRole('link').first().innerText(), /^Synthetic archive \d+/);
  assert.ok(!(await page.textContent('body')).includes('Document #'), 'The text "Document #" does not appear');
});

test('"Show all" for the hub\'s parties family lists every parties link', async t => {
  const page = await newPage(t);
  await open(page, 'nb-rowan', hubReady);
  const hub = await summaryOf(page, 'nb-rowan');
  const parties = hub.links.filter(link => link.family === 'parties' && link.neighbor.label !== 'Document').length;
  assert.equal(parties, 25);
  const panel = inspector(page);
  const list = panel.getByRole('list', { name: 'Parties and roles links' });
  assert.equal(await list.getByRole('listitem').count(), 6);
  await panel.getByRole('button', { name: `Show all ${parties}`, exact: true }).click();
  assert.equal(await list.getByRole('listitem').count(), parties);
});

test('a pill lists its hidden links; "Add" and Shift+click put them on the canvas', async t => {
  const page = await newPage(t);
  await open(page, 'nb-rowan', hubReady);
  await settled(page);
  const box = await page.locator('canvas').first().boundingBox();
  const pill = (await page.evaluate(() => window.__frame))['+23 Parties and roles'];
  // The renderer refreshes its hit canvas at most every 800 ms.
  await page.waitForTimeout(900);
  await page.mouse.click(box.x + pill.x, box.y + pill.y);
  const panel = inspector(page);
  const group = panel.getByRole('list', { name: 'Parties and roles group' });
  await group.waitFor();
  assert.equal(await group.getByRole('listitem').count(), 10);
  await panel.getByRole('button', { name: 'Show all 23', exact: true }).click();
  assert.equal(await group.getByRole('listitem').count(), 23);
  assert.ok((await page.evaluate(() => window.__frame))['+23 Parties and roles'], 'Selecting a pill adds nothing');
  // Harbor Mutual's parties link stays folded: it is drawn once, in money.
  await panel.getByRole('button', { name: 'Add 22 to the canvas', exact: true }).click();
  await page.waitForFunction(() => window.__frame['+1 Parties and roles']);
});

test('the owner\'s other matches are left out of its families and listed as possible duplicates', async t => {
  const page = await newPage(t);
  await open(page, 'nb-rowan', 'Rowan Vale: ');
  const hub = await summaryOf(page, 'nb-rowan');
  assert.deepEqual(hub.owner_duplicates.map(item => item.id), ['nb-rowan-dup-1', 'nb-rowan-dup-2']);
  assert.ok(hub.links.some(link => link.neighbor.id === 'nb-rowan-dup-1'), 'One other match is linked to the owner');
  await page.getByText(hubReady, { exact: false }).waitFor();
  const panel = inspector(page);
  await panel.getByRole('button', { name: /^Loose association/ }).click();
  assert.ok(!(await panel.getByRole('list', { name: 'Loose association links' }).innerText()).includes('Rowan Vale'));
  const duplicates = panel.getByRole('list', { name: 'Possible duplicates of this entity' });
  assert.deepEqual((await duplicates.getByRole('listitem').allInnerTexts()).map(text => text.replace(/\s+/g, ' ')),
    ['Rowan Vale 18 documents', 'Rowan Vale 11 documents']);
  assert.equal(await duplicates.getByRole('link').first().getAttribute('href'), '/graph?focus=nb-rowan-dup-1');
  assert.equal(await panel.getByRole('link', { name: 'Review on Entities', exact: true }).getAttribute('href'), '/entities/review');
});

test('"Ask about this" opens Ask with the question in the composer and sends nothing', async t => {
  const page = await newPage(t);
  await open(page, 'nb-rowan', hubReady);
  const writes = [];
  page.on('request', request => { if (request.method() !== 'GET') writes.push(request.url()); });
  await inspector(page).getByRole('link', { name: 'Ask about this', exact: true }).click();
  await page.waitForURL(url => url.pathname === '/query');
  await page.waitForFunction(() => document.querySelector('textarea')?.value === 'What do my documents say about Rowan Vale?');
  await page.waitForTimeout(500);
  assert.deepEqual(writes, [], 'Nothing is sent');
});

test('selecting the mixed link shows "Stated in 1, inferred in 1" and its documents', async t => {
  const page = await newPage(t);
  await open(page, 'nb-renewal', 'Renewal notice 2026: 1 links in 1 families.');
  await settled(page);
  await inspector(page).getByRole('button', { name: /^2014 Honda Fit/ }).click();
  const link = inspector(page).getByRole('region', { name: 'Inspector: link' });
  await link.getByText('Stated in 1, inferred in 1', { exact: true }).waitFor();
  assert.equal(await link.getByText('Link in Coverage and obligations', { exact: true }).count(), 1);
  assert.equal(await link.getByRole('link').count(), 2);
  assert.match(await link.getByRole('link').first().innerText(), /^Synthetic archive \d+\s+\S+\s+1 Sep 2026$/);
  // The shelf marks the selected link's documents.
  assert.equal(await shelfOf(page).locator('[data-marked]').count(), 2);
});

test('the fixture leaf shows its empty state and its documents', async t => {
  const page = await newPage(t);
  await open(page, 'nb-leaf', 'No direct links extracted. This entity appears in 2 documents.');
  assert.equal(await page.locator('canvas').count(), 0);
  assert.equal(await shelfOf(page).getByRole('link').count(), 2);
});

test('on a phone the leaf shows its inspector without a selection', async t => {
  const page = await newPage(t, { width: 390, height: 844 });
  await open(page, 'nb-leaf', 'No direct links extracted. This entity appears in 2 documents.');
  await page.getByRole('complementary', { name: 'Evidence inspector' }).getByRole('link', { name: 'Ask about this' }).waitFor();
});

test('the legend lists exactly the kinds and link styles in view, in token colors, and collapses', async t => {
  const page = await newPage(t);
  const legend = page.getByRole('region', { name: 'Legend' });
  const contents = async () => ({
    kinds: await legend.getByRole('list', { name: 'Kinds' }).getByRole('listitem').allInnerTexts(),
    links: await legend.getByRole('list', { name: 'Links' }).getByRole('listitem').allInnerTexts(),
  });
  await open(page, 'nb-desk', deskReady);
  const toggle = page.getByRole('button', { name: 'Legend', exact: true });
  assert.equal(await toggle.getAttribute('aria-expanded'), 'false');
  assert.equal(await legend.count(), 0, 'The legend starts collapsed on desktop');
  await toggle.click();
  assert.equal(await toggle.getAttribute('aria-expanded'), 'true');
  assert.deepEqual(await contents(), { kinds: ['Organization', 'Other'], links: ['Supported'] });
  await toggle.click();
  assert.equal(await toggle.getAttribute('aria-expanded'), 'false');
  assert.equal(await legend.count(), 0, 'Collapsing hides the legend');
  await toggle.click();

  // The open state is remembered.
  await open(page, 'nb-rowan', hubReady);
  assert.equal(await toggle.getAttribute('aria-expanded'), 'true');
  assert.deepEqual(await contents(), {
    kinds: ['Person', 'Organization', 'Location', 'Address', 'Financial item', 'Insurance policy', 'Product', 'System',
      'Condition', 'Medical result', 'Event', 'Date event'],
    links: ['Supported', 'Inferred', 'Hidden group'],
  });
  for (const theme of ['dark', 'light']) {
    if (theme === 'light') {
      await page.getByRole('button', { name: 'Toggle theme', exact: true }).click();
      await page.waitForFunction(() => !document.documentElement.classList.contains('dark'));
    }
    const swatches = await legend.getByRole('list', { name: 'Kinds' }).locator('[data-kind]').evaluateAll(items => items.map(item => {
      const style = getComputedStyle(item.querySelector('path'));
      return { kind: item.dataset.kind, color: item.dataset.outlined || item.dataset.kind === 'unknown' ? style.stroke : style.fill };
    }));
    assert.equal(swatches.length, 12);
    for (const { kind, color } of swatches)
      assert.equal(await rgb(page, color), await rgb(page, `var(--cat-${kind})`), `${theme}: the ${kind} swatch equals its token`);
  }
});

test('on a phone the legend is a sheet and the inspector opens on selection', async t => {
  const page = await newPage(t, { width: 390, height: 844 });
  await open(page, 'nb-desk', deskReady);
  assert.equal(await inspector(page).count(), 0, 'No inspector until something is selected');
  await page.getByRole('button', { name: 'Legend', exact: true }).click();
  const sheet = page.getByRole('dialog', { name: 'Legend' });
  await sheet.waitFor();
  assert.deepEqual(await sheet.getByRole('list', { name: 'Kinds' }).getByRole('listitem').allInnerTexts(), ['Organization', 'Other']);
  await page.keyboard.press('Escape');
  await settled(page);
  const box = await page.locator('canvas').first().boundingBox();
  const zoom = await page.locator('canvas').first().evaluate(element => element.__zoom);
  await page.waitForTimeout(900);
  await page.mouse.click(box.x + zoom.x, box.y + zoom.y);
  await inspector(page).getByRole('heading', { name: 'Front desk' }).waitFor();
});
