/** Timeline source marks, hub caps, range history and phone list against the synthetic backend. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
const base = process.env.UI_TEST_BASE_URL;
assert.ok(base && ['localhost', '127.0.0.1'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-timeline-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch();
const errors = [];
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.message));
  const calls = [];
  page.on('request', request => { if (request.url().includes('/api/graph/')) calls.push(request.url()); });
  await page.goto(`${base}/graph?focus=nb-timeline&view=timeline`);
  const timeline = page.getByRole('region', { name: 'Timeline of Policy TL-2208 (auto)', exact: true });
  await timeline.waitFor();
  assert.equal(calls.some(url => url.includes('/subgraph')), false, 'Timeline needs only the summary');
  const tracks = timeline.locator('[data-timeline-link]');
  assert.equal(await tracks.count(), 3);
  const marks = timeline.locator('[data-document-ids]');
  assert.equal(await marks.count(), 6, 'Each link draws one mark per supporting document');
  assert.equal(await timeline.locator('[data-month="2024-03"]').count(), 3, 'Parsed named date lands in March');
  assert.equal(await timeline.locator('[data-month="no-date"]').count(), 2, 'Unparsed and undated documents use No date');
  assert.equal(await timeline.locator('[data-inferred="true"]').count(), 3);
  const row = timeline.getByRole('button', { name: /^Harbor Mutual Insurance, outgoing issued by link, 3 documents/ });
  await row.waitFor();
  const positions = await timeline.locator('[data-month="2024-03"]').evaluateAll(nodes => nodes.map(n => n.querySelector('circle').getAttribute('cx')));
  assert.notEqual(positions[0], positions[1], 'Two documents in one month sit side by side');
  const before = await page.evaluate(() => history.length);
  const axis = timeline.getByRole('img', { name: 'Drag across the axis to set the document date range' });
  const box = await axis.boundingBox();
  await page.mouse.move(box.x + box.width * .40, box.y + 30);
  await page.mouse.down();
  await page.mouse.move(box.x + box.width * .90, box.y + 30, { steps: 8 });
  await page.mouse.up();
  await page.waitForURL(url => url.searchParams.has('from') && url.searchParams.has('to'));
  const range = new URL(page.url()).searchParams;
  assert.equal(await page.evaluate(() => history.length), before, 'Dragging replaces history');
  await page.keyboard.press('1');
  await page.getByText(/1 of 3 links match/).waitFor();
  assert.equal(new URL(page.url()).searchParams.get('from'), range.get('from'));
  await page.keyboard.press('3');
  await timeline.waitFor();
  await page.getByRole('group', { name: 'Timeline date range' }).getByLabel('From', { exact: true }).fill('2026-01');
  await page.waitForURL(url => url.searchParams.get('from') === '2026-01');
  await page.reload();
  await timeline.waitFor();
  assert.equal(await page.getByRole('group', { name: 'Timeline date range' }).getByLabel('From', { exact: true }).inputValue(), '2026-01');
  await page.goto(`${base}/graph?focus=nb-timeline&view=timeline`);
  await timeline.waitFor();
  await timeline.locator('[data-month="no-date"]').first().focus();
  await page.keyboard.press('Enter');
  await page.getByRole('region', { name: 'Inspector: link', exact: true }).waitFor();
  assert.ok(new URL(page.url()).searchParams.get('sel').includes('~'));
  await page.mouse.move(0, 0);
  await page.getByRole('heading', { name: 'Graph', exact: true }).click();
  assert.equal(await page.locator('#evidence-shelf-title').locator('..').locator('a[data-marked]').count(), 1, 'Clicked source stays highlighted after blur and mouse leave');
  await page.goto(`${base}/graph?focus=nb-rowan&view=timeline`);
  await page.getByRole('region', { name: 'Timeline of Rowan Vale', exact: true }).waitFor();
  const olderMark = page.locator('[data-document-ids="9030"]');
  const olderBox = await olderMark.boundingBox();
  await olderMark.click({ position: { x: olderBox.width / 2 + 2, y: olderBox.height / 2 } });
  const olderChip = page.getByRole('region', { name: /^Evidence ·/ }).locator('a[data-document-id="9030"][data-marked]');
  await olderChip.waitFor();
  await page.waitForFunction(() => {
    const chip = document.querySelector('a[data-document-id="9030"][data-marked]');
    if (!chip) return false;
    const source = chip.getBoundingClientRect(), list = chip.closest('ul').getBoundingClientRect();
    return source.left >= list.left - 1 && source.right <= list.right + 1;
  });
  assert.equal(await olderChip.locator('..').locator('..').locator('a[data-document-id]').count(), 12, 'The source stays within the twelve-chip cap');
  await page.goto(`${base}/graph?focus=nb-rowan&view=timeline`);
  await page.getByRole('region', { name: 'Timeline of Rowan Vale', exact: true }).waitFor();
  const counts = await page.locator('[data-timeline-family]').evaluateAll(lanes => lanes.map(lane => lane.querySelectorAll('[data-timeline-row]').length));
  assert.ok(counts.every(count => count <= 2));
  await page.getByRole('button', { name: /^\+\d+ more parties and roles$/ }).click();
  assert.equal(await page.locator('[data-timeline-family="parties"] [data-timeline-row]').count(), 24);
  await page.goto(`${base}/graph?focus=nb-leaf&view=timeline`);
  await page.getByText('No direct links extracted. This entity appears in 2 documents.', { exact: true }).waitFor();
  const source = await (await page.request.get(`${base}/api/graph/node/nb-timeline?summary=1`)).json();
  await page.route('**/api/graph/node/nb-timeline?summary=1', route => {
    const records = Array.from({ length: 61 }, (_, i) => ({ doc_id: 9400 + i, inferred: i % 2 === 0, confidence: .9 }));
    const data = { ...source, links: [{ ...source.links[0], records, support_state: 'mixed', stated_count: 30, inferred_count: 31 }],
      documents: Object.fromEntries(records.map(record => [record.doc_id, { title: `Busy source ${record.doc_id}`, date: '2026-01-01', parsed_date: '2026-01-01', doc_type: 'insurance' }])) };
    return route.fulfill({ json: data });
  });
  await page.goto(`${base}/graph?focus=nb-timeline&view=timeline`);
  await timeline.waitFor();
  assert.equal(await timeline.locator('[data-document-ids]').count(), 1, '61 records merge to one monthly mark');
  assert.match(await timeline.locator('[data-document-ids]').getAttribute('aria-label'), /^61 documents, January 2026, Stated and inferred/);
  await page.unroute('**/api/graph/node/nb-timeline?summary=1');
  const party = source.links.find(link => link.type === 'ISSUED_BY');
  await page.route('**/api/graph/node/nb-timeline?summary=1', route => route.fulfill({ json: { ...source, links: [
    { ...party, records: [party.records[0]] },
    { ...party, key: 'nb-timeline~SIGNED_BY~nb-timeline-insurer', type: 'SIGNED_BY', records: [party.records[0]] },
  ] } }));
  await page.goto(`${base}/graph?focus=nb-timeline&view=timeline`);
  await timeline.waitFor();
  const upperMark = timeline.locator(`[data-timeline-link="${party.key}"] [data-document-ids="${party.records[0].doc_id}"]`);
  const upperBox = await upperMark.boundingBox();
  await upperMark.click({ position: { x: upperBox.width / 2, y: upperBox.height / 2 + 2 } });
  await page.waitForURL(url => url.searchParams.get('sel') === party.key);
  assert.equal(new URL(page.url()).searchParams.get('sel'), party.key, 'The next track cannot intercept a mark center');
  await page.unroute('**/api/graph/node/nb-timeline?summary=1');
  let fail = true;
  await page.route('**/api/graph/node/nb-timeline?summary=1', route => fail ? route.fulfill({ status: 503, json: { detail: 'Synthetic service unavailable' } }) : route.continue());
  await page.goto(`${base}/graph?focus=nb-timeline&view=timeline`);
  await page.getByRole('alert').filter({ hasText: 'Could not load the neighborhood' }).waitFor();
  fail = false;
  await page.getByRole('button', { name: 'Retry', exact: true }).click();
  await timeline.waitFor();
  await page.unroute('**/api/graph/node/nb-timeline?summary=1');
  for (const width of [1440, 390]) for (const theme of ['dark', 'light']) {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    await page.addInitScript(theme => localStorage.setItem('theme', theme), theme);
    await page.goto(`${base}/graph?focus=nb-timeline&view=timeline`);
    await timeline.waitFor();
    await page.evaluate(() => document.fonts.ready);
    const search = page.getByRole('combobox', { name: 'Search entities', exact: true });
    const viewControl = width === 390 ? page.getByRole('button', { name: 'View: Timeline', exact: true }) : page.getByRole('toolbar', { name: 'View', exact: true });
    const controls = await Promise.all([search.locator(".."), viewControl, page.getByRole('button', { name: 'Filters', exact: true })].map(control => control.boundingBox()));
    assert.ok(controls.every(box => box && box.x >= 0 && box.x + box.width <= width), 'Search, view and Filters fit the header');
    if (width === 1440) assert.equal(Math.round(controls[0].width), 300);
    for (let i = 0; i < controls.length; i++) for (let j = i + 1; j < controls.length; j++) {
      const a = controls[i], b = controls[j];
      assert.equal(a.x < b.x + b.width && a.x + a.width > b.x && a.y < b.y + b.height && a.y + a.height > b.y, false, 'Header controls do not overlap');
    }
    if (width === 390) {
      assert.equal(await timeline.locator('[data-timeline-desktop]').count(), 0);
      await timeline.getByRole('heading', { name: 'September 2026', exact: true }).waitFor();
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
      await page.goto(`${base}/graph?focus=nb-timeline&view=timeline&from=2026-01`);
      await timeline.getByText(/Outside date range/).first().waitFor();
      assert.equal(await timeline.locator('li[data-outside-range]').count(), 4);
      await page.goto(`${base}/graph?focus=nb-timeline&view=timeline`);
      await timeline.waitFor();
    } else await timeline.locator('[data-document-ids]').first().waitFor();
    await page.screenshot({ path: path.join(artifacts, `timeline-${width}-${theme}.png`) });
  }
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    await page.goto(`${base}/graph?focus=nb-unnamed`);
    await page.getByRole('heading', { name: 'Evidence · 2 documents', exact: true }).waitFor();
    if (width === 390) await page.getByRole('button', { name: 'View: Focus', exact: true }).click();
    const choice = (width === 390 ? page.getByRole('dialog', { name: 'View', exact: true }) : page.getByRole('toolbar', { name: 'View', exact: true })).getByRole('button', { name: width === 390 ? /^Timeline/ : 'Timeline', exact: width !== 390 });
    assert.equal(await choice.getAttribute('aria-disabled'), null, 'An unnamed focus enables Timeline');
    await choice.click();
    await page.getByRole('region', { name: 'Timeline of Organization nb-unn', exact: true }).waitFor();
  }
  await page.goto(`${base}/graph?focus=nb-leaf&view=timeline`);
  await page.getByText('No direct links extracted. This entity appears in 2 documents.', { exact: true }).waitFor();
  assert.equal(await page.getByRole('region', { name: 'Timeline of Spare key tag', exact: true }).getByRole('link').count(), 2, 'A leaf document opens its document page');
  assert.deepEqual(errors, []);
  console.log('Graph Timeline browser checks passed');
} finally { await browser.close(); }
