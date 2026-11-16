/** Overview: group selection in the URL, the top entities, the sortable list, the phone sheet, the states and the timings. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
const base = process.env.UI_TEST_BASE_URL;
assert.ok(base && ['localhost', '127.0.0.1'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-overview-ui');
await mkdir(artifacts, { recursive: true });

/** A seeded archive with preferential attachment: a few entities with many links, most with one or two. */
function synthetic(entities, pairs) {
  let state = 7;
  const random = () => ((state = (state * 1664525 + 1013904223) >>> 0) / 4294967296);
  const labels = ['Person', 'Organization', 'Address', 'FinancialItem', 'Contract', 'InsurancePolicy', 'Product', 'Event', 'Condition', 'Location'];
  const nodes = Array.from({ length: entities }, (_, at) => [`s-${String(at).padStart(6, '0')}`, labels[Math.floor(random() * labels.length)], `Entity ${at}`, 1 + Math.floor(random() ** 3 * 40), 1]);
  const seen = new Set(), links = [], ends = [];
  while (links.length < pairs) {
    const a = Math.floor(random() * entities);
    const b = ends.length && random() < 0.6 ? ends[Math.floor(random() * ends.length)] : Math.floor(random() * entities);
    const [source, target] = a < b ? [a, b] : [b, a];
    if (a === b || seen.has(`${source}-${target}`)) continue;
    seen.add(`${source}-${target}`); ends.push(a, b);
    links.push([source, target, 1 + Math.floor(random() * 4), random() < 0.2 ? 2 : 0, 1 << Math.floor(random() * 10)]);
  }
  links.sort((p, q) => p[0] - q[0] || p[1] - q[1]);
  return { version: `synthetic-${entities}-${pairs}`, columns: ['id', 'label', 'name', 'evidence_doc_count', 'link_count'], nodes,
    link_columns: ['source', 'target', 'weight', 'support', 'families'], links, owner: null, unlinked: 0, truncated: false };
}

const browser = await chromium.launch();
const errors = [];
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.setDefaultTimeout(15000);
  page.on('pageerror', error => errors.push(error.message));

  // Selecting a group writes sel=grp:<id> and lists its 10 entities with the most evidence documents.
  await page.goto(`${base}/graph?view=overview`);
  await page.getByRole('img', { name: /^Map of the archive: 277 entities in 15 groups and 18 small groups\.$/ }).waitFor();
  await page.getByRole('status').filter({ hasText: '412 entities link only to documents and 12 only to the owner. The owner is hidden.' }).waitFor();
  await page.setViewportSize({ width: 1440, height: 600 });
  await page.getByRole('button', { name: 'Show as list' }).click();
  const table = page.getByRole('table', { name: 'Groups of the archive' });
  const sizes = async () => (await table.locator('tbody tr td:nth-child(2)').allInnerTexts()).map(Number);
  const descending = await sizes();
  assert.equal(descending.length, 15);
  assert.deepEqual(descending, [...descending].sort((a, b) => b - a), 'the list opens largest first');
  // The list scrolls inside the page frame: the last group is reachable on a short screen.
  const list = await page.getByRole('region', { name: 'Groups' }).evaluate(section => ({
    bottom: section.getBoundingClientRect().bottom, scrolls: section.scrollHeight > section.clientHeight }));
  assert.ok(list.bottom <= 600 && list.scrolls, `the list scrolls inside the window: ${JSON.stringify(list)}`);
  await page.setViewportSize({ width: 1440, height: 900 });
  await table.getByRole('button', { name: 'Entities' }).click();
  assert.equal(await table.getByRole('columnheader', { name: 'Entities' }).getAttribute('aria-sort'), 'ascending');
  assert.deepEqual(await sizes(), [...descending].reverse());
  const history = await page.evaluate(() => window.history.length);
  await table.getByRole('button', { name: 'Harbor Mutual Insurance', exact: true }).click();
  await page.waitForURL(url => url.searchParams.get('sel') === 'grp:nb-money-0');
  assert.equal(await page.evaluate(() => window.history.length), history, 'a selection replaces the history entry, as on Focus');
  const group = page.getByRole('region', { name: 'Inspector: group' });
  await group.getByRole('heading', { name: 'Harbor Mutual Insurance' }).waitFor();
  const top = group.getByRole('list', { name: 'The 10 entities with the most evidence documents' }).getByRole('listitem');
  assert.equal(await top.count(), 10);
  const docs = (await top.allInnerTexts()).map(text => Number(/(\d+) docs?/.exec(text)[1]));
  assert.deepEqual(docs, [...docs].sort((a, b) => b - a));
  await page.getByRole('button', { name: 'Show as list' }).click();

  // A top entity selects it on the map; its group link goes back; Open in Focus leaves Overview.
  await top.filter({ hasText: 'Vehicle 1014' }).getByRole('button').click();
  await page.waitForURL(url => !url.searchParams.get('sel')?.startsWith('grp:'));
  const entity = page.getByRole('region', { name: 'Inspector: entity' });
  await entity.getByRole('heading', { name: 'Vehicle 1014' }).waitFor();
  await entity.getByRole('button', { name: 'Harbor Mutual Insurance' }).click();
  await page.waitForURL(url => url.searchParams.get('sel') === 'grp:nb-money-0');
  await group.getByRole('heading', { name: 'Harbor Mutual Insurance' }).waitFor();
  await page.screenshot({ path: path.join(artifacts, 'group-1440.png') });

  // A stale group key selects nothing; the map still draws.
  await page.goto(`${base}/graph?view=overview&sel=grp:nb-gone`);
  await page.getByRole('img', { name: /^Map of the archive:/ }).waitFor();
  assert.equal(await page.getByRole('region', { name: /^Inspector:/ }).count(), 0);

  // Filters regroup the map; a filter that hides every link offers a reset.
  await page.goto(`${base}/graph?view=overview&fam=places`);
  await page.getByRole('status').filter({ hasText: /Your filters hide the links of \d+ entities\./ }).waitFor();
  await page.goto(`${base}/graph?view=overview&fam=doclinks`);
  await page.getByText('Your filters hide every link on the map.', { exact: true }).waitFor();
  await page.getByRole('button', { name: 'Reset filters' }).last().click();
  await page.getByRole('img', { name: /^Map of the archive: 277 entities/ }).waitFor();

  // Empty, load error with Retry, and an arranging error.
  await page.route('**/api/graph/overview', route => route.fulfill({ json: { ...synthetic(1, 0), nodes: [], unlinked: 5 } }));
  await page.goto(`${base}/graph?view=overview`);
  await page.getByText('No links between entities yet.', { exact: true }).waitFor();
  await page.getByText('Run a sync, then come back. 5 entities link only to documents.', { exact: true }).waitFor();
  await page.getByRole('link', { name: 'Search entities' }).waitFor();
  await page.unroute('**/api/graph/overview');
  let fail = true;
  await page.route('**/api/graph/overview', route => (fail ? route.fulfill({ status: 503, json: { detail: 'Neo4j is down' } }) : route.fallback()));
  await page.goto(`${base}/graph?view=overview`);
  await page.getByText('The archive map did not load.', { exact: true }).waitFor();
  fail = false;
  await page.getByRole('button', { name: 'Retry' }).click();
  await page.getByRole('img', { name: /^Map of the archive:/ }).waitFor();
  await page.unroute('**/api/graph/overview');
  await page.route('**/api/graph/overview', route => route.fulfill({ json: { ...synthetic(10, 9), links: [[0, 99, 1, 0, 1]] } }));
  await page.goto(`${base}/graph?view=overview`);
  await page.getByText('The archive map could not be arranged.', { exact: true }).waitFor();
  await page.getByText('A link names node 0 or 99, outside the 10 nodes.', { exact: true }).waitFor();
  await page.unroute('**/api/graph/overview');

  // Phone: no canvas; a group row opens a sheet, and a row there opens Focus.
  const phone = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });
  phone.setDefaultTimeout(15000);
  phone.on('pageerror', error => errors.push(error.message));
  await phone.goto(`${base}/graph?view=overview`);
  const rows = phone.getByRole('list', { name: 'Groups' }).getByRole('button');
  await rows.first().waitFor();
  assert.equal(await phone.getByRole('img', { name: /^Map of the archive:/ }).count(), 0);
  assert.equal(await rows.count(), 15);
  assert.ok((await rows.first().boundingBox()).height >= 56);
  await rows.filter({ hasText: 'Harbor Mutual Insurance' }).click();
  const sheet = phone.getByRole('dialog', { name: 'Harbor Mutual Insurance' });
  await sheet.getByText('47 entities and 53 links. Open one in Focus.', { exact: true }).waitFor();
  assert.equal(await sheet.getByRole('list', { name: 'Most documented' }).getByRole('button').count(), 10);
  await phone.screenshot({ path: path.join(artifacts, 'sheet-390.png') });
  await sheet.getByRole('button', { name: /^Vehicle 1014/ }).click();
  await phone.waitForURL(url => !url.searchParams.has('view') && url.searchParams.has('focus'));
  await phone.close();

  // Timings on large synthetic archives, reported for the thresholds (worker 1.5 s, 30 fps, response 1.5 MB).
  for (const [count, pairs] of [[2500, 2300], [3500, 3100], [8000, 6000]]) {
    const body = synthetic(count, pairs);
    const size = Buffer.byteLength(JSON.stringify(body));
    await page.route('**/api/graph/overview', route => route.fulfill({ json: body }));
    await page.goto(`${base}/graph?view=overview`);
    await page.getByRole('img', { name: /^Map of the archive:/ }).waitFor({ timeout: 30000 });
    const arrange = await page.evaluate(() => performance.getEntriesByName('overview-arrange').at(-1)?.duration);
    const canvas = await page.getByRole('img', { name: /^Map of the archive:/ }).boundingBox();
    await page.mouse.move(canvas.x + canvas.width / 2, canvas.y + canvas.height / 2);
    const frames = page.evaluate(() => new Promise(resolve => {
      let count = 0; const start = performance.now();
      const step = now => { count++; if (now - start < 1000) requestAnimationFrame(step); else resolve(count * 1000 / (now - start)); };
      requestAnimationFrame(step);
    }));
    for (let at = 0; at < 10; at++) { await page.mouse.wheel(0, at < 5 ? -120 : 120); await page.waitForTimeout(80); }
    const fps = await frames;
    const name = await page.getByRole('img', { name: /^Map of the archive:/ }).getAttribute('aria-label');
    console.log(`overview timing: ${count} entities, ${body.links.length} pairs, ${(size / 1e6).toFixed(2)} MB, arrange ${Math.round(arrange)} ms, ${Math.round(fps)} fps while zooming; ${name}`);
    assert.ok(Number.isFinite(arrange), 'the page records the overview-arrange measure');
    await page.unroute('**/api/graph/overview');
  }
  assert.deepEqual(errors, []);
  console.log('Graph overview browser checks passed');
} finally { await browser.close(); }
