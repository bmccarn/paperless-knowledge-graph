/** Shared filters on Focus and Table, URL history, record recomputation and responsive sheets. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
const base = process.env.UI_TEST_BASE_URL;
assert.ok(base && ['localhost', '127.0.0.1'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-filters-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch();
const errors = [];
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/graph/node/nb-renewal?summary=1', async route => {
    const response = await route.fetch();
    const summary = await response.json();
    const [stated, inferred] = summary.links[0].records;
    summary.documents[stated.doc_id] = { title: 'Current statement', date: '2026-01-10', doc_type: 'insurance' };
    summary.documents[inferred.doc_id] = { title: 'Previous statement', date: '2025-12-01', doc_type: 'work' };
    inferred.confidence = null;
    await route.fulfill({ json: summary });
  });
  await page.goto(`${base}/graph?focus=nb-renewal&view=table`);
  const table = page.getByRole('table', { name: 'Links of Renewal notice 2026' });
  await table.waitFor();
  const history = await page.evaluate(() => window.history.length);
  await page.getByRole('button', { name: 'Filters', exact: true }).filter({ visible: true }).click();
  const sheet = page.getByRole('dialog', { name: 'Filters', exact: true });
  await sheet.getByLabel('From', { exact: true }).fill('2026-01');
  await page.waitForURL(url => url.searchParams.get('from') === '2026-01');
  assert.equal(await page.evaluate(() => window.history.length), history, 'Filters replace the current history entry');
  await sheet.getByRole('button', { name: 'Show 1 links', exact: true }).click();
  assert.equal(await table.locator('tbody tr[data-link-key] td').nth(6).innerText(), 'Supported');
  assert.equal(await table.locator('tbody tr[data-link-key] td').nth(7).innerText(), '1');
  assert.equal(await table.locator('tbody tr[data-link-key] td').nth(8).innerText(), '0.90');
  await page.reload();
  await page.getByRole('group', { name: 'Active filters' }).getByRole('button', { name: 'Remove From 2026-01' }).waitFor();
  await page.getByRole('button', { name: 'Filters', exact: false }).filter({ visible: true }).first().click();
  assert.equal(await sheet.getByLabel('From', { exact: true }).inputValue(), '2026-01');
  await sheet.getByLabel('From', { exact: true }).fill('2027-01');
  await sheet.getByRole('button', { name: 'Show 0 links', exact: true }).click();
  await page.getByText('Your filters hide all 1 links.', { exact: true }).waitFor();
  await page.getByRole('group', { name: 'Active filters' }).getByRole('button', { name: /^Remove From/ }).click();
  await table.locator('tbody tr[data-link-key]').waitFor();

  // Each control writes its own URL field; the sheet stays open as counts change.
  await page.getByRole('button', { name: 'Filters', exact: true }).filter({ visible: true }).click();
  await sheet.getByRole('radio', { name: /^Supported only/ }).click();
  await page.waitForURL(url => url.searchParams.get('support') === 'supported');
  await sheet.getByLabel('Minimum', { exact: false }).focus();
  await page.keyboard.press('Home');
  for (let index = 0; index < 14; index++) await page.keyboard.press('ArrowRight');
  await page.waitForURL(url => url.searchParams.get('minconf') === '0.70');
  assert.ok(await sheet.getByRole('button', { name: 'Show 1 links', exact: true }).isVisible(), 'No score remains included');
  await sheet.getByRole('checkbox', { name: /^Include links with no score/ }).click();
  await page.waitForURL(url => url.searchParams.get('noscore') === '0');
  await sheet.getByRole('button', { name: 'Show 0 links', exact: true }).waitFor();
  await sheet.getByRole('button', { name: 'Reset filters', exact: true }).click();
  await sheet.getByRole('button', { name: /^At least 2 documents/ }).click();
  await page.waitForURL(url => url.searchParams.get('mindocs') === '2');
  await sheet.getByRole('button', { name: /^Work / }).click();
  await page.waitForURL(url => url.searchParams.has('dtype'));
  await sheet.getByLabel('To', { exact: true }).fill('2026-11');
  await page.waitForURL(url => url.searchParams.get('to') === '2026-11');
  await sheet.getByRole('checkbox', { name: /^Hide the owner entity/ }).click();
  await page.waitForURL(url => url.searchParams.get('owner') === 'hide');
  await sheet.getByRole('checkbox', { name: /^Coverage and obligations/ }).click();
  await page.waitForURL(url => url.searchParams.has('fam'));
  await page.reload();
  await page.getByRole('group', { name: 'Active filters' }).getByRole('button', { name: 'Remove Owner hidden' }).waitFor();

  // An unlisted view still renders Focus and must use Focus's enabled filters.
  await page.goto(`${base}/graph?focus=nb-renewal&view=overview&from=2030-01`);
  await page.getByText('Your filters hide all 1 links.', { exact: true }).waitFor();
  await page.getByRole('button', { name: /^Filters/ }).filter({ visible: true }).click();
  assert.equal(await sheet.getByRole('group', { name: 'Document date', exact: true }).getAttribute('disabled'), null);
  await sheet.getByRole('button', { name: 'Close', exact: true }).click();

  // Cross-links use the family predicate too; the count announces the remaining links.
  await page.goto(`${base}/graph?focus=nb-rowan&fam=parties`);
  await page.getByRole('status').filter({ hasText: '1 link between neighbors.' }).waitFor();
  await page.getByRole('button', { name: 'Links between neighbors', exact: true }).waitFor();

  await page.route('**/api/graph/subgraph?**', async route => {
    const response = await route.fetch();
    await route.fulfill({ json: { ...await response.json(), truncated: true, total: 700 } });
  });
  await page.goto(`${base}/graph?focus=nb-rowan&support=supported`);
  await page.getByText(/Filters apply to the 600 strongest links between neighbors/).waitFor();
  await page.unroute('**/api/graph/subgraph?**');

  for (const width of [1440, 390]) for (const theme of ['dark', 'light']) {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    await page.addInitScript(theme => localStorage.setItem('theme', theme), theme);
    await page.goto(`${base}/graph?focus=nb-rowan&support=supported&minconf=0.70&from=2026-01`);
    await page.getByRole('group', { name: 'Active filters' }).waitFor();
    await page.getByRole('button', { name: /^Filters/ }).filter({ visible: true }).click();
    await sheet.waitFor();
    await page.evaluate(() => document.fonts.ready);
    await page.addStyleTag({ content: '*, *::before, *::after { animation: none !important; transition: none !important; }' });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    const box = await sheet.boundingBox();
    assert.ok(box.x === 0 && box.height === (width === 390 ? 844 : 900));
    if (width === 390) assert.equal(box.width, 390);
    await page.screenshot({ path: path.join(artifacts, `filters-${width}-${theme}.png`) });
    await sheet.getByRole('button', { name: /^Show \d+ links$/ }).click();
    await page.screenshot({ path: path.join(artifacts, `filtered-focus-${width}-${theme}.png`) });
    await page.keyboard.press('t');
    await page.getByRole('table', { name: 'Links of Rowan Vale' }).waitFor();
    assert.equal(new URL(page.url()).searchParams.get('from'), '2026-01');
    await page.screenshot({ path: path.join(artifacts, `filtered-table-${width}-${theme}.png`) });
  }
  await page.goto(`${base}/gallery`);
  await page.getByRole('group', { name: 'Overview filters' }).getByRole('button', { name: /^Filters/ }).click();
  await sheet.getByText('The map does not use document dates, types or confidence, and leaves out the owner.', { exact: true }).waitFor();
  for (const name of ['Confidence', 'Document date', 'Document type', 'Owner']) {
    assert.equal(await sheet.getByRole('group', { name, exact: true }).getAttribute('disabled'), '');
  }
  assert.deepEqual(errors, []);
  console.log('Graph filters browser checks passed');
} finally { await browser.close(); }
