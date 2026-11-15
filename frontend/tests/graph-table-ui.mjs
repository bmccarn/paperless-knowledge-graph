/** Complete all-link table, shared selection, keyboard controls and the listed view on the real graph page. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
import { neighborhoodLayout, familyCap } from '../.test-build/graph-layout.js';
import { tableRows } from '../.test-build/graph-table.js';

const base = process.env.UI_TEST_BASE_URL;
assert.ok(base && ['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-table-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch();
const errors = [];
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.message));
  const hub = await (await page.request.get(`${base}/api/graph/node/nb-rowan?summary=1`)).json();
  const table = page.getByRole('table', { name: 'Links of Rowan Vale', exact: true });
  await page.goto(`${base}/graph?focus=nb-rowan&view=table`);
  await table.waitFor();
  assert.equal(await page.locator('canvas').count(), 0);
  assert.equal(await page.getByText('Rowan Vale: 105 links in 9 families.', { exact: true }).count(), 0, 'Table uses its own full link count');
  assert.equal(await page.getByRole('region', { name: /^Evidence ·/ }).count(), 0);
  const expected = tableRows(hub, neighborhoodLayout(hub, familyCap(true, false)));
  const keys = [], drawn = [];
  do {
    const rows = await table.locator('tbody tr[data-link-key]').evaluateAll(rows => rows.map(row => ({ key: row.dataset.linkKey, drawn: row.dataset.onCanvas === 'true' })));
    keys.push(...rows.map(row => row.key));
    drawn.push(...rows.filter(row => row.drawn).map(row => row.key));
    assert.equal(rows.length, keys.length <= 100 ? 50 : expected.length - 100);
    const next = page.getByRole('button', { name: 'Next page', exact: true });
    if (await next.isDisabled()) break;
    await table.locator('tbody tr').last().scrollIntoViewIfNeeded();
    await next.click();
    assert.equal(await table.evaluate(element => element.parentElement.parentElement.scrollTop), 0);
  } while (true);
  assert.deepEqual(keys.sort(), expected.map(row => row.link.key).sort());
  assert.deepEqual(drawn.sort(), expected.filter(row => row.onCanvas).map(row => row.link.key).sort());
  await page.getByRole('button', { name: 'First page', exact: true }).click();
  const documents = table.getByRole('columnheader').filter({ hasText: 'Documents' });
  await documents.getByRole('button', { name: 'Documents', exact: true }).click();
  assert.equal(await documents.getAttribute('aria-sort'), 'ascending');
  assert.equal(await table.locator('[aria-sort]').count(), 1);
  assert.equal(await table.locator('tbody tr').first().getByRole('cell').nth(7).innerText(), '1');
  const row = table.locator('tbody tr').first();
  const key = await row.getAttribute('data-link-key');
  await row.focus();
  assert.equal(await table.locator('[tabindex="0"]').count(), 1);
  await page.keyboard.press('ArrowDown');
  assert.notEqual(await page.locator(':focus').getAttribute('data-link-key'), key);
  await page.keyboard.press('ArrowUp');
  assert.equal(await page.locator(':focus').getAttribute('data-link-key'), key);
  const history = await page.evaluate(() => window.history.length);
  await page.keyboard.press('Enter');
  await page.getByRole('region', { name: 'Inspector: link' }).waitFor();
  assert.equal(await table.locator('tr[aria-current=true]').getAttribute('data-link-key'), key);
  await page.waitForFunction(() => {
    const selected = document.querySelector('tr[aria-current=true]');
    return selected && [...selected.cells].every(cell => getComputedStyle(cell).color === getComputedStyle(selected).color);
  });
  assert.equal(new URL(page.url()).searchParams.get('sel'), key);
  assert.equal(await page.evaluate(() => window.history.length), history, 'Selecting replaces history');
  await page.getByRole('button', { name: 'Clear selection', exact: true }).click();
  await page.waitForURL(url => !url.searchParams.has('sel'));
  // Keyboard row navigation reaches sortable headers and leaves exactly one table Tab stop.
  await row.focus();
  await page.keyboard.press('ArrowUp');
  assert.equal(await page.locator(':focus').innerText(), 'Documents');
  await page.keyboard.press('ArrowRight');
  assert.equal(await page.locator(':focus').innerText(), 'Lowest confidence');
  await page.keyboard.press('Enter');
  assert.equal(await table.getByRole('columnheader').filter({ hasText: 'Lowest confidence' }).getAttribute('aria-sort'), 'descending');
  assert.equal(await table.locator('[tabindex="0"]').count(), 1);

  // Both aliases and the numbered keys push exactly one history step and preserve link selection.
  await page.goto(`${base}/graph?focus=nb-rowan&sel=${encodeURIComponent(key)}`);
  await page.locator('canvas').first().waitFor();
  const toolbar = page.getByRole('toolbar', { name: 'View', exact: true });
  assert.deepEqual(await toolbar.getByRole('button').allTextContents(), ['Focus', 'Table']);
  await toolbar.getByRole('button', { name: 'Focus', exact: true }).focus();
  await page.keyboard.press('ArrowRight');
  assert.equal(await page.locator(':focus').innerText(), 'Table');
  await page.keyboard.press('Enter');
  await table.waitFor();
  assert.equal(await page.locator(':focus').innerText(), 'Table');
  assert.equal(new URL(page.url()).searchParams.get('sel'), key);
  await page.goBack();
  await page.locator('canvas').first().waitFor();
  await page.goForward();
  await table.waitFor();
  await page.keyboard.press('n');
  await page.waitForURL(url => !url.searchParams.has('view'));
  for (const [pressed, view] of [['t', 'table'], ['n', null], ['4', 'table'], ['1', null]]) {
    const before = await page.evaluate(() => window.history.length);
    if (view === null) await table.locator('tbody tr').first().focus();
    await page.keyboard.press(pressed);
    await page.waitForURL(url => url.searchParams.get('view') === view);
    assert.equal(await page.evaluate(() => window.history.length), before + 1);
    assert.equal(new URL(page.url()).searchParams.get('sel'), key);
    assert.equal(await page.locator(':focus').getAttribute('data-graph-view'), view ?? 'focus', 'View keys keep focus on the switcher');
  }
  for (const tag of ['input', 'textarea', 'div']) {
    await page.evaluate(tag => { const field = document.createElement(tag); field.id = 'typing-guard'; if (tag === 'div') field.contentEditable = 'true'; document.body.append(field); field.focus(); }, tag);
    const url = page.url();
    await page.keyboard.press('t');
    await page.keyboard.press('n');
    await page.keyboard.press('4');
    assert.equal(page.url(), url);
    await page.locator('#typing-guard').evaluate(field => field.remove());
  }

  for (const width of [1440, 390]) for (const theme of ['dark', 'light']) {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    await page.addInitScript(theme => localStorage.setItem('theme', theme), theme);
    await page.goto(`${base}/graph?focus=nb-rowan`);
    await page.locator('canvas').first().waitFor();
    await page.addStyleTag({ content: '*, *::before, *::after { transition: none !important; animation: none !important; }' });
    if (width === 390) {
      await page.getByRole('button', { name: 'View: Focus', exact: true }).click();
      const sheet = page.getByRole('dialog', { name: 'View', exact: true });
      await sheet.waitFor();
      for (const row of await sheet.getByRole('listitem').all()) assert.ok((await row.boundingBox()).height >= 56);
      await page.screenshot({ path: path.join(artifacts, `view-sheet-${width}-${theme}.png`) });
      await sheet.getByRole('button', { name: /^Table/ }).click();
      await table.waitFor();
      await page.getByRole('button', { name: 'View: Table', exact: true }).waitFor();
    } else {
      await page.screenshot({ path: path.join(artifacts, `focus-switcher-${width}-${theme}.png`) });
      await toolbar.getByRole('button', { name: 'Table', exact: true }).click();
      await table.waitFor();
    }
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.evaluate(() => document.fonts.ready);
    await page.addStyleTag({ content: '*, *::before, *::after { transition: none !important; animation: none !important; }' });
    await page.screenshot({ path: path.join(artifacts, `table-${width}-${theme}.png`) });
    if (width === 390) {
      await table.locator('tbody tr').first().focus();
      await page.keyboard.press('n');
      await page.waitForURL(url => !url.searchParams.has('view'));
      assert.equal(await page.locator(':focus').getAttribute('data-graph-view'), 'sheet');
      await page.keyboard.press('t');
      await table.waitFor();
      assert.equal(await page.locator(':focus').getAttribute('data-graph-view'), 'sheet');
    }
  }

  // Invalid URL selections do not leave an empty inspector. A restored row is visible and the table's Tab stop.
  for (const selection of ['pill:parties', 'missing-entity']) {
    await page.goto(`${base}/graph?focus=nb-rowan&view=table&sel=${selection}`);
    await table.waitFor();
    assert.equal(await page.getByRole('complementary', { name: 'Evidence inspector' }).count(), 0);
  }
  const restoredRow = [...expected].sort((a, b) => b.documents - a.documents || a.link.key.localeCompare(b.link.key))[40];
  await page.goto(`${base}/graph?focus=nb-rowan&view=table&sel=${encodeURIComponent(restoredRow.link.key)}`);
  await table.waitFor();
  const restored = table.locator('tr[aria-current=true]');
  assert.equal(await restored.getAttribute('tabindex'), '0');
  assert.equal(await restored.getAttribute('data-link-key'), restoredRow.link.key);
  const bounds = await restored.boundingBox();
  assert.ok(bounds.y >= 0 && bounds.y + bounds.height <= 788, 'Restored selected row is above the phone tab bar');

  // Selecting the focus entity does not select one of its self-link rows.
  const self = structuredClone(hub);
  self.links.push({ ...hub.links[0], key: 'nb-rowan~ASSOCIATED_WITH~nb-rowan', family: 'loose', neighbor: hub.focus });
  await page.route('**/api/graph/node/nb-rowan?summary=1', route => route.fulfill({ json: self }));
  await page.goto(`${base}/graph?focus=nb-rowan&view=table&sel=nb-rowan`);
  await table.waitFor();
  assert.equal(await table.locator('tr[aria-current=true]').count(), 0);
  await page.unroute('**/api/graph/node/nb-rowan?summary=1');

  // At the 150-entity cap the group opens the full table instead of adding more neighbors.
  const large = structuredClone(hub);
  large.owner_duplicates = [];
  large.links = Array.from({ length: 175 }, (_, index) => ({ ...hub.links.find(link => link.type === 'EMPLOYED_BY'),
    key: `nb-rowan~EMPLOYED_BY~cap-${index}`, neighbor: { id: `cap-${index}`, label: 'Organization', name: `Synthetic cap ${index}` } }));
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.route('**/api/graph/node/nb-rowan?summary=1', route => route.fulfill({ json: large }));
  await page.goto(`${base}/graph?focus=nb-rowan&sel=pill:parties`);
  const inspector = page.getByRole('complementary', { name: 'Evidence inspector' });
  await inspector.getByRole('heading', { name: 'Parties and roles', exact: true }).waitFor();
  for (let step = 0; step < 6; step++) await inspector.getByRole('button', { name: /^Add \d+ to the canvas$/ }).click();
  await inspector.getByRole('button', { name: 'Open in table', exact: true }).click();
  await table.waitFor();
  assert.equal(await page.getByText('175 links, 0 inferred', { exact: true }).count(), 1);
  assert.equal(new URL(page.url()).searchParams.has('sel'), false, 'A pill selection cannot enter Table');
  assert.deepEqual(errors, []);
  console.log('PASS all 106 links, exact On canvas marks, sorting, row evidence, one Tab stop, view keys, text guard, phone sheet, 150-entity escape');
} finally {
  await browser.close();
}
