/** Domain-hub browser contracts against the isolated synthetic fixture. */
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3002';
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || 'docs/audits/accuracy-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
const page = await context.newPage();
const errors = [], outcomes = [];
page.on('pageerror', error => errors.push(error.message));
const visible = text => page.getByText(text, { exact: false }).first().waitFor({ state: 'visible' });
const record = name => { outcomes.push({ name, status: 'passed' }); console.log(`PASS ${name}`); };
const capture = name => page.screenshot({ path: path.join(artifacts, `${name}.png`), fullPage: true });

try {
  const marker = await context.request.get(`${base}/api/_fixture`);
  assert.equal(marker.status(), 200);
  assert.equal((await marker.json()).fixture, 'paperless-accuracy-ui-v1');
  await page.goto(`${base}/hubs`);
  await visible('303 matching indexed documents');
  await visible('1–12 of 303');
  await page.getByRole('button', { name: 'Next hub page', exact: true }).click();
  await visible('13–24 of 303');
  await page.getByRole('button', { name: 'Last hub page', exact: true }).click();
  await visible('301–303 of 303');
  await visible('Synthetic archive 300');
  await page.getByRole('button', { name: 'Medical', exact: true }).click();
  await visible('1–12 of 303');
  await page.getByRole('button', { name: 'Insurance', exact: true }).click();
  await visible('301–303 of 303');
  await capture('hubs-last-page');
  record('hubs_complete_pagination_and_domain_page');

  let failTax = true;
  await page.route('**/api/graph/search?**', async route => {
    const query = new URL(route.request().url()).searchParams.get('q');
    if (failTax && query.startsWith('tax ')) {
      failTax = false;
      await route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ detail: 'Synthetic hub search failure' }) });
    } else await route.continue();
  });
  await page.getByRole('button', { name: 'Taxes', exact: true }).click();
  await visible('Synthetic hub search failure');
  await capture('hubs-error-detail');
  await page.getByRole('button', { name: 'Retry hub search', exact: true }).click();
  await visible('303 matching indexed documents');
  assert.equal(await page.getByRole('alert').filter({ hasText: 'Synthetic hub search failure' }).count(), 0);
  record('hubs_error_and_retry');
  await page.unroute('**/api/graph/search?**');

  // Hold two requests independently. Old completions must not clear the newer
  // loading state, replace its rows, or publish an error into its domain.
  for (const oldOutcome of ['success', 'error']) {
    let oldReady, newReady;
    const oldRequest = new Promise(resolve => { oldReady = resolve; });
    const newRequest = new Promise(resolve => { newReady = resolve; });
    const oldPrefix = oldOutcome === 'success' ? 'vehicle ' : 'tax ';
    const newPrefix = oldOutcome === 'success' ? 'home ' : 'medical ';
    await page.route('**/api/graph/search?**', async route => {
      const query = new URL(route.request().url()).searchParams.get('q');
      if (query.startsWith(oldPrefix)) oldReady(route);
      else if (query.startsWith(newPrefix)) newReady(route);
      else await route.continue();
    });
    await page.getByRole('button', { name: oldOutcome === 'success' ? 'Vehicles' : 'Taxes', exact: true }).click();
    const oldRoute = await oldRequest;
    await page.getByRole('button', { name: oldOutcome === 'success' ? 'Home & Mortgage' : 'Medical', exact: true }).click();
    const newRoute = await newRequest;
    const body = oldOutcome === 'success'
      ? { total: 1, results: [{ labels: ['Document'], properties: { paperless_id: 999, title: 'STALE_HUB_DOCUMENT' } }] }
      : { detail: 'STALE_HUB_ERROR' };
    await oldRoute.fulfill({ status: oldOutcome === 'success' ? 200 : 503, contentType: 'application/json', body: JSON.stringify(body) });
    await page.waitForTimeout(150);
    await visible('Loading documents...');
    assert.ok(!(await page.locator('body').innerText()).includes('STALE_HUB_'));
    assert.equal(await page.getByRole('alert').filter({ hasText: 'STALE_HUB_ERROR' }).count(), 0);
    await newRoute.continue();
    await visible('303 matching indexed documents');
    assert.ok(!(await page.locator('body').innerText()).includes('STALE_HUB_'));
    await page.unroute('**/api/graph/search?**');
  }
  await capture('hubs-stale-request-guard');
  record('hubs_stale_success_error_and_loading');
  assert.deepEqual(errors, []);
  await writeFile(path.join(artifacts, 'hubs-results.json'), JSON.stringify({ outcomes, page_errors: errors }, null, 2) + '\n');
} finally {
  await browser.close();
}
