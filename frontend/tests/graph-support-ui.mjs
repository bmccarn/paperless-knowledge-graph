/** Targeted localhost graph support inspection, using the backend storage shape. */
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
const errors = [];
page.on('pageerror', error => errors.push(error.message));
try {
  const marker = await context.request.get(`${base}/api/_fixture`);
  assert.equal(marker.status(), 200);
  assert.equal((await marker.json()).fixture, 'paperless-accuracy-ui-v1');
  await page.goto(`${base}/graph`);
  await page.getByRole('button', { name: /Alex Example/ }).click();
  const summary = page.locator('summary').filter({ hasText: 'Evidence from document #101' });
  assert.match(await summary.innerText(), /inferred/);
  await summary.click();
  const quote = 'Alex is a customer. <img src=x onerror="window.__relationship_xss=1">';
  await page.getByText(quote, { exact: true }).waitFor({ state: 'visible' });
  await page.getByText('Extraction rationale: The source names Alex as the customer of Example Utility.', { exact: true }).waitFor({ state: 'visible' });
  assert.equal(await page.locator('img[src="x"]').count(), 0);
  assert.equal(await page.evaluate(() => window.__relationship_xss), undefined);
  assert.deepEqual(errors, []);
  await page.screenshot({ path: path.join(artifacts, 'graph-relationship-support.png'), fullPage: true });
  await writeFile(path.join(artifacts, 'graph-support-results.json'), JSON.stringify({
    scenario: 'graph_relationship_support', status: 'passed', page_errors: errors,
    detail: 'Nested JSON evidence spans and rationale render after expanding source document support; inferred label remains explicit and hostile quote text does not create an image or execute.',
  }, null, 2) + '\n');
  console.log('PASS graph_relationship_support');
} finally {
  await browser.close();
}
