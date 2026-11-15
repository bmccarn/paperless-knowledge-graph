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
  await page.goto(`${base}/graph?focus=person-1`);
  const canvas = page.locator('canvas').first();
  await canvas.waitFor({ state: 'visible' });
  assert.equal(await page.getByRole('button', { name: /^(2D|3D)$/ }).count(), 0, 'Graph dimension controls must be removed');
  assert.equal(await page.getByLabel('Graph dimensions', { exact: true }).count(), 0);
  // The focus inspector lists the inferred link; selecting it opens the link summary with its document.
  await page.getByRole('region', { name: 'Inspector: Alex Example', exact: true }).getByRole('button', { name: /Example Utility/ }).first().click();
  const link = page.getByRole('region', { name: 'Inspector: link', exact: true });
  await link.getByText('Inferred', { exact: true }).waitFor({ state: 'visible' });
  assert.equal(await link.getByRole('link').count(), 1);
  assert.equal(await page.getByText(/Document #/).count(), 0);
  assert.deepEqual(errors, []);
  await page.screenshot({ path: path.join(artifacts, 'graph-relationship-support.png'), fullPage: true });
  await writeFile(path.join(artifacts, 'graph-support-results.json'), JSON.stringify({
    scenario: 'graph_relationship_support', status: 'passed', page_errors: errors,
    detail: 'Selecting the inferred link opens its summary: the inferred label stays explicit and its one document is listed by title.',
  }, null, 2) + '\n');
  console.log('PASS graph_relationship_support');
} finally {
  await browser.close();
}
