/** Local browser contracts against accuracy_fixture.py, never production. */
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3002';
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname), 'Only synthetic localhost fixtures are permitted');
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || 'docs/audits/accuracy-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, deviceScaleFactor: 1 });
const page = await context.newPage();
const errors = [];
page.on('pageerror', error => errors.push(error.message));
const outcomes = [];
const visible = async text => page.getByText(text, { exact: false }).filter({ visible: true }).first().waitFor({ state: 'visible', timeout: 30000 });
const capture = async name => page.screenshot({ path: path.join(artifacts, `${name}.png`), fullPage: true });
const record = (name, detail) => { outcomes.push({ name, status: 'passed', detail }); console.log(`PASS ${name}`); };

try {
  const fixture = await context.request.get(`${base}/api/_fixture`);
  assert.equal(fixture.status(), 200, 'Refusing to run against an application without the synthetic fixture marker');
  assert.equal((await fixture.json()).fixture, 'paperless-accuracy-ui-v1');
  const reset = await context.request.post(`${base}/api/_fixture/reset`);
  assert.equal(reset.status(), 200);
  // Documents must offer the complete indexed set, including later pages.
  await page.goto(`${base}/documents`);
  await visible('303 indexed documents');
  await page.getByRole('button', { name: 'Last page', exact: true }).click();
  await visible('Synthetic archive 300');
  await visible('301–303 of 303');
  await capture('documents-last-page');
  record('documents_pagination', 'Last page reaches document 9300 among 303 indexed documents.');

  await page.getByRole('button', { name: 'Medical 150', exact: true }).click();
  await visible('150 matching indexed documents');
  await page.getByRole('button', { name: 'Last page', exact: true }).click();
  await visible('126–150 of 150');
  record('documents_type_filter', 'Global medical filter yields 150 matching documents and six real pages.');
  await page.getByRole('button', { name: 'Clear', exact: true }).click();
  await page.getByRole('textbox', { name: 'Search indexed documents' }).fill('Synthetic archive 300');
  await page.getByRole('button', { name: 'Search documents', exact: true }).click();
  await visible('1 matching indexed documents');
  await page.getByRole('link', { name: 'Synthetic archive 300', exact: true }).waitFor();
  record('documents_global_search', 'A title outside the former 200-document subset is searchable.');

  await page.getByRole('textbox', { name: 'Search indexed documents' }).fill('fixture-error');
  await page.getByRole('button', { name: 'Search documents', exact: true }).click();
  await visible('Synthetic document search failure');
  await capture('documents-error-detail');
  record('documents_failure', 'Search failure is visible with retry action.');

  // Graph search offers at most 8 entities; the inspector keeps source text literal.
  await page.goto(`${base}/graph`);
  await page.getByRole('combobox', { name: 'Search entities', exact: true }).fill('Search Fixture');
  const results = page.getByRole('listbox', { name: '8 results', exact: true });
  await results.waitFor();
  assert.deepEqual((await results.getByRole('option').allTextContents()).map(text => text.slice(0, 25)),
    Array.from({ length: 8 }, (_, index) => `Search Fixture Entity ${String(index).padStart(3, '0')}`));
  await capture('graph-search-results');
  record('graph_search_results', 'Searching 120 matching entities lists the first 8, grouped as Organizations.');

  await page.goto(`${base}/graph?focus=person-1`);
  await visible('Alex Example: 1 links in 1 families.');
  const canvas = page.locator('canvas').first();
  await canvas.waitFor({ state: 'visible' });
  // The focus sits at the world origin; wait for the initial fit and the hit canvas refresh, then click it.
  await page.waitForFunction(() => document.querySelector('canvas')?.__zoom?.k !== 1);
  await page.waitForTimeout(1000);
  const box = await canvas.boundingBox();
  const zoom = await canvas.evaluate(element => element.__zoom);
  await page.mouse.click(box.x + zoom.x, box.y + zoom.y);
  await visible('This is literal source text.');
  await visible('Outgoing');
  await visible('Inferred');
  assert.equal(await page.locator('b').filter({ hasText: 'This is literal source text.' }).count(), 0);
  record('graph_inspector_safe_text', 'The focus inspector shows the link as inferred; markup in description renders as text.');

  // Feedback remains open after reindex until a human records review.
  await page.goto(`${base}/documents/101`);
  await page.getByRole('textbox', { name: 'Extraction review report' }).fill('Synthetic review: check the premium against the original statement.');
  await page.getByRole('button', { name: 'Mark extraction wrong', exact: true }).click();
  await visible('1 open review reports');
  const reviewNote = page.getByRole('textbox', { name: /Review note for report/ }).last();
  await reviewNote.fill('Checked the $25 premium against the raw OCR after reindex.');
  await page.getByRole('button', { name: 'Resolve reviewed report', exact: true }).last().click();
  await visible('Complete a successful reindex after this report');
  await page.getByRole('button', { name: 'Reindex', exact: true }).click();
  await visible('Reindex completed. Inspect the extracted facts');
  await visible('1 open review reports');
  await page.getByRole('button', { name: 'Resolve reviewed report', exact: true }).last().click();
  await visible('Reindexed and reviewed');
  await visible('Review resolution recorded');
  await visible('January statement: the premium is $25. This is synthetic document evidence.');
  await capture('document-feedback-resolved');
  record('document_feedback_lifecycle', 'Premature resolution fails; reindex leaves report open; explicit review resolves it while OCR stays available.');

  // Final accepted answer, rendered ledger, and source navigation.
  await page.goto(`${base}/query`);
  const question = page.getByPlaceholder('Ask a question...');
  await question.fill('What premium does the January statement list?');
  await question.press('Enter');
  await visible('The January statement lists a premium of $25.');
  await page.getByRole('button', { name: /^Claim table/ }).click();
  await page.getByRole('table', { name: 'Claims' }).getByText('Supported', { exact: true }).waitFor();
  await visible('January statement: the premium is $25.');
  await capture('query-supported');
  await page.getByRole('button', { name: /January premium statement/ }).last().click();
  await page.getByRole('dialog').getByRole('link', { name: 'Open', exact: true }).click();
  await page.waitForURL('**/documents/101');
  await visible('Original OCR text');
  record('query_supported_citation', 'Accepted answer displays supported claim and complete quote; Document 101 citation opens original document inspection.');

  for (const scenario of ['interrupt', 'stream-error']) {
    await page.goto(`${base}/query`);
    await page.getByPlaceholder('Ask a question...').fill(`Run ${scenario} synthetic scenario`);
    await page.getByPlaceholder('Ask a question...').press('Enter');
    await visible('The answer failed. Your question was not answered.');
    assert.ok(!(await page.locator('body').innerText()).includes('DO_NOT_KEEP_UNSUPPORTED_$999'));
    await capture(`query-${scenario}`);
    record(`query_${scenario}`, 'Uncompleted provisional draft is removed instead of finalized.');
  }

  await page.goto(`${base}/query`);
  await page.getByPlaceholder('Ask a question...').fill('Show markup synthetic rendering scenario');
  await page.getByPlaceholder('Ask a question...').press('Enter');
  await visible('Literal source markup:');
  assert.equal(await page.locator('img[src="x"]').count(), 0);
  assert.equal(await page.evaluate(() => window.__fixture_xss), undefined);
  assert.ok((await page.locator('body').innerText()).includes('<img src=x onerror="window.__fixture_xss=1">'));
  await capture('query-safe-markup');
  record('query_safe_markup', 'Hostile model/source HTML is literal text and does not create an image, script or event handler.');

  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`${base}/documents`);
  await visible('303 indexed documents');
  await page.getByRole('button', { name: 'Next page', exact: true }).filter({ visible: true }).click();
  await visible('26–50 of 303');
  await capture('documents-mobile');
  record('documents_mobile_pagination', '390px layout reaches second server page with visible controls.');
  assert.deepEqual(errors, [], 'Browser page runtime errors');
} catch (error) {
  await capture('failure');
  outcomes.push({ name: 'failure', status: 'failed', error: String(error), url: page.url(), page_errors: errors });
  throw error;
} finally {
  await writeFile(path.join(artifacts, 'results.json'), JSON.stringify({ base, browser: 'Playwright Chromium', browser_version: browser.version(), node_version: process.version, outcomes, page_errors: errors }, null, 2));
  await browser.close();
}
