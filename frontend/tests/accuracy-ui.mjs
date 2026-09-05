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

  await page.getByRole('button', { name: /medical/ }).click();
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

  // Graph search pages and explicit expansion preserve counts.
  await page.goto(`${base}/graph`);
  await visible('4 nodes');
  await page.getByRole('textbox', { name: 'Find an entity or document' }).fill('Search Fixture');
  await page.getByRole('button', { name: 'Search graph', exact: true }).click();
  await visible('120 matches');
  await page.getByRole('button', { name: 'Next', exact: true }).click();
  await visible('Page 2');
  await page.getByRole('button', { name: 'Next', exact: true }).click();
  await visible('Page 3');
  await page.getByRole('button', { name: /Search Fixture Entity 119/ }).click();
  await visible('Synthetic pageable graph search fixture.');
  await capture('graph-search-last-page');
  record('graph_search_pagination', 'Third search page exposes entity 119 among 120 matches and opens its inspector.');

  await page.goto(`${base}/graph`);
  await page.getByRole('button', { name: /Alex Example/ }).click();
  await visible('This is literal source text.');
  await visible('Outgoing');
  await visible('Inferred');
  const expand = page.getByRole('button', { name: 'Load neighbors into graph', exact: true });
  await expand.click();
  await expand.waitFor({ state: 'visible' });
  await page.waitForTimeout(200);
  await expand.click();
  await page.waitForTimeout(200);
  await visible('4 nodes');
  await visible('3 relationships');
  assert.equal(await page.locator('b').filter({ hasText: 'This is literal source text.' }).count(), 0);
  record('graph_expansion_and_safe_text', 'Repeated expansion preserves 4 nodes/3 edges; markup in description renders as text.');

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
  await visible('Claim ledger (1)');
  await visible('supported');
  await visible('January statement: the premium is $25.');
  await capture('query-supported');
  await page.getByRole('link', { name: 'Document 101', exact: true }).first().click();
  await page.waitForURL('**/documents/101');
  await visible('Raw OCR');
  record('query_supported_citation', 'Accepted answer displays supported claim and complete quote; Document 101 citation opens original document inspection.');

  for (const scenario of ['interrupt', 'stream-error']) {
    await page.goto(`${base}/query`);
    await page.getByPlaceholder('Ask a question...').fill(`Run ${scenario} synthetic scenario`);
    await page.getByPlaceholder('Ask a question...').press('Enter');
    await visible('Connection lost.');
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
