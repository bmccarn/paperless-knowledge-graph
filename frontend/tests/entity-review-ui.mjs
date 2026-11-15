/** Entity review browser contracts against the isolated synthetic fixture. */
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
const merges = [];
const unmerges = [];
page.on('request', request => {
  const route = new URL(request.url()).pathname;
  if (route === '/api/entity-review/merge') merges.push(request);
  if (route === '/api/entity-review/unmerge') unmerges.push(request);
});
const record = name => { outcomes.push({ name, status: 'passed' }); console.log(`PASS ${name}`); };
const capture = name => page.screenshot({ path: path.join(artifacts, `${name}.png`), fullPage: true });
const mergeButtons = page.getByRole('button', { name: 'Merge right into left', exact: true });
const dialog = page.getByRole('dialog');

try {
  const marker = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await marker.json()).fixture, 'paperless-accuracy-ui-v1');
  assert.equal((await context.request.post(`${base}/api/_fixture/reset`)).status(), 200);
  await page.goto(`${base}/entities/review`);
  await page.getByText('Cedar Lab 1', { exact: true }).waitFor();

  await page.getByText('No merges recorded.', { exact: true }).waitFor();

  // Merge deletes an entity, so the first click only asks.
  await mergeButtons.first().click();
  await dialog.waitFor();
  const text = await dialog.innerText();
  for (const expected of ['Cedar Lab 1', 'Cedar Laboratory 1', 'undo it from Recent merges']) assert.ok(text.includes(expected), expected);
  assert.equal(merges.length, 0);
  await capture('entity-review-merge-confirm');
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await dialog.waitFor({ state: 'hidden' });
  await page.waitForTimeout(150);
  assert.equal(merges.length, 0);
  await page.getByText('Cedar Lab 1', { exact: true }).waitFor();
  record('entity_merge_cancel_sends_nothing');

  // Hold the confirmed merge: every card stays locked until it settles.
  let release;
  const held = new Promise(resolve => { release = resolve; });
  await page.route('**/api/entity-review/merge', route => release(route));
  await mergeButtons.first().click();
  await dialog.getByRole('button', { name: 'Merge', exact: true }).click();
  const route = await held;
  await dialog.waitFor({ state: 'hidden' });
  for (const name of ['Merge right into left', 'Merging...', 'Keep split', 'Ignore']) {
    const buttons = await page.getByRole('button', { name, exact: true }).all();
    for (const button of buttons) assert.ok(await button.isDisabled(), `${name} enabled during a pending merge`);
  }
  assert.equal(await page.getByRole('button', { name: 'Keep split', exact: true }).count(), 3);
  await capture('entity-review-merge-pending');
  await route.continue();
  await page.getByText('Merged "Cedar Laboratory 1" into "Cedar Lab 1".', { exact: true }).waitFor();
  await page.getByText('Cedar Lab 1', { exact: true }).waitFor({ state: 'detached' });
  assert.equal(merges.length, 1);
  assert.ok(await mergeButtons.first().isEnabled());
  record('entity_merge_confirm_sends_once_and_locks_cards');

  // The merge appears under Recent merges; Undo records a split, so the pair stays out of the candidates.
  const undo = page.getByRole('button', { name: 'Undo', exact: true });
  await undo.waitFor();
  await capture('entity-review-recent-merges');
  await undo.click();
  await page.getByText('Undid the merge of "Cedar Laboratory 1" into "Cedar Lab 1".', { exact: true }).waitFor();
  await page.getByText('reverted', { exact: true }).waitFor();
  assert.equal(unmerges.length, 1);
  assert.equal(JSON.parse(unmerges[0].postData()).merge_id, 'synthetic-merge-1');
  assert.equal(await undo.count(), 0);
  record('entity_unmerge_sends_once_and_marks_the_merge_reverted');
  assert.deepEqual(errors, []);
  await writeFile(path.join(artifacts, 'entity-review-results.json'), JSON.stringify({ outcomes, page_errors: errors }, null, 2) + '\n');
} finally {
  await browser.close();
}
