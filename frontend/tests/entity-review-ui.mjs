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
const queue = page.getByRole('region', { name: 'Duplicate candidates', exact: true });
const recent = page.getByRole('region', { name: 'Recent merges', exact: true });
const candidate = name => queue.getByRole('article').filter({ has: page.getByRole('link', { name, exact: true }) });
const mergeButtons = queue.getByRole('button', { name: 'Merge', exact: true });
const dialog = page.getByRole('alertdialog');
const toasts = page.getByRole('status', { name: 'Notifications', exact: true });
// Every visible word and every accessible name; a candidate is never "left" or "right".
const sideWords = () => page.evaluate(() => {
  const names = [...document.querySelectorAll('button, a, [role], [aria-label]')]
    .map(element => `${element.getAttribute('aria-label') || ''} ${element.getAttribute('title') || ''}`);
  return `${document.body.innerText} ${names.join(' ')}`.match(/\b(left|right)\b/gi) || [];
});

try {
  const marker = await context.request.get(`${base}/api/_fixture`);
  assert.equal((await marker.json()).fixture, 'paperless-accuracy-ui-v1');
  assert.equal((await context.request.post(`${base}/api/_fixture/reset`)).status(), 200);
  await page.goto(`${base}/entities/review`);
  await candidate('Cedar Lab 1').waitFor();

  await recent.getByText('No merges yet.', { exact: true }).waitFor();
  for (const name of ['Cedar Lab 1', 'Cedar Laboratory 1']) {
    const href = await candidate('Cedar Lab 1').getByRole('link', { name, exact: true }).getAttribute('href');
    assert.match(href, /^\/graph\?focus=synthetic-review-1-(left|right)$/);
  }
  assert.deepEqual(await sideWords(), []);
  record('entity_candidates_named_and_linked_to_the_graph');

  // Merge deletes an entity, so the first click only asks.
  await toasts.waitFor({ state: 'attached' });
  await candidate('Cedar Lab 1').getByRole('button', { name: 'Merge', exact: true }).click();
  await dialog.waitFor();
  const text = await dialog.innerText();
  for (const expected of ['Cedar Lab 1', 'Cedar Laboratory 1', 'You can undo']) assert.ok(text.includes(expected), expected);
  assert.deepEqual(await sideWords(), []);
  assert.equal(merges.length, 0);
  await capture('entity-review-merge-confirm');
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await dialog.waitFor({ state: 'hidden' });
  await page.waitForTimeout(150);
  assert.equal(merges.length, 0);
  await candidate('Cedar Lab 1').waitFor();
  record('entity_merge_cancel_sends_nothing');

  // Hold the confirmed merge: every candidate stays locked until it settles.
  let release;
  const held = new Promise(resolve => { release = resolve; });
  await page.route('**/api/entity-review/merge', route => release(route));
  await candidate('Cedar Lab 1').getByRole('button', { name: 'Merge', exact: true }).click();
  await dialog.getByRole('button', { name: 'Merge', exact: true }).click();
  const route = await held;
  await dialog.waitFor({ state: 'hidden' });
  for (const name of ['Merge', 'Merging...', 'Keep split', 'Ignore']) {
    const buttons = await queue.getByRole('button', { name, exact: true }).all();
    for (const button of buttons) assert.ok(await button.isDisabled(), `${name} enabled during a pending merge`);
  }
  assert.equal(await queue.getByRole('button', { name: 'Keep split', exact: true }).count(), 3);
  await capture('entity-review-merge-pending');
  await route.continue();
  await page.unroute('**/api/entity-review/merge');
  await toasts.getByText('Merged Cedar Laboratory 1 into Cedar Lab 1.', { exact: true }).waitFor();
  await candidate('Cedar Lab 1').waitFor({ state: 'detached' });
  assert.equal(merges.length, 1);
  assert.ok(await mergeButtons.first().isEnabled());
  record('entity_merge_confirm_sends_once_and_locks_candidates');

  // The toast's Undo calls the unmerge route with the id the merge returned.
  await recent.getByRole('button', { name: 'Undo', exact: true }).waitFor();
  const toastUndo = toasts.getByRole('button', { name: 'Undo', exact: true });
  assert.ok(await toastUndo.evaluate(element => element === document.activeElement), 'focus moves to the toast Undo');
  await capture('entity-review-merged-toast');
  // While another review action runs, the toast's Undo waits like every other control.
  let releaseSplit;
  const heldSplit = new Promise(resolve => { releaseSplit = resolve; });
  await page.route('**/api/entity-review/split', route => releaseSplit(route));
  await candidate('Cedar Lab 3').getByRole('button', { name: 'Keep split', exact: true }).click();
  const split = await heldSplit;
  assert.ok(await toastUndo.isDisabled(), 'toast Undo enabled during a pending action');
  await split.fulfill({ status: 500, json: { detail: 'Synthetic split failure' } });
  await page.unroute('**/api/entity-review/split');
  await page.getByRole('alert').getByText('Synthetic split failure').waitFor();
  assert.ok(await toastUndo.isEnabled());
  await toasts.getByRole('button', { name: 'Undo', exact: true }).click();
  await toasts.getByText('Undid the merge of Cedar Laboratory 1 into Cedar Lab 1.', { exact: true }).waitFor();
  await recent.getByText('Undone', { exact: true }).waitFor();
  assert.equal(unmerges.length, 1);
  assert.equal(JSON.parse(unmerges[0].postData()).merge_id, 'synthetic-merge-1');
  assert.equal(await page.getByRole('button', { name: 'Undo', exact: true }).count(), 0);
  // Both entities exist again: the restored duplicate links back to the graph.
  for (const name of ['Cedar Lab 1', 'Cedar Laboratory 1']) await recent.getByRole('link', { name, exact: true }).waitFor();
  record('entity_toast_undo_restores_both_entities');

  // Recent merges has its own Undo for a merge made earlier.
  await candidate('Cedar Lab 2').getByRole('button', { name: 'Merge', exact: true }).click();
  await dialog.getByRole('button', { name: 'Merge', exact: true }).click();
  await toasts.getByText('Merged Cedar Laboratory 2 into Cedar Lab 2.', { exact: true }).waitFor();
  await toasts.getByRole('button', { name: 'Dismiss', exact: true }).click();
  await toasts.getByText('Merged Cedar Laboratory 2 into Cedar Lab 2.', { exact: true }).waitFor({ state: 'detached' });
  await capture('entity-review-recent-merges');
  // Undo must clear an earlier candidate-load error before refreshing the queue.
  await page.route('**/api/entity-review/candidates?*', route => route.fulfill({ status: 503, json: { detail: 'Synthetic candidate failure' } }));
  await page.getByRole('button', { name: 'Refresh', exact: true }).first().click();
  await queue.getByText('API error: 503 Synthetic candidate failure', { exact: true }).waitFor();
  await page.unroute('**/api/entity-review/candidates?*');
  await recent.getByRole('button', { name: 'Undo', exact: true }).click();
  await toasts.getByText('Undid the merge of Cedar Laboratory 2 into Cedar Lab 2.', { exact: true }).waitFor();
  await candidate('Cedar Lab 3').waitFor();
  assert.equal(await queue.getByRole('alert').count(), 0);
  assert.equal(unmerges.length, 2);
  assert.equal(JSON.parse(unmerges[1].postData()).merge_id, 'synthetic-merge-2');
  assert.equal(await recent.getByText('Undone', { exact: true }).count(), 2);
  record('entity_recent_merge_undo_sends_once_and_marks_the_merge_undone');

  // A merge whose undo hit a conflict shows that status instead of Undo.
  await page.route('**/api/entity-review/merges', route => route.fulfill({ json: { merges: [
    { merge_id: 'synthetic-conflict', primary_uuid: 'synthetic-review-9-left', duplicate_uuid: 'synthetic-review-9-right',
      primary_name: 'Birch Clinic', duplicate_name: 'Birch Medical Clinic', status: 'conflict',
      created_at: '2026-09-03T08:30:00+00:00', risk_flags: ['shared_source_document', 'scalar_clash:phone'] },
  ] } }));
  await page.reload();
  const conflict = recent.getByRole('listitem').filter({ hasText: 'Birch Medical Clinic' });
  await conflict.getByText('Conflict', { exact: true }).waitFor();
  for (const flag of ['Shared source document', 'Different phone']) await conflict.getByText(flag, { exact: true }).waitFor();
  assert.equal(await conflict.getByRole('button', { name: 'Undo', exact: true }).count(), 0);
  record('entity_conflict_merge_shows_its_status_instead_of_undo');

  assert.deepEqual(errors, []);
  await writeFile(path.join(artifacts, 'entity-review-results.json'), JSON.stringify({ outcomes, page_errors: errors }, null, 2) + '\n');
} finally {
  await browser.close();
}
