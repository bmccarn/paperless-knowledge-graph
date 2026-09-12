import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import path from 'node:path';
const require = createRequire(new URL('../frontend/package.json', import.meta.url));
const { chromium } = require('playwright');
const options = JSON.parse(await readFile(process.argv[2], 'utf8'));
assert.equal(new URL(options.base).hostname, '127.0.0.1');
const browser = await chromium.launch();
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  let submissions = 0;
  page.on('request', request => {
    if (new URL(request.url()).pathname === '/api/query/stream') submissions++;
  });
  const capture = async name => writeFile(path.join(options.directory, name),
    await page.screenshot({ fullPage: true, animations: 'disabled' }), { flag: 'wx', mode: 0o600 });
  const safe = async (followupsVisible = true) => {
    const body = await page.locator('body').innerText();
    for (const forbidden of ['Latest source:', 'Wrong index header', 'March 9, 2026',
      'following the completed cancellation']) assert.ok(!body.includes(forbidden), forbidden);
    if (followupsVisible && Object.hasOwn(options, 'acquisition_complete')) {
      const coverage = page.getByRole('region', { name: 'Question coverage', exact: true });
      if (!options.acquisition_complete) assert.ok((await coverage.innerText()).includes(
        'Some source searches or document reads could not finish. Coverage remains partial.'));
      assert.ok((await coverage.innerText()).includes(options.acquisition_complete
        ? 'Requested aspects answered' : 'Partly answered'));
    }
    if (followupsVisible) for (const text of options.neutral)
      await page.getByRole('button', { name: text, exact: true }).waitFor();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true);
  };
  await page.goto(options.base + '/query');
  await page.getByRole('button', { name: 'Strict', exact: true }).click();
  await page.getByPlaceholder('Ask a question...').fill(options.question);
  const responsePromise = page.waitForResponse(r => new URL(r.url()).pathname === '/api/query/stream');
  await page.getByRole('button', { name: 'Send question', exact: true }).click();
  const response = await responsePromise;
  assert.equal(response.status(), 200);
  const events = (await response.text()).split('\n\n').filter(x => x.startsWith('data: ')).map(x => JSON.parse(x.slice(6)));
  const final = events.at(-1);
  assert.equal(final.type, 'complete');
  assert.deepEqual(final.follow_up_suggestions, options.neutral);
  assert.equal(final.finalization.answer_verified, !options.failed);
  if (Object.hasOwn(options, 'acquisition_complete')) {
    assert.equal(final.evidence_pack.coverage.evidence_item_count, 1);
    assert.equal(final.finalization.source_acquisition.complete, options.acquisition_complete);
    assert.equal(final.finalization.question_coverage.complete, options.acquisition_complete);
  }
  await page.waitForFunction(() => !document.querySelector('textarea[placeholder="Ask a question..."]').disabled);
  await page.locator('[id^="answer-"]').last().waitFor();
  await safe(); await capture('desktop-answer.png');
  if (options.failed) assert.deepEqual(final.sources, []);
  else {
    assert.equal(final.sources.length, 1);
    await page.getByRole('button', { name: final.sources[0].title, exact: true }).click();
    const drawer = page.getByRole('dialog', { name: 'Source detail' });
    await drawer.waitFor();
    assert.ok((await drawer.innerText()).includes('Monthly premium: $321.00 USD.'));
    await safe(false); await capture('desktop-source.png'); await page.keyboard.press('Escape');
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await page.locator('[id^="answer-"]').last().scrollIntoViewIfNeeded();
  await safe(); await capture('mobile-answer.png');
  await page.reload();
  await page.getByRole('button', { name: 'Open conversations', exact: true }).click();
  await page.getByRole('dialog').getByRole('button', { name: options.question, exact: true }).click();
  await page.locator('[id^="answer-"]').last().waitFor();
  await safe(); await capture('mobile-restored.png');
  if (Object.hasOwn(options, 'acquisition_complete')) {
    await page.getByRole('region', { name: 'Question coverage', exact: true }).scrollIntoViewIfNeeded();
    await capture('mobile-restored-coverage.png');
  }
  assert.equal(submissions, 1);
  assert.deepEqual(errors, []);
  await writeFile(path.join(options.directory, 'browser.json'), JSON.stringify({
    completed: true, submissions, errors, failed_answer: options.failed,
    source_count: final.sources.length, browser_version: browser.version(),
  }), { flag: 'wx', mode: 0o600 });
} finally { await browser.close(); }
