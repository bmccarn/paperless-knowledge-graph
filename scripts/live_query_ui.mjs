/** One actual built-UI submission. The owner must validate admission before launch. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const require = createRequire(new URL('../frontend/package.json', import.meta.url));
const { chromium } = require('playwright');
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const quote = text => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const privateWrite = (file, bytes) => writeFile(file, bytes, { flag: 'wx', mode: 0o600 });

export async function inspectQuery({ base, request, directory, timeout_ms = 900000 }) {
  const url = new URL(base);
  assert.equal(url.hostname, '127.0.0.1');
  assert.equal(url.protocol, 'http:');
  const report = { completed: false, request, page_errors: [], submission_count: 0,
    screenshots_sha256: {}, inspected_sources: [], inspected_timeline_events: 0 };
  let browser, failure;
  try {
    browser = await chromium.launch({ executablePath: chromium.executablePath(), headless: true });
    report.browser_version = browser.version();
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
    await context.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: base });
    const marker = await context.request.get(`${base}/api/_fixture`);
    assert.equal(marker.status(), 200);
    assert.equal((await marker.json()).fixture, 'paperless-live-evaluation-v1');
    const page = await context.newPage();
    page.setDefaultTimeout(30000);
    page.on('pageerror', error => report.page_errors.push(error.name));
    const submitted = [];
    page.on('request', req => {
      if (new URL(req.url()).pathname === '/api/query/stream') {
        report.submission_count += 1;
        submitted.push({ method: req.method(), body: req.postDataJSON() });
      }
    });
    const screenshot = async name => {
      const bytes = await page.screenshot({ fullPage: true, animations: 'disabled' });
      await privateWrite(path.join(directory, name), bytes);
      report.screenshots_sha256[name] = hash(bytes);
    };
    const noOverflow = async () => assert.equal(await page.evaluate(() =>
      document.documentElement.scrollWidth <= window.innerWidth + 1), true);
    await page.goto(`${base}/query`);
    if (request.history.length) {
      await page.getByRole('button', { name: 'Evaluation conversation', exact: true }).click();
      for (const message of request.history)
        await page.getByText(message.content, { exact: true }).first().waitFor({ state: 'visible' });
    }
    await page.getByRole('button', { name: request.model, exact: true }).waitFor();
    await page.getByRole('button', { name: { quick: 'Fast', deep: 'Deep', timeline: 'Timeline', strict: 'Strict' }[request.mode], exact: true }).click();
    await page.getByPlaceholder('Ask a question...').fill(request.question);
    const responsePromise = page.waitForResponse(response => new URL(response.url()).pathname === '/api/query/stream', { timeout: timeout_ms });
    await page.getByRole('button', { name: 'Send question', exact: true }).click();
    const response = await responsePromise;
    assert.equal(response.status(), 200);
    const body = await response.body();
    report.sse_sha256 = hash(body);
    await privateWrite(path.join(directory, 'browser-sse.bin'), body);
    assert.deepEqual(submitted, [{ method: 'POST', body: {
      question: request.question, conversation_id: 'evaluation', model: request.model, mode: request.mode,
    } }]);
    const events = body.toString('utf8').split('\n\n').filter(frame => frame.startsWith('data: ')).map(frame => JSON.parse(frame.slice(6)));
    assert.equal(events.filter(event => event.type === 'complete').length, 1);
    const final = events.at(-1);
    assert.equal(final.type, 'complete');
    const answer = page.locator('[id^="answer-"]').last();
    await answer.waitFor({ state: 'visible' });
    await page.getByPlaceholder('Ask a question...').waitFor({ state: 'visible' });
    await page.waitForFunction(() => !document.querySelector('textarea[placeholder="Ask a question..."]').disabled);
    assert.ok((await answer.innerText()).trim());
    await page.getByRole('button', { name: 'Copy answer', exact: true }).last().click();
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), final.answer);
    report.rendered_answer_sha256 = hash(Buffer.from(final.answer));
    await noOverflow(); await screenshot('desktop-answer.png');
    const seen = new Map();
    for (const [index, source] of (final.sources || []).entries()) {
      const title = source.title || `Document #${source.document_id}`;
      const label = title + (source.excerpt_count > 1 ? ` (${source.excerpt_count} excerpts)` : '');
      const occurrence = seen.get(label) || 0; seen.set(label, occurrence + 1);
      const button = page.locator('button').filter({ has: page.locator('span').filter({ hasText: new RegExp(`^${quote(label)}$`) }) }).nth(occurrence);
      await button.click();
      const drawer = page.getByRole('dialog', { name: 'Source detail' });
      await drawer.waitFor();
      assert.ok((await drawer.innerText()).includes(source.excerpt || 'No excerpt was returned for this source.'));
      if (source.document_id) assert.equal(await drawer.getByRole('link', { name: 'Open in Paperless' }).getAttribute('href'), source.paperless_url);
      await screenshot(`desktop-source-${String(index).padStart(3, '0')}.png`);
      report.inspected_sources.push(source.document_id);
      await page.keyboard.press('Escape');
    }
    if (request.mode === 'timeline') {
      const receipt = final.finalization?.timeline;
      assert.ok(['ready', 'no_dates'].includes(receipt?.status));
      if (receipt.status === 'ready') {
        const section = page.getByRole('region', { name: 'Dates in verified observations' });
        await section.waitFor();
        assert.equal(await section.locator('article').count(), final.timeline_events.length);
        for (const [index, event] of final.timeline_events.entries()) {
          const article = section.locator('article').nth(index);
          assert.equal(await article.locator('time').getAttribute('datetime'), event.date);
          await article.getByRole('link', { name: 'Read in the full answer' }).click();
          assert.ok((await page.url()).includes('#answer-'));
          await article.getByRole('button').first().click();
          await page.getByRole('dialog', { name: 'Source detail' }).waitFor();
          await page.keyboard.press('Escape');
          report.inspected_timeline_events += 1;
        }
        await screenshot('desktop-timeline.png');
      } else await page.getByText('No supported calendar dates were identified in this answer.', { exact: true }).waitFor();
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await answer.scrollIntoViewIfNeeded(); await noOverflow(); await screenshot('mobile-answer.png');
    if (final.sources?.length) {
      const source = final.sources[0];
      const label = (source.title || `Document #${source.document_id}`) + (source.excerpt_count > 1 ? ` (${source.excerpt_count} excerpts)` : '');
      await page.locator('button').filter({ has: page.locator('span').filter({ hasText: new RegExp(`^${quote(label)}$`) }) }).first().click();
      const drawer = page.getByRole('dialog', { name: 'Source detail' });
      await drawer.waitFor(); await noOverflow();
      assert.ok((await drawer.innerText()).includes(source.excerpt || 'No excerpt was returned for this source.'));
      await screenshot('mobile-source.png'); await page.keyboard.press('Escape');
    }
    // Restore the same private conversation; this must never submit another query.
    await page.reload();
    await page.getByRole('button', { name: 'Open conversations', exact: true }).click();
    await page.getByRole('dialog').getByRole('button', { name: request.history.length ? 'Evaluation conversation' : request.question.slice(0, 80), exact: true }).click();
    await page.locator('[id^="answer-"]').last().waitFor();
    await page.getByRole('button', { name: 'Copy answer', exact: true }).last().click();
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), final.answer);
    await noOverflow(); await screenshot('mobile-restored.png');
    assert.equal(report.submission_count, 1);
    assert.deepEqual(report.page_errors, []);
    report.completed = true;
  } catch (error) { failure = error; report.exception_type = error.name; }
  finally {
    try { await browser?.close(); }
    catch (error) { failure ||= error; report.cleanup_exception_type = error.name; report.completed = false; }
    await privateWrite(path.join(directory, 'browser.json'), JSON.stringify(report));
  }
  if (failure) throw failure;
  return report;
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = JSON.parse(await readFile(process.argv[2], 'utf8'));
  await inspectQuery(options);
}
