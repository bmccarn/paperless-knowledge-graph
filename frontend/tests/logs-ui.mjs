/** Log history and stream contracts against an isolated browser connection. */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { chromium } from 'playwright';

const base = process.env.UI_TEST_BASE_URL;
assert.ok(['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const line = message => ({ timestamp: '2026-09-04T12:00:00Z', level: 'INFO', logger: 'fixture.logs', message });

test('Logs retains early lines and commits a burst only on the next frame', async () => {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(5000);
    await page.addInitScript(() => {
      window.__streams = [];
      window.EventSource = class {
        constructor() { window.__streams.push(this); }
        close() { this.closed = true; }
      };
    });
    let history;
    const requested = new Promise(resolve => { history = resolve; });
    await page.route('**/api/logs?**', route => history(route));
    await page.goto(`${base}/debug`);
    const historyRoute = await requested;
    await page.waitForFunction(() => window.__streams.length === 1);
    await page.evaluate(early => {
      window.__streams[0].onopen();
      window.__streams[0].onmessage({ data: JSON.stringify(early) });
    }, line('Early streamed line'));
    await page.getByText('Early streamed line', { exact: false }).waitFor();
    await historyRoute.fulfill({ json: { lines: [line('Historical line')] } });
    await page.getByText('Historical line', { exact: false }).waitFor();
    assert.equal(await page.getByText('Early streamed line', { exact: false }).count(), 1);
    assert.equal(await page.getByRole('heading', { name: 'Logs', level: 1 }).count(), 1);

    await page.evaluate(() => {
      window.__frames = [];
      window.requestAnimationFrame = callback => { window.__frames.push(callback); return window.__frames.length; };
      window.__commits = 0;
      window.__observer = new MutationObserver(() => window.__commits++);
      window.__observer.observe(document.querySelector('[aria-label="Log output"]'), { childList: true, subtree: true });
    });
    // Separate tasks make React flush if the implementation updates each message.
    for (let i = 0; i < 50; i++) await page.evaluate(value => {
      window.__streams[0].onmessage({ data: JSON.stringify(value) });
    }, line(`Burst line ${i}`));
    assert.equal(await page.getByText('Burst line ', { exact: false }).count(), 0);
    assert.equal(await page.evaluate(() => window.__frames.length), 1);
    await page.evaluate(() => window.__frames.shift()(performance.now()));
    await page.getByText('Burst line 49', { exact: false }).waitFor();
    assert.equal(await page.getByText('Burst line ', { exact: false }).count(), 50);
    assert.equal(await page.evaluate(() => window.__commits), 1);
    await page.getByRole('button', { name: 'Pause', exact: true }).click();
    assert.equal(await page.getByRole('button', { name: 'Pause', exact: true }).getAttribute('aria-pressed'), 'true');
    await page.evaluate(value => window.__streams[0].onmessage({ data: JSON.stringify(value) }), line('Paused line'));
    assert.equal(await page.getByText('Paused line', { exact: false }).count(), 0);
    await page.getByRole('button', { name: 'Follow', exact: true }).click();
    assert.equal(await page.getByRole('button', { name: 'Follow', exact: true }).getAttribute('aria-pressed'), 'false');
    await page.getByRole('button', { name: 'Clear', exact: true }).click();
    await page.getByText('No log lines yet.', { exact: true }).waitFor();
  } finally { await browser.close(); }
});

test('Logs reports failed history, reconnects, and reports a lost stream', async () => {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(5000);
    await page.addInitScript(() => {
      window.__streams = [];
      window.EventSource = class {
        constructor() { window.__streams.push(this); }
        close() { this.closed = true; }
      };
    });
    let attempts = 0;
    await page.route('**/api/logs?**', route => {
      attempts++;
      return route.fulfill(attempts === 1 ? { status: 503, json: { lines: [line('Must not be accepted')] } } : { json: { lines: [] } });
    });
    await page.goto(`${base}/debug`);
    await page.getByRole('alert').filter({ hasText: '503' }).waitFor();
    assert.equal(await page.getByText('Must not be accepted', { exact: false }).count(), 0);
    await page.getByRole('button', { name: 'Reconnect', exact: true }).click();
    await page.waitForFunction(() => window.__streams.length === 2);
    await page.evaluate(() => window.__streams[1].onopen());
    await page.getByText('Connected', { exact: true }).waitFor();
    await page.getByText('No log lines yet.', { exact: true }).waitFor();
    assert.equal(attempts, 2);
    await page.route('**/api/logs?**', route => route.fulfill({ status: 503, json: {} }));
    await page.evaluate(() => window.__streams[1].onerror());
    await page.getByRole('button', { name: 'Reconnect', exact: true }).click();
    await page.getByRole('alert').filter({ hasText: '503' }).waitFor();
    await page.evaluate(() => { window.__streams[2].onerror(); window.__streams[2].onopen(); });
    await page.getByRole('alert').filter({ hasText: '503' }).waitFor();
    await page.unroute('**/api/logs?**');
    await page.route('**/api/logs?**', route => route.fulfill({ json: { lines: [] } }));
    await page.getByRole('button', { name: 'Reconnect', exact: true }).click();
    await page.waitForFunction(() => window.__streams.length === 4);
    await page.evaluate(() => window.__streams[3].onopen());
    assert.equal(await page.evaluate(() => window.__streams[0].closed), true);
    await page.evaluate(() => window.__streams[3].onerror());
    await page.getByRole('alert').filter({ hasText: 'Live connection lost' }).waitFor();
    await page.getByRole('button', { name: 'Reconnect', exact: true }).waitFor();
  } finally { await browser.close(); }
});
