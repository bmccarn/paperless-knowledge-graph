/** The path view: the p key, pickers, the owner and inferred controls, the ranked list, its title, Ask and the phone list. */
import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const base = process.env.UI_TEST_BASE_URL;
assert.ok(base && ['127.0.0.1', 'localhost'].includes(new URL(base).hostname));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || '.browser-artifacts/graph-path-ui');
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch();
const errors = [];
const OWNER = 'Rowan Vale';
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.message));
  const searches = [];
  page.on('request', request => { if (request.url().includes('/api/graph/paths?')) searches.push(new URL(request.url()).searchParams); });
  const list = page.getByRole('list', { name: 'Paths', exact: true });
  const inspector = page.getByRole('region', { name: 'Inspector: paths', exact: true });
  const history = () => page.evaluate(() => window.history.length);

  // p does nothing while a text field has focus, then opens Paths with A set to the focus.
  await page.goto(`${base}/graph?focus=nb-money-0`);
  await page.locator('canvas').first().waitFor();
  await page.getByText('Harbor Mutual Insurance:', { exact: false }).waitFor();
  const focusUrl = page.url();
  await page.evaluate(() => { const field = document.createElement('input'); field.id = 'typing-guard'; document.body.append(field); field.focus(); });
  await page.keyboard.press('p');
  await page.waitForTimeout(150);
  assert.equal(page.url(), focusUrl, 'p in a text field does nothing');
  await page.locator('#typing-guard').evaluate(field => field.remove());
  const before = await history();
  await page.keyboard.press('p');
  await page.waitForURL(url => url.searchParams.get('view') === 'path');
  assert.equal(new URL(page.url()).searchParams.get('a'), 'nb-money-0', 'a switch to Paths sets A to the focus');
  assert.equal(await history(), before + 1);
  assert.equal(await page.locator(':focus').getAttribute('data-graph-view'), 'path', 'focus stays on the switcher');
  await page.getByRole('status').filter({ hasText: 'Paths of Harbor Mutual Insurance' }).waitFor({ state: 'attached' });
  assert.equal(await page.getByRole('combobox', { name: 'From' }).inputValue(), 'Harbor Mutual Insurance');
  await page.getByText('Pick two entities to see how they are connected.', { exact: true }).waitFor();
  assert.equal(await page.locator('canvas').count(), 0);
  assert.equal(await page.getByRole('region', { name: /^Evidence ·/ }).count(), 0, 'Paths has no evidence shelf');

  // B by search: type, move with the keyboard, choose with Enter. A new end is a history step.
  const to = page.getByRole('combobox', { name: 'To' });
  await to.fill('Port Ell');
  await page.getByRole('option', { name: /Port Ellery/ }).first().waitFor();
  const beforeB = await history();
  await to.press('Enter');
  await page.waitForURL(url => url.searchParams.get('b') === 'nb-places-1');
  assert.equal(await history(), beforeB + 1);
  await inspector.waitFor();
  const routes = async () => (await list.getByRole('button').allTextContents());
  // With the owner left out, the fixture has two paths, neither through the owner.
  assert.equal((await routes()).length, 2);
  assert.ok((await routes()).every(text => !text.includes(OWNER)));
  assert.equal(await page.getByRole('img', { name: /paths from Harbor Mutual Insurance to Port Ellery; path 1 selected$/ }).count(), 1);
  assert.match(await inspector.innerText(), /2 paths of at most 4 links, strongest first\. The owner is left out\./);
  assert.equal(await page.getByText('Best of the paths checked', { exact: true }).count(), 0, 'an exhausted search has no such title');
  assert.equal(searches.at(-1).get('include_owner'), '0');
  assert.equal(searches.at(-1).get('allow_inferred'), '1');
  await page.screenshot({ path: path.join(artifacts, 'path-owner-left-out.png') });

  // Include the owner runs the search again: at most 5 paths, now through the owner. Rules replace history.
  const beforeOwner = await history();
  const count = searches.length;
  await page.getByRole('button', { name: 'Include the owner', exact: true }).click();
  await page.waitForURL(url => url.searchParams.get('owner') === 'show');
  await list.getByRole('button').filter({ hasText: OWNER }).first().waitFor();
  assert.equal(searches.length, count + 1);
  assert.equal(searches.at(-1).get('include_owner'), '1');
  assert.equal(await history(), beforeOwner);
  const withOwner = await routes();
  assert.equal(withOwner.length, 5, 'the fixture has 29 paths through the owner; at most 5 show');
  assert.ok(withOwner.some(text => text.includes(OWNER)));
  assert.equal(await page.getByRole('button', { name: 'Include the owner' }).getAttribute('aria-pressed'), 'true');

  // Selecting a path shows its steps; a link that runs against the path is read in its own direction.
  await list.getByRole('button').nth(1).click();
  assert.equal(await list.getByRole('button').nth(1).getAttribute('aria-pressed'), 'true');
  await inspector.getByRole('heading', { name: 'Path 2', exact: true }).waitFor();
  const steps = inspector.getByRole('list', { name: 'Steps of the path' });
  assert.deepEqual((await steps.locator(':scope > li > p:first-child').allTextContents()).map(text => text.trim()),
    ['Harbor Mutual Insurance', 'Policy HM-2208 (auto)', OWNER, 'Port Ellery']);
  assert.match(await steps.innerText(), /Policy HM-2208 \(auto\) issued by Harbor Mutual Insurance\./);
  // Each link shows its best quote, with both end names marked.
  const quote = steps.locator('blockquote').filter({ hasText: 'issued by' });
  await quote.waitFor();
  assert.equal((await quote.innerText()).trim(), '“Policy HM-2208 (auto) issued by Harbor Mutual Insurance, as listed in this record.”');
  assert.deepEqual(await quote.locator('mark').allTextContents(), ['Policy HM-2208 (auto)', 'Harbor Mutual Insurance']);
  assert.equal(await steps.locator('blockquote').count(), 3);

  // The inferred-link control and Most links are filters too.
  await page.getByRole('button', { name: 'Inferred links', exact: true }).click();
  await page.waitForURL(url => url.searchParams.get('support') === 'supported');
  await page.getByRole('group', { name: 'Most links' }).getByRole('button', { name: '2', exact: true }).click();
  await page.waitForURL(url => url.searchParams.get('maxlen') === '2');
  await inspector.getByText('of at most 2 links', { exact: false }).waitFor();
  assert.equal(searches.at(-1).get('allow_inferred'), '0');
  assert.equal(searches.at(-1).get('max_len'), '2');
  assert.match(await inspector.innerText(), /Inferred links are left out\./);

  // Swap is a history step and keeps the rules.
  await page.getByRole('button', { name: 'Swap the two entities' }).click();
  await page.waitForURL(url => url.searchParams.get('a') === 'nb-places-1' && url.searchParams.get('b') === 'nb-money-0');
  assert.equal(new URL(page.url()).searchParams.get('maxlen'), '2');

  // A search that stopped before it was exhausted is titled "Best of the paths checked".
  await page.route('**/api/graph/paths?**', async route => {
    const response = await route.fetch();
    await route.fulfill({ response, json: { ...(await response.json()), exhausted: false } });
  });
  await page.goto(`${base}/graph?focus=nb-money-0&view=path&a=nb-money-0&b=nb-places-1`);
  await page.getByRole('heading', { name: 'Best of the paths checked', exact: true }).waitFor();
  assert.match(await inspector.innerText(), /The search stopped before it checked every path of at most 4 links\. These are the strongest 2\./);
  await page.unroute('**/api/graph/paths?**');

  // The 5 s budget answers 503; the view says so and Retry searches again.
  await page.route('**/api/graph/paths?**', route => route.fulfill({ status: 503, json: { detail: 'path_search_timeout' } }));
  await page.reload();
  await page.getByText('The path search took too long.', { exact: true }).waitFor();
  await page.unroute('**/api/graph/paths?**');
  await page.getByRole('button', { name: 'Retry', exact: true }).click();
  await inspector.waitFor();

  // No path joins the two: the response still names both ends.
  await page.goto(`${base}/graph?view=path&a=nb-money-0&b=nb-leaf`);
  await page.getByText('No path of at most 4 links joins Harbor Mutual Insurance and Spare key tag. The owner is left out.', { exact: true }).waitFor();

  // "Ask how they are related" opens Ask with the question in the composer, not sent.
  await page.goto(`${base}/graph?focus=nb-money-0&view=path&a=nb-money-0&b=nb-places-1`);
  await inspector.getByRole('link', { name: 'Ask how they are related' }).click();
  await page.waitForURL(url => url.pathname === '/query');
  const composer = page.getByPlaceholder('Ask a question...');
  await page.waitForFunction(() => document.querySelector('textarea')?.value.length > 0);
  assert.equal(await composer.inputValue(), 'How is Harbor Mutual Insurance related to Port Ellery?');
  assert.equal(await page.getByText('The January statement lists a premium of $25.').count(), 0, 'the question is not submitted');

  // Without a focus, p on the landing opens Paths with empty pickers; Focus and Table wait for an entity.
  await page.goto(`${base}/graph`);
  await page.getByRole('region', { name: 'Most connected', exact: true }).getByRole('link').first().waitFor();
  await page.locator('h1').click();
  await page.keyboard.press('p');
  await page.waitForURL(url => url.searchParams.get('view') === 'path');
  assert.equal(new URL(page.url()).searchParams.has('a'), false);
  await page.getByText('Pick two entities to see how they are connected.', { exact: true }).waitFor();
  const toolbar = page.getByRole('toolbar', { name: 'View', exact: true });
  assert.equal(await toolbar.getByRole('button', { name: 'Focus' }).getAttribute('aria-disabled'), 'true');
  assert.equal(await toolbar.getByRole('button', { name: 'Paths' }).getAttribute('aria-pressed'), 'true');

  // Phone: no canvas; the selected path is a stepper, then the other paths.
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`${base}/graph?focus=nb-money-0&view=path&a=nb-money-0&b=nb-places-1&owner=show`);
  await page.getByRole('heading', { name: /^Path 1 of 5/ }).waitFor();
  assert.equal(await page.locator('svg[role=img]').count(), 0);
  assert.equal(await page.getByRole('list', { name: 'Paths' }).getByRole('button').count(), 4);
  await page.getByRole('heading', { name: 'Other paths', exact: true }).waitFor();
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  await page.screenshot({ path: path.join(artifacts, 'path-phone.png'), fullPage: true });
  assert.deepEqual(errors, []);
} finally {
  await browser.close();
}
