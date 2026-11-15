/** Check the standalone design reference without an application server. */
import assert from 'node:assert/strict';
import { mkdir, readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('../../docs/design/', import.meta.url));
const readme = await readFile(path.join(root, 'README.md'), 'utf8');
const files = [];
for (const name of await readdir(root, { recursive: true })) {
  if ((await stat(path.join(root, name))).isFile()) files.push(name);
}
for (const name of files) assert.ok(readme.includes(`\`${name}\``), `README omits ${name}`);
for (const name of ['tokens.css', 'answer-mockup.html', 'categories.md', 'components.md', 'quality-bar.md']) {
  assert.ok(files.includes(name), `Missing ${name}`);
}
assert.equal(files.filter(name => /^graph\/\d.*\.html$/.test(name)).length, 9);
const template = await readFile(new URL('../../.github/pull_request_template.md', import.meta.url), 'utf8');
assert.ok(template.includes('docs/design/quality-bar.md'));
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || fileURLToPath(new URL('../.browser-artifacts/design-reference', import.meta.url)));
await mkdir(artifacts, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext();
  const network = [];
  const errors = [];
  await context.route(/^https?:/, route => route.abort());
  context.on('request', request => {
    if (/^https?:/.test(request.url())) network.push(request.url());
  });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push(error.message));
  for (const name of files.filter(name => name.endsWith('.html'))) {
    await page.goto(pathToFileURL(path.join(root, name)).href);
    assert.ok((await page.locator('body').innerText()).length > 100, `${name} is empty`);
    for (const href of await page.locator('[href], [src]').evaluateAll(elements =>
      elements.map(element => element.getAttribute('href') || element.getAttribute('src')))) {
      if (!href || href.startsWith('#') || href.startsWith('data:')) continue;
      assert.ok(!/^[a-z]+:/i.test(href) && !href.startsWith('//'), `External asset in ${name}: ${href}`);
      assert.ok((await stat(path.resolve(root, path.dirname(name), href))).isFile());
    }
  }
  for (const [width, height] of [[1440, 900], [1024, 900], [390, 844]]) {
    for (const theme of ['dark', 'light']) {
      await page.setViewportSize({ width, height });
      await page.goto(pathToFileURL(path.join(root, 'answer-mockup.html')).href);
      await page.evaluate(theme => { document.documentElement.dataset.theme = theme; }, theme);
      await page.evaluate(async () => {
        await Promise.all([...document.fonts].map(font => font.load()));
        await document.fonts.ready;
      });
      const rendered = await page.evaluate(() => ({
        overflow: document.documentElement.scrollWidth > innerWidth,
        background: getComputedStyle(document.body).backgroundColor,
        title: getComputedStyle(document.querySelector('h1')).fontFamily,
        prose: getComputedStyle(document.querySelector('.prose')).fontFamily,
        fonts: [...document.fonts].map(font => [font.family, font.status]),
        notes: [...document.querySelectorAll('.side')].filter(note => getComputedStyle(note).display !== 'none').length,
      }));
      assert.equal(rendered.overflow, false, `${theme} ${width}px overflows`);
      assert.ok(rendered.background.includes(theme === 'dark' ? '0.16' : '0.99'));
      assert.ok(rendered.title.includes('Geist Serif'));
      assert.ok(rendered.prose.includes('Source Serif 4'));
      assert.equal(rendered.fonts.length, 5);
      assert.ok(rendered.fonts.every(([, status]) => status === 'loaded'));
      assert.equal(rendered.notes, width < 768 ? 1 : 3);
      await page.screenshot({ path: path.join(artifacts, `answer-${theme}-${width}x${height}.png`), fullPage: true });
    }
  }
  assert.deepEqual(network, [], 'The design reference attempted network requests');
  assert.deepEqual(errors, [], 'The design reference raised browser errors');
  console.log(`PASS design reference: ${files.length} indexed files, 10 local pages, six theme/viewport renders, no network requests`);
} finally {
  await browser.close();
}
