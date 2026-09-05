import assert from 'node:assert/strict';
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || '/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs');
const base=process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3006';
assert.ok(['127.0.0.1','localhost'].includes(new URL(base).hostname));
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:1000}});
const page=await context.newPage();
assert.equal((await (await context.request.get(base+'/api/_fixture')).json()).fixture,'paperless-accuracy-ui-v1');
let release, started;
const held=new Promise(resolve=>{release=resolve});
const captured=new Promise(resolve=>{started=resolve});
await page.route('**/api/reindex/101',route=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({task_id:'held-reindex',status:'started'})}));
await page.route('**/api/task/held-reindex',async route=>{started();await held;await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({status:'completed',result:{processed:1,errors:0}})});});
try {
  await page.goto(base+'/documents');
  await page.getByText('303 indexed documents',{exact:true}).waitFor();
  await page.getByRole('row').filter({hasText:'January premium statement'}).getByRole('button').click();
  await captured;
  const input=page.getByRole('textbox',{name:'Search indexed documents'});
  await input.fill('Synthetic archive 300');
  await page.getByRole('button',{name:'Search documents',exact:true}).click();
  await page.getByText('1 matching indexed documents',{exact:true}).waitFor();
  release();
  await page.getByText('303 matching indexed documents',{exact:true}).waitFor();
  assert.equal(await input.inputValue(),'Synthetic archive 300');
  assert.ok(await page.getByRole('row').filter({hasText:'January premium statement'}).count());
  console.log(JSON.stringify({finding:'reindex_resets_current_search',search:'Synthetic archive 300',expected_matching:1,actual_matching_after_reindex:303}));
  await page.screenshot({path:process.env.UI_TEST_SCREENSHOT || '/private/tmp/paperless-reindex-search-race.png',fullPage:true});
} finally {release();await browser.close();}
