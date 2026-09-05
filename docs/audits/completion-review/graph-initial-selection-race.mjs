import assert from 'node:assert/strict';
const { chromium }=await import(process.env.PLAYWRIGHT_MODULE || '/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs');
const base=process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3006';
assert.ok(['127.0.0.1','localhost'].includes(new URL(base).hostname));
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:1000}});
const page=await context.newPage();
assert.equal((await (await context.request.get(base+'/api/_fixture')).json()).fixture,'paperless-accuracy-ui-v1');
const initial=await (await context.request.get(base+'/api/graph/initial?limit=100')).json();
let release;
const held=new Promise(resolve=>{release=resolve});
await page.route('**/api/graph/initial?**',async route=>{await held; await route.fulfill({status:200,contentType:'application/json',body:JSON.stringify(initial)});});
try {
  await page.goto(base+'/graph?q=Search%20Fixture');
  await page.getByRole('button',{name:/Search Fixture Entity 000/}).click();
  await page.getByLabel('Evidence inspector',{exact:true}).waitFor();
  await page.getByText('Synthetic pageable graph search fixture.',{exact:true}).waitFor();
  release();
  await page.getByLabel('Evidence inspector',{exact:true}).waitFor({state:'hidden'});
  await page.getByText('4 nodes',{exact:false}).waitFor();
  console.log(JSON.stringify({finding:'initial_graph_erases_selected_search_node',selected:'synthetic-search-000',inspector_visible_before_initial:true,inspector_visible_after_initial:false}));
  await page.screenshot({path:process.env.UI_TEST_SCREENSHOT || '/private/tmp/paperless-graph-initial-selection-race.png',fullPage:true});
} finally {release();await browser.close();}
