import assert from 'node:assert/strict';
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || '/private/tmp/paperless-ui-validation/node_modules/playwright/index.mjs');
const base = process.env.UI_TEST_BASE_URL || 'http://127.0.0.1:3006';
assert.ok(['127.0.0.1','localhost'].includes(new URL(base).hostname));
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
const page = await context.newPage();
const fixture = await context.request.get(base + '/api/_fixture');
assert.equal((await fixture.json()).fixture, 'paperless-accuracy-ui-v1');
await context.request.post(base + '/api/_fixture/reset');
const b = await (await context.request.post(base + '/api/conversations', { data: { title: 'Conversation B' } })).json();
let release;
const held = new Promise(resolve => { release = resolve; });
let started;
const captured = new Promise(resolve => { started = resolve; });
let firstId;
let nextId;
let count = 0;
await page.route('**/api/query/stream', async route => {
  const data = route.request().postDataJSON();
  if (++count === 1) {
    firstId = data.conversation_id;
    started();
    await held;
  } else nextId = data.conversation_id;
  await route.fulfill({ status: 200, contentType: 'text/event-stream', body: 'data: ' + JSON.stringify({type:'complete', answer: count === 1 ? 'ANSWER_FROM_CONVERSATION_A' : 'FOLLOWUP_IN_B', sources:[], confidence: 0, finalization:{disposition:'unsupported',complete:false}}) + '\n\n' });
});
try {
  await page.goto(base + '/query');
  await page.getByRole('button', { name: 'Conversation B', exact: true }).waitFor();
  const input = page.getByPlaceholder('Ask a question...');
  await input.fill('QUESTION_FOR_A');
  await input.press('Enter');
  await captured;
  assert.notEqual(firstId, b.id);
  await page.getByRole('button', { name: 'Conversation B', exact: true }).click();
  await page.getByText('QUESTION_FOR_A', { exact: true }).waitFor({state:'hidden'});
  release();
  await page.getByText('ANSWER_FROM_CONVERSATION_A', {exact:true}).waitFor();
  await input.fill('Follow up on that answer');
  await input.press('Enter');
  await page.getByText('FOLLOWUP_IN_B',{exact:true}).waitFor();
  assert.equal(nextId, b.id);
  const output = {finding:'conversation_cross_contamination', first_query_conversation:firstId, selected_conversation:b.id, subsequent_query_conversation:nextId, stale_answer_displayed:true};
  console.log(JSON.stringify(output));
  await page.screenshot({path:process.env.UI_TEST_SCREENSHOT || '/private/tmp/paperless-conversation-cross-contamination.png',fullPage:true});
} finally { release(); await browser.close(); }
