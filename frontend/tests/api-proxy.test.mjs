import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
const require = createRequire(import.meta.url);
const { proxyBackend, mutationAllowed } = require('../.test-build/api-proxy.js');

const request = (headers = {}, options = {}) => new Request('https://kg.test/api/query/stream', {
  method: 'POST', body: '{"question":"test"}', headers: { origin: 'https://kg.test', ...headers }, ...options,
});

test('mutation origin validation rejects hostile, missing and same-site origins', () => {
  assert.equal(mutationAllowed(request()), true);
  assert.equal(mutationAllowed(new Request('http://0.0.0.0:3000/api/sync', {
    method:'POST', headers:{host:'kg.test', origin:'https://kg.test', 'sec-fetch-site':'same-origin'},
  })), true);
  assert.equal(mutationAllowed(request({host:'kg.test', 'x-forwarded-host':'evil.test', origin:'https://evil.test'})), false);
  for (const headers of [{origin:'https://evil.test'}, {origin:'null'}, {'sec-fetch-site':'cross-site'}, {'sec-fetch-site':'same-site'}]) {
    assert.equal(mutationAllowed(request(headers)), false);
  }
  assert.equal(mutationAllowed(new Request('https://kg.test/api/sync', {method:'POST'})), false);
  assert.equal(mutationAllowed(new Request('https://kg.test/api/status')), true);
});

test('both proxy paths inject only server key and preserve errors, SSE and cancellation', async () => {
  const original = global.fetch;
  const env = {...process.env};
  process.env.KG_API_KEY = 'server-secret';
  process.env.BACKEND_URL = 'http://backend.test:8000';
  try {
    for (const streaming of [false, true]) {
      let called = 0;
      global.fetch = async () => { called++; throw Error('must not call'); };
      assert.equal((await proxyBackend(request({origin:'https://evil.test'}), 'sync', streaming)).status, 403);
      assert.equal(called, 0);
      for (const status of [401,403,422,500,200]) {
        global.fetch = async (url, init) => {
          assert.equal(url, 'http://backend.test:8000/query/stream');
          assert.equal(init.headers['X-KG-API-Key'], 'server-secret');
          assert.equal(init.headers.cookie, undefined);
          assert.equal(init.redirect, 'manual');
          assert.equal(new TextDecoder().decode(init.body), '{"question":"test"}');
          return new Response(status === 200 ? 'data: ok\n\n' : '{"detail":"test error"}', {
            status, headers:{'content-type': status === 200 ? 'text/event-stream' : 'application/json'},
          });
        };
        const response = await proxyBackend(request({'X-KG-API-Key':'attacker',cookie:'private'}), 'query/stream', streaming);
        assert.equal(response.status,status);
        assert.equal(response.headers.has('X-KG-API-Key'),false);
        assert.equal(await response.text(),status === 200 ? 'data: ok\n\n' : '{"detail":"test error"}');
      }
      const controller = new AbortController();
      global.fetch = async (_url, init) => {
        controller.abort();
        assert.equal(init.signal.aborted,true);
        throw new DOMException('Aborted','AbortError');
      };
      assert.equal((await proxyBackend(request({}, {signal:controller.signal}), 'query/stream',streaming)).status,499);
      let cancelled = false;
      global.fetch = async () => new Response(new ReadableStream({cancel(){cancelled=true;}}), {headers:{'content-type':'text/event-stream'}});
      const response = await proxyBackend(request(), 'query/stream', streaming);
      await response.body.cancel();
      assert.equal(cancelled,true);
    }
  } finally { global.fetch = original; process.env = env; }
});

test('browser callers use same origin and both route adapters use protected proxy', () => {
  for (const file of ['src/lib/api.ts','src/app/debug/page.tsx']) {
    const source = readFileSync(new URL(`../${file}`,import.meta.url),'utf8');
    assert.ok(!source.includes('NEXT_PUBLIC_API_URL'));
    assert.ok(source.includes('"/api"'));
    assert.ok(!source.includes('KG_API_KEY'));
  }
  for (const file of ['src/app/api/[...path]/route.ts','src/app/api/query/stream/route.ts']) {
    assert.ok(readFileSync(new URL(`../${file}`,import.meta.url),'utf8').includes('proxyBackend(req,'));
  }
});
