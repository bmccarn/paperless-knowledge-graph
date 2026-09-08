/** Own the synthetic backend and production frontend for repeatable browser checks. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { cp } from "node:fs/promises";
import { createServer } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const frontend = fileURLToPath(new URL("..", import.meta.url));
const children = [];
const artifacts = path.resolve(process.env.UI_TEST_ARTIFACTS || path.join(frontend, ".browser-artifacts"));
async function freePort() {
  const server = createServer();
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  const port = server.address().port;
  await new Promise(resolve => server.close(resolve));
  return port;
}
function start(command, args, env = {}) {
  const child = spawn(command, args, { cwd: frontend, stdio: "inherit", env: { ...process.env, ...env } });
  // Register the exit promise immediately so early startup failures are observed.
  child.done = new Promise(resolve => {
    child.once("error", error => { console.error(error); resolve(1); });
    child.once("exit", code => resolve(code ?? 1));
  });
  children.push(child);
  return child;
}
async function ready(url, child) {
  const deadline = Date.now() + 45000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode) throw new Error("Test server exited before readiness");
    try {
      const result = await fetch(url, { signal: AbortSignal.timeout(1000) });
      if (result.ok && (await result.json()).fixture === "paperless-accuracy-ui-v1") return;
    } catch { /* wait for the owned listener */ }
    await delay(100);
  }
  throw new Error(`Synthetic fixture not ready: ${url}`);
}
async function cleanup() {
  await Promise.all(children.map(async child => {
    if (child.exitCode !== null || child.signalCode) return;
    child.kill("SIGTERM");
    const stopped = await Promise.race([child.done.then(() => true), delay(5000, false, { ref: false })]);
    if (!stopped) { child.kill("SIGKILL"); await child.done; }
  }));
}
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => {
  void cleanup().finally(() => process.exit(1));
});
try {
  const fixturePort = await freePort();
  const fixture = start(process.env.PYTHON || "python3", ["tests/accuracy_fixture.py", "--port", String(fixturePort)]);
  const backend = `http://127.0.0.1:${fixturePort}`;
  await ready(`${backend}/_fixture`, fixture);
  const uiPort = await freePort();
  const base = `http://127.0.0.1:${uiPort}`;
  // Match the runtime layout and entry point used by the frontend Dockerfile.
  await cp(path.join(frontend, ".next/static"), path.join(frontend, ".next/standalone/.next/static"), { recursive: true });
  await cp(path.join(frontend, "public"), path.join(frontend, ".next/standalone/public"), { recursive: true });
  const ui = start(process.execPath, [".next/standalone/server.js"],
    { BACKEND_URL: backend, NEXT_TELEMETRY_DISABLED: "1", HOSTNAME: "127.0.0.1", PORT: String(uiPort) });
  await ready(`${base}/api/_fixture`, ui);
  for (const suite of ["completion-ui", "accuracy-ui", "graph-support-ui", "graph-zoom-ui", "hubs-ui"]) {
    const args = suite === "completion-ui" ? ["--test", "tests/completion-ui.mjs"] : [`tests/${suite}.mjs`];
    const check = start(process.execPath, args, {
      UI_TEST_BASE_URL: base, UI_TEST_ARTIFACTS: path.join(artifacts, suite),
    });
    assert.equal(await check.done, 0, `${suite} failed`);
  }
} finally {
  await cleanup();
}
