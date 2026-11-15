import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { playwrightImage } from "./visual-policy.mjs";

test("the existing runner owns both servers and always reaches the final visual stage", () => {
  const runner = readFileSync(new URL("./run-browser-checks.mjs", import.meta.url), "utf8");
  assert.equal(runner.match(/const fixture = start\(/g).length, 1);
  assert.equal(runner.match(/const ui = start\(/g).length, 1);
  assert.ok(runner.indexOf('"tests/visual.config.mjs"') > runner.indexOf('"entity-review-ui"'));
  assert.ok(runner.indexOf("assert.equal(failed") > runner.indexOf("await visual.done"));
});

test("CI uploads the comparison on failure and isolates the manual baseline update", () => {
  const workflow = readFileSync(new URL("../../.github/workflows/container-images.yml", import.meta.url), "utf8");
  assert.match(workflow, /name: Upload screenshot comparison and accessibility results\n        if: always\(\)/);
  assert.match(workflow, /if: always\(\) && github.event_name == 'workflow_dispatch' && inputs.update_visual_baselines/);
  assert.match(workflow, /!\(github.event_name == 'workflow_dispatch' && inputs.update_visual_baselines\)/);
  const images = workflow.match(/mcr\.microsoft\.com\/playwright:[^\s]+/g);
  assert.equal(images.length, 2);
  assert.equal(images[0], images[1]);
  assert.equal(images[0], playwrightImage);
  assert.match(images[0], /@sha256:[a-f0-9]{64}$/);
});
