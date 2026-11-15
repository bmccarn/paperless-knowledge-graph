import assert from "node:assert/strict";
import { test } from "node:test";
import { playwrightImage, unexpectedViolations, visualMode } from "./visual-policy.mjs";

test("baseline writes require the manual CI job on the pinned image", () => {
  assert.equal(visualMode({}), "capture");
  assert.equal(visualMode({ UI_VISUAL_GATE: "1" }), "compare");
  const env = { UI_BASELINE_UPDATE: "1", CI: "true", GITHUB_EVENT_NAME: "workflow_dispatch", UI_PLAYWRIGHT_IMAGE: playwrightImage };
  assert.equal(visualMode(env), "update");
  for (const key of ["CI", "GITHUB_EVENT_NAME", "UI_PLAYWRIGHT_IMAGE"]) assert.throws(() => visualMode({ ...env, [key]: "wrong" }));
});

test("exceptions only allow known targets for the same rule, route and state", () => {
  const violation = { id: "button-name", nodes: [{ target: ["#old"] }] };
  const exceptions = [{ rule: "button-name", route: "/query", state: "main", issue: 86, targets: ['["#old"]'] }];
  assert.deepEqual(unexpectedViolations([violation], exceptions, "/query", "main"), []);
  for (const [route, state] of [["/", "main"], ["/query", "dialog"]])
    assert.equal(unexpectedViolations([violation], exceptions, route, state).length, 1);
  assert.equal(unexpectedViolations([{ ...violation, nodes: [{ target: ["#new"] }] }], exceptions, "/query", "main").length, 1);
  assert.equal(unexpectedViolations([{ ...violation, id: "label" }], exceptions, "/query", "main").length, 1);
});
