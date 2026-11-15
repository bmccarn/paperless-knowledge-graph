import assert from "node:assert/strict";

export const playwrightImage = "mcr.microsoft.com/playwright:v1.65.1-noble@sha256:1507cbfc550a63a6b7396791ca4f00df17beb56342d2447b3f106c306f525579";

export function visualMode(env = process.env) {
  const update = env.UI_BASELINE_UPDATE === "1";
  if (update) {
    assert.equal(env.CI, "true", "Baselines can only be updated in CI");
    assert.equal(env.GITHUB_EVENT_NAME, "workflow_dispatch", "Use the manual baseline workflow");
    assert.equal(env.UI_PLAYWRIGHT_IMAGE, playwrightImage, "Use the pinned Playwright image");
  }
  return update ? "update" : env.UI_VISUAL_GATE === "1" ? "compare" : "capture";
}

export function unexpectedViolations(violations, exceptions, route, state) {
  return violations.filter(violation => !exceptions.some(exception =>
    exception.rule === violation.id && exception.route === route && exception.state === state
    && Number.isInteger(exception.issue) && exception.issue > 0
    && violation.nodes.every(node => exception.targets.includes(JSON.stringify(node.target)))));
}
