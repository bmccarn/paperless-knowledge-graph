# Strands and compatible dependency refresh

Requested September 9 alongside source-reading evaluation. Keep this a separately
reviewable dependency change; do not attribute model accuracy changes to it without
matched evaluation. Findings: [SDK review](../audits/2026-09-09-strands-sdk-review.md).

- Upgrade Strands to 1.55.0 and use its OpenAI extra, matching the imported adapter.
  Preserve the external LiteLLM URL, model route and application defaults.
- Refresh compatible Python dependencies and frontend packages within declared
  ranges. Preserve the SDK's OpenAI<3 constraint and existing explicit frontend pins. Keep React Hooks lint at the previous
  validated 7.0.1 until the separately recorded 12 new diagnostics are addressed.
- Install from committed locks under Python3.12/Node24; run backend regressions,
  dependency compatibility, frontend regression/lint/type/build/browser checks and
  current advisory scans. Review both axes at the combined revision.
- Qualify the final source-reading design on the updated dependency set. Earlier
  SDK1.54 model results remain baseline evidence, not qualification of SDK1.55.
- Deploy only via reviewed GitOps image pins after the existing release gates.
  No Python package operation updates the external proxy deployment.
