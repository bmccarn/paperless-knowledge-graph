# Strands SDK and dependency review

Checked September 9, 2026 against the official current docs, GitHub releases,
installed Python 1.54.0 implementation and isolated 1.55.0 environment. This is
independent of the frozen source-reading accuracy experiment.

## Version and dependency findings

The latest stable Python release observed is **1.55.0**, published September 8.
The repository moved from `sdk-python` to `harness-sdk`; package name remains
`strands-agents`. Relevant release changes include closing trace spans on
cancellation and cached-token accounting, plus new task/context machinery.
These are operational improvements, not evidence of better source interpretation.
[Official release](https://github.com/strands-agents/harness-sdk/releases/tag/python/v1.55.0).

The current application imports `strands.models.openai.OpenAIModel` and uses the
OpenAI-compatible HTTP endpoint of the external LiteLLM server. No application or
test imports the local `litellm` Python package. The former `[litellm]` dependency
extra was therefore unnecessary. The update selects `[openai]>=1.55,<2`, removing
22 unused packages and adding the adapter's required Bedrock-token helper. The
external proxy/model route does not change.

Installed 1.55.0 metadata explicitly requires OpenAI `>=1.68,<3` for this adapter.
Although the registry reports OpenAI 3.10.0, forcing it would violate that declared
compatibility. Keep 2.54.0. The unused LiteLLM extra caps LiteLLM at 1.96.0; forcing
1.100.0 with Strands 1.55.0 produced an unsatisfiable resolution. Removing the
unused extra is preferable to silently downgrading Strands. The external LiteLLM
server has its own GitOps lifecycle and is not updated by a Python lock refresh.

The refreshed Python lock also updates AnyIO, boto3/botocore, NumPy and
sse-starlette within their current version lines. Pydantic core remains coupled to
Pydantic's exact dependency. Frontend refresh stays within declared ranges; pinned
Next.js and React versions were still the latest registry versions observed.
Unrelated major migrations (OpenAI 3, Lucide 1 and Three.js beyond the declared
0.170 range) are not forced into this update.

## Features and application fit

| Feature | Current use or opportunity | Decision |
| --- | --- | --- |
| Native structured outputs | We already use provider JSON schemas plus independent application validation. Strands `structured_output_model` provides Pydantic/tool-based output processing. | Consider only as a separately measured protocol implementation; valid structure does not establish truth and automatic correction behavior must remain budgeted. |
| Graph orchestration | Graphs support dependencies and automatically propagate task/dependency output to downstream nodes. | Useful if stages grow, but explicit reader/verifier calls currently keep candidate blindness easy to audit. A Graph must not accidentally pass the candidate to the reader. |
| Conversation/context managers | Python 1.54 already supplies Null, SlidingWindow and Summarizing managers; latest docs also cover proactive compression/offloading. | Useful for long agent/tool conversations. Do not summarize or silently drop certifying source windows. Test explicit no-reduction behavior for single audit calls before any adoption. |
| Hooks, metrics and tracing | SDK supports invocation metadata and OpenTelemetry. Current app records stage timings and native usage without raw source logging. | Add content-free stage correlation and actual transport-attempt accounting in the reviewed design. Avoid exporting OCR/prompts as routine traces. |
| Evals SDK | Supports custom deterministic and model-based evaluators, including faithfulness, correctness, relevance and trajectory assessments. | Useful for end-to-end coverage reports; preserve independent source labels and hard original-source gates. A model judge cannot replace the gold corpus. |
| Retry strategy | Installed Agent defaults to a model retry strategy with up to six attempts; OpenAI client `max_retries=0` does not disable that separate layer. | Record this limitation: existing evaluation counters count native agent invocations, not proven upstream request totals. Make retries explicit and observe them before claiming strict transport-call budgets. |
| Session/memory/task features | Useful for persistent, long-running tool agents. | No demonstrated benefit for a fresh two-stage document audit; persistent model notes could become stale. Do not add shared memory as certifying evidence. |

Sources: [structured output](https://strandsagents.com/docs/user-guide/concepts/agents/structured-output/),
[Graph](https://strandsagents.com/docs/user-guide/concepts/multi-agent/graph/),
[conversation management](https://strandsagents.com/docs/user-guide/concepts/agents/conversation-management/),
[traces](https://strandsagents.com/docs/user-guide/observability-evaluation/traces/),
[evaluation](https://strandsagents.com/docs/user-guide/evals-sdk/quickstart/),
[retry strategies](https://strandsagents.com/docs/user-guide/concepts/agents/retry-strategies/).
Installed retry evidence: `strands/agent/agent.py` constructor documentation and
sentinel resolution; `retry_strategy=None` selects a single-attempt strategy.
No retry behavior was changed during the frozen comparison.

## Validation status

The refreshed OpenAI-extra lock installs with hashes into an isolated Python 3.12
environment. `uv pip check` confirms all 68 installed packages are compatible.
The combined source-reading/schema-correction head passes 590 offline tests with
48 expected datastore skips under the updated SDK.
Frontend locked installation reports zero vulnerabilities; Python advisory scanning
also reports zero known vulnerabilities. The production build, typecheck and 12
frontend regressions pass. All five browser suites pass, including 15 completion
contracts, graph support/zoom at 1000x and hub/error/mobile flows. A test race was
corrected: the document refresh test waited for the page counter but captured the
old rows before the new page response committed. It now waits for the known second
page row before comparison; application behavior and assertions are unchanged.

React Hooks lint 7.1.1 introduces 12 diagnostics on existing effect/ref patterns.
This refresh explicitly retains the previously validated 7.0.1 as a direct exact
dev dependency; lint passes with that explicit pin and no rule is disabled. Treat upgrading that analyzer and addressing
its new diagnostics as a separate React lifecycle/compiler migration, not a clean
latest-version result. OpenAI3 remains excluded by the SDK's declared constraint.

Rendered Chromium artifacts were visually reviewed for the desktop document list,
mobile verified-partial query and mobile graph inspector. Native Computer Use
connection failed (empty browser inventory/native pipe startup failure), so this
is screenshot inspection of actual browser contracts, not a new native interactive
or production acceptance pass. Source-head reviews are recorded separately.
No production deployment, raw trace export, model route or token cap changed.
