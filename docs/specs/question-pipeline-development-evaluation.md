# Native question-pipeline development evaluation

Status: proposed execution contract, awaiting independent review. This does not
activate the candidate or replace the original assertion benchmark or sealed holdout.

## Question rubric

`evals/reliability/question-development.json` preserves the original synthetic
source text and document identities from the original-table development dataset.
Its new questions and explicit required aspects test composition, action/date/value
roles, historical preservation, conflict handling and final question coverage.
Requirements are meaning-based: exact wording and observation order are not gold.
Each required aspect must be present in delivered independently supported text.
Source-grounded absence/uncertainty statements count; blanket refusal does not.

Every additional factual assertion must also be checked against originals, not just
the listed forbidden examples. Any unsupported native auditor approval is a false
approval even if a later deterministic guard rejects it. Grade raw auditor outputs,
all repaired candidates and delivered facts separately. A false-complete coverage
receipt occurs whenever any required aspect is missing or wrongly answered but the
receipt claims complete. Never use the model's own coverage score as evaluation gold.

The credit question deliberately does not ask which statement owns the revised
balance: the prior benchmark's ambiguity remains independently disputed, and its
failed labels and scores remain unchanged. Printed -40 USD can be reported directly;
this does not relax production signed-value checks. This corpus is development data,
not a new independent holdout.

## Boundary and execution

Use the actual planner, request-local reader, composer, finalizer/repair/subset and
coverage stages under the locked Strands 1.55 runtime. Supply every case's originals
as a fixed certified evidence pack. This isolates answer production after retrieval;
it does not qualify live retrieval completeness, corpus search, or full application
end-to-end behavior. HTTP/SSE/cache/history and browser contracts are measured
separately. A later live-corpus run is still required before release.

Freeze source, harness, dataset and dependency hashes plus runtime/model route,
deadlines and concurrency before calling a model. Run in a separate process without
changing the serving application's files, runtime, configuration or indexed data.
No ingestion/reindex. Native SDK retry strategy is disabled, OpenAI transport retries
are zero, and proxy cache bypass is requested. Upstream transport attempts and
cache behavior remain unestablished unless independently observed.

Initial slice: the 12 cases once in Strict, in dataset order. Ceiling: 300 native
calls, 1,800 elapsed seconds, estimated 2 million total tokens. Token figure is a
cost planning estimate, not an output-token cap. The hard stop controls are native
call count and wall-clock timeout. No automatic rerun. Each completed case gets
independent original-source grading before the next case is authorized by the
harness operator; stop the candidate on any false approval or missing required
aspect. Unavailable or incomplete runs fail rather than silently disappearing.

Only a passing initial slice permits a separately frozen all-mode run: the same
12 cases in Quick, Deep, Timeline and Strict, 48 case-runs, at most 900 native calls
and 3,600 elapsed seconds, estimated 5 million tokens. This is still development,
not the sealed holdout or a production activation gate by itself.

Record every native input/output, nonterminal output, correction/repair attempt,
usage, stage duration, final candidate/ledger/receipt and exception. Artifacts are
private and exclusive-create; earlier failures must not be overwritten. Record
started/completed/not-started cases, raw and delivered false approvals, missing
required aspects and false-complete coverage. Report unavailable usage as unknown,
not zero. Reviewers grade independently against originals and resolve disagreements
before any continuation. A failure leads to a diagnosed, reviewed next candidate
and fresh manifest; never edit gold to make a candidate pass.
