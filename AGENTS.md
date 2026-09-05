# Working in this repo

Read `CONTEXT.md` before exploring ingestion, retrieval, entity review, or freshness. Use `README.md` for the system map and `examples/README.md` for disposable local infrastructure.

## Agent skills

Matt Pocock's engineering and productivity skills are installed in `.agents/skills/`. See `docs/agents/skills.md` for provenance, audit workflow, and invocation examples.

### Issue tracker

Track issues and specs in `bmccarn/paperless-knowledge-graph` on GitHub. Read `docs/agents/issue-tracker.md` when working with tickets or publishing plans.

### Triage labels

Use the five default triage roles. Read `docs/agents/triage-labels.md` before triaging issues.

### Domain docs

Use one root `CONTEXT.md` and `docs/adr/` for accepted decisions. Read `docs/agents/domain.md` when changing domain terminology or recording decisions.

## Validation and audit

Read `docs/agents/validation.md` before running checks or evaluating performance. Keep audit findings grounded in the checked-out revision, with file/line evidence and a way to verify each proposed fix. Separate reproduced defects from architecture candidates and unmeasured optimization hypotheses.
