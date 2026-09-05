# Domain docs

This repository uses a single context, despite having Python and Next.js folders.

- Read root `CONTEXT.md` before exploring domain behavior; use its names in findings, tests, and tickets.
- Read relevant decisions under `docs/adr/` when that directory exists.
- Add an ADR only for an accepted, consequential decision. Audit proposals do not establish architecture decisions.
- Missing ADRs are not a blocker. Create the directory lazily with the first accepted decision.
- Update glossary definitions when the conversation resolves a new term or corrects an existing one.
- Explicitly identify any proposal that would reverse an accepted ADR.

Reports live under `docs/audits/`; issue tracker conventions live in `docs/agents/issue-tracker.md`.
