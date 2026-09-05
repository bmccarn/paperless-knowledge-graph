# Issue tracker: GitHub

Issues and specs live in [bmccarn/paperless-knowledge-graph](https://github.com/bmccarn/paperless-knowledge-graph/issues). This default follows the repository's origin remote.

Use `gh` from this clone, or pass `--repo bmccarn/paperless-knowledge-graph`. GitHub connector reads are also suitable when the CLI is unavailable.

## Operations

- Read: `gh issue view <number> --comments`.
- List: `gh issue list --state open --json number,title,body,labels`.
- Create: write the exact Markdown body to a file, then `gh issue create --title "..." --body-file <file>`.
- Comment: `gh issue comment <number> --body-file <file>`.
- Label: `gh issue edit <number> --add-label "..."` or `--remove-label "..."`.
- Close: `gh issue close <number>`.

When a skill says to publish to the issue tracker, it means creating or updating a GitHub issue. Local audits and draft tickets remain in `docs/audits/` until publishing is part of the user's request. Skill installation and a local audit do not authorize posting issues, comments, or pull requests.

## Triage

**PRs as a request surface: no.**

Use `docs/agents/triage-labels.md` for role names. GitHub issues and pull requests share numbers; resolve ambiguous references before taking action.

## Wayfinding

A map is an issue labelled `wayfinder:map`. Child issues use `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling`, or `wayfinder:task` and link to their parent map.

Use native sub-issues and dependencies where available. For a dependency, the REST `issue_id` is the blocker's database ID, not its issue number. If unavailable, keep a parent task list and `Part of #<map>` / `Blocked by: #<number>` lines in child bodies. Claim only unassigned children whose blockers are closed. Record the outcome in the map when resolving a child.
