# Matt Pocock skills

The 25 engineering and productivity skills from [mattpocock/skills](https://github.com/mattpocock/skills/tree/3cca18b368ae95cdbdebbff572ccafa662551015/skills) are vendored as ordinary project files under `.agents/skills/`. Deprecated, in-progress, and miscellaneous collections are excluded.

Installed revision: `3cca18b368ae95cdbdebbff572ccafa662551015`. Source paths and per-file hashes are recorded in `skills-lock.json` beside this file. The upstream MIT license is preserved in `matt-pocock-LICENSE.txt`. The lock is a provenance manifest, not a lockfile managed by the `skills` CLI.

Codex discovers these project skills on the next turn. Project copies provide a reviewable snapshot even when similarly named skills are already installed globally.

## Useful starting points

| Task | Skill / prompt |
| --- | --- |
| Find architecture improvements | `$improve-codebase-architecture audit ingestion, retrieval, and query streaming` |
| Choose an engineering workflow | `$ask-matt help me work through the repo audit findings` |
| Investigate a confirmed defect | `$diagnosing-bugs` with the finding and reproduction |
| Implement a behavior change with regression coverage | `$tdd` with the accepted scope |
| Review a branch against a baseline | `$code-review` with a commit/branch and spec |
| Turn agreed findings into a draft spec | `$to-spec` with the agreed scope and destination |

The full-repo audit differs from `code-review`, which expects a non-empty diff from a specified fixed point and, for its spec axis, an originating spec.

## Repo setup

`AGENTS.md` points skills to GitHub issue conventions, default triage labels, one root domain glossary, and validation guidance. These defaults are editable in `docs/agents/*.md`; re-running `setup-matt-pocock-skills` is optional when changing them.

For an update, choose an upstream revision, review the skill changes, replace the vendored files, and refresh the provenance hashes and license from that revision. Keep project-specific conventions here instead of modifying upstream skill text. The upstream README also documents its interactive `npx skills@latest add mattpocock/skills` installer.
