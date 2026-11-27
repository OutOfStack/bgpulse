# CLAUDE.md

Shared project instructions for all agents live in AGENTS.md:

@AGENTS.md

## Claude-specific

- Do not add `Co-Authored-By: Claude` lines to commit messages, and no "Generated with Claude Code" line in PR descriptions. This overrides Claude Code's default attribution.
- Keep commit messages and PR descriptions short: a few bullets on what changed. No intro line naming the plan ticket, no "differs from plan" section, no testing summary, no per-test lists, and no repeats of the diff.
- Before committing, run `/simplify`, then run the `code-reviewer` subagent (`.claude/agents/code-reviewer.md`) to review the resulting diff against `docs/local/plan.md`.
