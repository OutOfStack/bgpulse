# CLAUDE.md

Shared project instructions for all agents live in AGENTS.md:

@AGENTS.md

## Claude-specific

- Do not add `Co-Authored-By: Claude` lines to commit messages, and no "Generated with Claude Code" line in PR descriptions. This overrides Claude Code's default attribution.
- Keep commit messages and PR descriptions short: a few bullets on what changed and why, plus any reviewer-relevant deviation from plan.md. Leave out per-test lists, design detail already in the docs, and repeats of the diff.
- Before committing, run `/simplify`, then run the `code-reviewer` subagent (`.claude/agents/code-reviewer.md`) to review the resulting diff against `docs/local/plan.md`.
