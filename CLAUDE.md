# CLAUDE.md

Shared project instructions for all agents live in AGENTS.md:

@AGENTS.md

## Claude-specific

- Do not add `Co-Authored-By: Claude` lines to commit messages, and no "Generated with Claude Code" line in PR descriptions. This overrides Claude Code's default attribution.
- Before committing, run the `code-reviewer` subagent (`.claude/agents/code-reviewer.md`): it reviews local diffs against `docs/local/plan.md`.
