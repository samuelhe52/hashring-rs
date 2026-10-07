# Project stage

- This repository is in a purely experimental stage. There are no compatibility commitments to earlier versions of its CLI, topology configuration, protocols, or persisted coordinator state. Implement the intended current behavior directly.
- Do not add legacy validation exceptions, compatibility shims, migrations, or tests solely for old experimental states unless explicitly requested.
- Old local state may need to be recreated after a change; do not delete it automatically.

# Optimization notes

- Record important system optimizations in `docs/optimizations/`, including the evidence, tradeoffs, and potentially, links to other optimizations, source code, or docs.
- This only applies to real system optimizations; for example, changing a configurable value to increase throughput and make a benchmark pass does not count as a system optimization.

# Local version control

- Use jj (colocated with Git) for local version control: `jj describe`, `jj new`, and `jj log` instead of `git add` and `git commit`.
- If jj is not installed or the repository is not initialized for jj, use normal Git workflows, including proactively recording your own work with `git add` and `git commit`.
- Record your own work proactively with `jj describe` and `jj new`, even when the user does not explicitly request a commit. Do not squash, rebase, abandon, or edit changes you did not create in the current task without asking.
- Use conventional commits (`<type>: <subject>`) with concise, imperative, capitalized subjects and no trailing period. Add a body when explanation is needed, and organize complex work into coherent changes.

# Authorization boundary

- Never push (including `jj git push`) or create, update, close, or merge a PR unless the user's current message explicitly authorizes that exact action.
