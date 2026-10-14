# Project stage

- This repository is in a purely experimental stage. There are no compatibility commitments to earlier versions of its CLI, topology configuration, protocols, or persisted coordinator state. Implement the intended current behavior directly.
- Do not add legacy validation exceptions, compatibility shims, migrations, or tests solely for old experimental states unless explicitly requested.
- Old local state may need to be recreated after a change; do not delete it automatically.

# Optimization notes

- Record important system optimizations in `docs/optimizations/`, including the evidence, tradeoffs, and potentially, links to other optimizations, source code, or docs.
- This only applies to real system optimizations; for example, changing a configurable value to increase throughput and make a benchmark pass does not count as a system optimization.

# Version control

- Use Git for version control. Commit only when the maintainer asks; pushing requires explicit authorization. Do not amend, rebase, reset, or otherwise rewrite commits you did not create in the current task without asking.
- Other agents may be editing the same working tree. Stage only your own files by path (`git add <paths>`); never use `git add -A`, `git add .`, or `git commit -a`. Keep parallel agents on separate files.
- Use conventional commits (`<type>: <subject>`) with concise, imperative, capitalized subjects and no trailing period. Add a body when explanation is needed.
- When committing, organize work into coherent commits; fold review corrections and reverts into the commit they fix rather than stacking them.

# Authorization boundary

- Never push or create, update, close, or merge a PR unless the user's current message explicitly authorizes that exact action.
