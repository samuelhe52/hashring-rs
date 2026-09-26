# Project stage

This repository is in a purely experimental stage. There are no compatibility
commitments to earlier versions of its CLI, topology configuration, protocols,
or persisted coordinator state. Implement the intended current behavior
directly. Do not add legacy validation exceptions, compatibility shims,
migrations, or tests solely for old experimental states unless explicitly
requested. Old local state may need to be recreated after a change; do not
delete it automatically.

# Optimization notes

Record important system optimizations in `docs/optimizations/`, including the
evidence, tradeoffs, and potentially, links to other optimizations, source code,
or docs. This only applies to real system optimizations; for example, changing
a configurable value to increase throughput and make a benchmark pass does not
count as a system optimization.

# Authorization boundary

Never commit, push, create/update/close/merge a PR, tag, release, publish, or
modify remote state unless the user's current message explicitly authorizes
that exact action.
