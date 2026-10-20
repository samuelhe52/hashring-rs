# Experimental Lua scope

This crate is purely experimental. Review robustness concerns only at P0 and
correctness concerns at P0 or P1. Do not expand this task into lower-priority
hardening or Redis compatibility work unless the maintainer requests it.

Reuse `hashring-redis` through its generic extension interfaces. Do not copy its
RESP implementation, server loop or ordinary command handlers. Keep Lua-specific
policy and dependencies in this crate.
