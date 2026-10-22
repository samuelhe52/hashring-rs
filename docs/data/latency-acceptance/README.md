# The acceptance run of the `latency` mode

The numbers in [`docs/latency-acceptance.md`](../../latency-acceptance.md) come
from the runs in this directory. The runs themselves are not in git: they are
big, and every one of them is reproducible from the script.

## What is here

| File | What it is |
| --- | --- |
| `host.txt` | The machine the numbers were measured on. |
| `stages.tsv` | One row per stage of every run. |
| `medians.tsv` | One row per stage, over the repetitions of it. |
| `source.patch` | The source the campaign was built from, as one patch. |
| `<run>/` | The tables of one campaign, in the directory the run wrote. |

The `stages.tsv` columns are the run's own numbers, renamed to fit a table:
`get_p50` is the p50 of the `GET` latency in microseconds, `err_deadline` is
the number of requests that ran out of their deadline, `owned_mom` is the
`owned_keys_max_over_mean` of the load balance, and so on. The `run` column
names the directory under `results/`, and `kind` says what the run was for:
`campaign` is the repeated run at the target rates, `ladder` is the run that
went past the capacity, and `vnode` is the virtual node comparison.

`host_preload_ops` is the rate of the run's own preload, the same 100,000
key write in every run. It is the run's measure of how fast the machine was
while it ran, and it is the column to look at first: the machine is shared,
and its speed moves between campaigns. It moved by a factor of 1.8 between
two of them, and by 5% within the one the report is written from.

## How to reproduce

```sh
cargo build --release
./scripts/latency-sweep.sh
```

The script measures the capacity with `performance` mode, takes the target
rates from it, runs the campaign, the ladder and the virtual node comparison,
and writes the tables back here. `scripts/latency-sweep.sh` documents every
knob it takes.

The campaign was run from a working tree that had not been committed, so the
`manifest.json` of every run holds the diff of the files that were tracked
and changed. It does not hold the files that were never added to the index,
because `git diff` does not hold them, and the mode itself was one of those.
`source.patch` is the whole change, the tracked diff as the manifest recorded
it and the files that were not in the index as new files, and it is the source
the campaign was built from:

```sh
git worktree add --detach /tmp/campaign 079b218
cd /tmp/campaign && git apply /path/to/source.patch
```

The patched tree has the source tree digest the run recorded in every
manifest, `52875185bd69bc52c9a8460da2e94650c134b4de57155fcb5e825475d5daf817`.
The digest is the part of the source that does not depend on where the tree
is built. The binary's bytes do: the absolute build root is compiled into
them, so the hash in `host.txt` comes back only from a build at the same
absolute path, with the same target directory layout. The manifest now keeps
the files that were not in the index, with the content of the ones that are
text and small enough, so a run of an uncommitted tree carries its own
source; that came after this campaign, which is why this one needed the
patch.

The `host.txt` holds the hash of the binary.
