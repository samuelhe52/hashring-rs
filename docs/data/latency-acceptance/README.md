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
two of them, and by 6.8% within the one the report is written from.

## How to reproduce

```sh
cargo build --release
./scripts/latency-sweep.sh
```

The script measures the capacity with `performance` mode, takes the target
rates from it, runs the campaign, the ladder and the virtual node comparison,
and writes the tables back here. `scripts/latency-sweep.sh` documents every
knob it takes.

## The source of the run

The campaign was run from the commit it was built from, `52adf85`, with no
tracked file changed. Every run's `manifest.json` holds that commit, an empty
`git diff`, and the same source tree digest at build time and at run time,
so the source is a checkout away:

```sh
git checkout 52adf85
```

The manifests of this run do report `source_reproducible: false`, for one
reason that was the script's own: it wrote the `host.txt` into `<run>/`
before the first run started, so `git status` of every run saw that one
untracked file. The diff of every manifest is empty and nothing in the
source tree differs from the commit. The script writes the `host.txt` after
the last run now, so a run of a clean tree reports
`source_reproducible: true` — which needs the tables of the run before it
committed, because they live in this tracked directory too.

A run of a tree that was never committed carries its source too: the manifest
keeps the files that are not in the index, with the content of the ones that
are text and small enough. The campaign the report replaces needed a patch
file for exactly that reason — its mode and its script were never added to
the index — and the patch was dropped when the campaign was.

The `host.txt` holds the hash of the binary. The build is deterministic at a
fixed path, but the absolute build root is compiled into the binary, so the
hash comes back only from a build at the same path with the same target
directory layout; the source tree digest comes back from any path.
