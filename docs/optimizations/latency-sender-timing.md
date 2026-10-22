# The latency mode's sender cannot use a runtime timer

The `latency` mode measures the latency of a load that is offered at a fixed
rate. Every request's latency starts at the moment it was **due**, not at the
moment it was sent, so a slow cluster cannot hide its tail behind a delayed
send. That makes the sender's own timing part of the measurement: if the
sender wakes up late, every request carries that delay, and the number the
run reports is the sender's delay plus the cluster's.

The first version of the sender used `tokio::time::sleep_until`, one sleep per
request. It produced numbers that were plausible and wrong: at 500 operations
per second it reported a p50 of 1493 us for a `GET`, and the p50 barely moved
between 200 and 20000 operations per second. The cluster's round trip is
tens of microseconds, so a p50 of 1493 us was the sender, not the cluster.

## What the timer does

Measured on this host, 200 sleeps per interval, the overshoot of each wake-up
against the deadline it was given:

| Wait | interval | p50 | p90 | max |
| --- | --- | --- | --- | --- |
| `tokio::time::sleep_until` | 50 us | 847 us | 1297 us | 1604 us |
| `tokio::time::sleep_until` | 1 ms | 733 us | 1156 us | 2116 us |
| `tokio::time::sleep_until` | 5 ms | 762 us | 1162 us | 1252 us |
| `std::thread::sleep` | 50 us | 52 us | 53 us | 57 us |
| `std::thread::sleep` | 5 ms | 56 us | 64 us | 412 us |
| `std::thread::sleep` + spin | 50 us | 0 us | 0 us | 67 us |
| `std::thread::sleep` + spin | 5 ms | 0 us | 0 us | 660 us |

The runtime timer is a millisecond-granularity wheel, so its wake-up is late
by about three quarters of a millisecond whatever the deadline was. The
kernel's own timer is late by about 50 us, and a spin for the last few
microseconds removes even that.

The overshoot does not accumulate: the sender sleeps until the **absolute**
deadline of the next request, so a late wake-up does not push the schedule
forward. That is why the offered rate was right and only the latency was
wrong. At a 50 us interval the rate itself is off by 6%, because there the
overshoot is a fifth of the interval.

## What the sender does now

The sender runs on a thread of its own, outside the runtime, and waits against
the wall clock in two parts: a sleep that ends a little before the deadline,
and a spin for the rest. The margin is the overshoot the last sleep showed,
plus 20 us, so the spin stays short whatever the host's timer does. A request
is handed to the runtime only once it is due, and the runtime's own wake-up
then adds its few microseconds to the request's latency.

The cost of the spin is bounded by the margin, not by the interval, so it is
about 20 us per request: a fifth of a core at 10000 operations per second.

## What it bought

The same run, 3 nodes, 128-byte values, `GET:PUT:DELETE` of `90:9:1`:

| | p50 `GET` | p99 `GET` | max `GET` |
| --- | --- | --- | --- |
| runtime timer | 1493 us | 2223 us | 2767 us |
| dedicated thread | 142 us | 589 us | 1642 us |

The p50 fell by a factor of ten and the number now means something: the
latencies a cluster is judged by are tens of microseconds, and an instrument
that is a millisecond late cannot measure them.

The rule this leaves behind: a measurement that resolves microseconds needs a
clock that is not a millisecond wide, and the cost of that clock is part of
the measurement budget.

## Trade-offs

- The sender needs a thread of its own, so a phase cannot be driven from a
  plain async task. The requests themselves still run on the runtime.
- The spin holds a core for a few microseconds per request. At 10000
  operations per second that is about a fifth of a core, and it competes with
  the nodes for the host, which is the same tradeoff every accurate load
  generator makes.
- The margin adapts to the host's timer, so the sender behaves the same on a
  host with a coarse timer and on one with a fine timer, at the cost of a
  margin that is only right after the first sleep.
