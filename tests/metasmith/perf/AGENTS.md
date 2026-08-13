# tests/perf

Scale and wall-clock ceilings: does it still work, and still finish, at sizes the
other axes never reach — 10k-item libraries, 21k-instance plans.

Default marker: `slow`. Each test here is tens of seconds by design, which is the
whole reason the axis exists separately: while these lived alongside the
workspace-prep tests, the entire `bootstrap/` axis had to be marked slow to
contain them, and ~100 sub-millisecond tests sat out the daily loop.

A test belongs here when its assertion is about *cost* — a duration, a count of
operations, an O(n) claim. If it is about correctness and merely happens to use a
large fixture, it belongs in the axis that owns the behaviour.
