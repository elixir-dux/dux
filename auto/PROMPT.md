# Autoresearch Prompt

Paste this into a Claude Code session at `~/src/tmp/dux`:

---

You are running an autonomous performance optimization loop on the Dux dataframe library (Elixir). Your goal is to make Dux faster, bringing its performance closer to Explorer/Polars.

## Setup

Read these two files first — they contain everything you need:

- `auto/autoresearch.md` — Objective, metrics, how to run, file scope, constraints, architecture, baseline, and progress log
- `auto/autoresearch.ideas.md` — Ideas to try, dead ends to avoid, and learnings

## The loop

1. Read both docs above
2. Pick the highest-impact untried idea (or form a new hypothesis by reading the source code)
3. Read the relevant source files under `lib/` to understand the current implementation
4. Implement the change
5. Run: `bash auto/autoresearch.sh` (compiles, runs all tests, benchmarks best-of-3). Timeout ~3 min.
6. Check `combined_ratio` in the METRIC output. Baseline is in the progress log.
7. **If improved**: `git add` the changed files under `lib/`, commit with a descriptive `perf:` message, update the progress log in `auto/autoresearch.md` and mark the idea in `auto/autoresearch.ideas.md`
8. **If regressed or tests failed**: revert with `git checkout -- lib/`, note what you learned in the dead ends section of `auto/autoresearch.ideas.md`
9. Go to step 2. **NEVER STOP.**

## Rules

- Only edit files under `lib/`. Never edit `test/`, `auto/bench_quick.exs`, `auto/autoresearch.sh`, `bench/`, or `mix.exs`
- All tests must pass before a commit counts
- Don't change the public API
- Keep each experiment as a separate commit
- Update the docs (progress log + ideas) after every experiment, whether it succeeded or failed
- If you're unsure whether a change is safe, run the gate script — tests are the source of truth
