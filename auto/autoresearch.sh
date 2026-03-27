#!/usr/bin/env bash
#
# Autoresearch gate script for Dux performance optimization.
#
# Runs: compile → test → benchmark → output metrics.
# Exit 0 = success (metrics are valid). Non-zero = broken build.
#
# Usage:
#   bash auto/autoresearch.sh

set -euo pipefail

cd "$(dirname "$0")/.."

# Source profile to pick up Elixir/Erlang PATH (needed for non-interactive SSH)
# shellcheck disable=SC1090
[ -f "$HOME/.bashrc" ] && source "$HOME/.bashrc" 2>/dev/null || true

echo "=== Step 1: Compile ==="
if ! mix compile --warnings-as-errors 2>&1; then
  echo "FATAL: compilation failed"
  exit 1
fi

echo ""
echo "=== Step 2: Tests ==="
# Graph tests excluded: they OOM on machines with <8GB RAM (not related to perf work).
# All core pipeline/verb/IO/query tests are included — these cover the hot path.
TEST_FILES=(
  test/dux/verb_test.exs test/dux/io_test.exs test/dux/query_test.exs
  test/dux/backend_test.exs test/dux/connection_test.exs test/dux/json_test.exs
  test/dux/security_test.exs test/dux/telemetry_test.exs test/dux/types_property_test.exs
  test/dux/nx_test.exs test/dux/cross_source_test.exs test/dux/asof_join_test.exs
  test/dux/fts_test.exs test/dux/e2e_test.exs test/dux/dataset_e2e_test.exs
  test/dux/excel_test.exs test/dux/adbc_edge_cases_test.exs test/dux/lattice_test.exs
  test/dux_test.exs
)
if ! mix test "${TEST_FILES[@]}" --exclude distributed --exclude container 2>&1; then
  echo "FATAL: tests failed"
  exit 1
fi

echo ""
echo "=== Step 3: Benchmark (best of 3 runs) ==="

best_combined=""
best_output=""

for run in 1 2 3; do
  echo "--- Run $run/3 ---"
  output=$(mix run auto/bench_quick.exs 2>&1)
  echo "$output"

  combined=$(echo "$output" | grep "^METRIC combined_ratio=" | sed 's/METRIC combined_ratio=//')

  if [ -z "$combined" ]; then
    echo "FATAL: benchmark did not produce combined_ratio metric"
    exit 1
  fi

  # Keep the run with the lowest combined_ratio (best for Dux)
  if [ -z "$best_combined" ] || [ "$(echo "$combined < $best_combined" | bc -l)" = "1" ]; then
    best_combined="$combined"
    best_output="$output"
  fi
done

echo ""
echo "=== Best run (combined_ratio=$best_combined) ==="
echo "$best_output" | grep "^METRIC"

# Check for stale views (memory leak regression)
stale=$(echo "$best_output" | grep "^METRIC stale_views=" | sed 's/METRIC stale_views=//')
if [ -n "$stale" ] && [ "$stale" != "0" ]; then
  echo ""
  echo "WARNING: $stale stale views detected after benchmark — possible memory leak"
fi
