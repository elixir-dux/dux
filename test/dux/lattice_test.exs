defmodule Dux.LatticeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Dux.Lattice
  alias Dux.Lattice.{Avg, Count, Hll, Max, Min, Sum, Welford}

  # ---------------------------------------------------------------------------
  # Property tests: commutativity + associativity for all lattices
  # ---------------------------------------------------------------------------

  describe "Sum lattice" do
    property "merge is commutative" do
      check all(a <- integer(), b <- integer()) do
        assert Sum.merge(a, b) == Sum.merge(b, a)
      end
    end

    property "merge is associative" do
      check all(a <- integer(), b <- integer(), c <- integer()) do
        assert Sum.merge(Sum.merge(a, b), c) == Sum.merge(a, Sum.merge(b, c))
      end
    end

    property "bottom is identity" do
      check all(a <- integer()) do
        assert Sum.merge(a, Sum.bottom()) == a
        assert Sum.merge(Sum.bottom(), a) == a
      end
    end

    test "finalize returns the value" do
      assert Sum.finalize(42) == 42
    end
  end

  describe "Count lattice" do
    property "merge is commutative" do
      check all(a <- positive_integer(), b <- positive_integer()) do
        assert Count.merge(a, b) == Count.merge(b, a)
      end
    end

    property "merge is associative" do
      check all(a <- positive_integer(), b <- positive_integer(), c <- positive_integer()) do
        assert Count.merge(Count.merge(a, b), c) == Count.merge(a, Count.merge(b, c))
      end
    end

    property "bottom is identity" do
      check all(a <- positive_integer()) do
        assert Count.merge(a, Count.bottom()) == a
        assert Count.merge(Count.bottom(), a) == a
      end
    end
  end

  describe "Min lattice" do
    property "merge is commutative" do
      check all(a <- integer(), b <- integer()) do
        assert Min.merge(a, b) == Min.merge(b, a)
      end
    end

    property "merge is associative" do
      check all(a <- integer(), b <- integer(), c <- integer()) do
        assert Min.merge(Min.merge(a, b), c) == Min.merge(a, Min.merge(b, c))
      end
    end

    property "bottom is identity" do
      check all(a <- integer()) do
        assert Min.merge(a, Min.bottom()) == a
        assert Min.merge(Min.bottom(), a) == a
      end
    end

    property "merge returns the minimum" do
      check all(a <- integer(), b <- integer()) do
        assert Min.merge(a, b) == min(a, b)
      end
    end

    test "finalize of bottom returns nil" do
      assert Min.finalize(:infinity) == nil
    end

    test "finalize of value returns value" do
      assert Min.finalize(42) == 42
    end
  end

  describe "Max lattice" do
    property "merge is commutative" do
      check all(a <- integer(), b <- integer()) do
        assert Max.merge(a, b) == Max.merge(b, a)
      end
    end

    property "merge is associative" do
      check all(a <- integer(), b <- integer(), c <- integer()) do
        assert Max.merge(Max.merge(a, b), c) == Max.merge(a, Max.merge(b, c))
      end
    end

    property "bottom is identity" do
      check all(a <- integer()) do
        assert Max.merge(a, Max.bottom()) == a
        assert Max.merge(Max.bottom(), a) == a
      end
    end

    property "merge returns the maximum" do
      check all(a <- integer(), b <- integer()) do
        assert Max.merge(a, b) == max(a, b)
      end
    end

    test "finalize of bottom returns nil" do
      assert Max.finalize(:neg_infinity) == nil
    end
  end

  describe "Avg lattice" do
    property "merge is commutative" do
      check all(
              s1 <- float(min: -1.0e12, max: 1.0e12),
              c1 <- positive_integer(),
              s2 <- float(min: -1.0e12, max: 1.0e12),
              c2 <- positive_integer()
            ) do
        assert Avg.merge({s1, c1}, {s2, c2}) == Avg.merge({s2, c2}, {s1, c1})
      end
    end

    property "merge is associative" do
      check all(
              s1 <- float(min: -1.0e6, max: 1.0e6),
              c1 <- integer(1..100),
              s2 <- float(min: -1.0e6, max: 1.0e6),
              c2 <- integer(1..100),
              s3 <- float(min: -1.0e6, max: 1.0e6),
              c3 <- integer(1..100)
            ) do
        a = {s1, c1}
        b = {s2, c2}
        c = {s3, c3}

        {left_s, left_c} = Avg.merge(Avg.merge(a, b), c)
        {right_s, right_c} = Avg.merge(a, Avg.merge(b, c))

        assert left_c == right_c
        assert_in_delta left_s, right_s, 1.0e-6
      end
    end

    property "bottom is identity" do
      check all(s <- float(min: -1.0e12, max: 1.0e12), c <- positive_integer()) do
        assert Avg.merge({s, c}, Avg.bottom()) == {s, c}
        assert Avg.merge(Avg.bottom(), {s, c}) == {s, c}
      end
    end

    test "finalize computes the mean" do
      assert Avg.finalize({30.0, 6}) == 5.0
    end

    test "finalize of zero count returns nil" do
      assert Avg.finalize({0, 0}) == nil
    end

    test "distributed avg matches local avg" do
      # Simulate: 3 workers, each with a subset
      # sum=10, count=3
      w1 = {10.0, 3}
      # sum=20, count=4
      w2 = {20.0, 4}
      # sum=15, count=3
      w3 = {15.0, 3}

      merged = w1 |> Avg.merge(w2) |> Avg.merge(w3)
      # 45/10
      assert Avg.finalize(merged) == 4.5
    end
  end

  describe "Hll lattice" do
    property "merge is commutative" do
      check all(a <- positive_integer(), b <- positive_integer()) do
        assert Hll.merge(a, b) == Hll.merge(b, a)
      end
    end

    property "merge is associative" do
      check all(a <- positive_integer(), b <- positive_integer(), c <- positive_integer()) do
        assert Hll.merge(Hll.merge(a, b), c) == Hll.merge(a, Hll.merge(b, c))
      end
    end

    property "bottom is identity" do
      check all(a <- positive_integer()) do
        assert Hll.merge(a, Hll.bottom()) == a
        assert Hll.merge(Hll.bottom(), a) == a
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Welford: property tests + correctness
  # ---------------------------------------------------------------------------

  describe "Welford lattice" do
    property "merge is commutative" do
      check all(
              vals_a <- list_of(float(min: -1.0e6, max: 1.0e6), min_length: 1, max_length: 50),
              vals_b <- list_of(float(min: -1.0e6, max: 1.0e6), min_length: 1, max_length: 50)
            ) do
        a = build_welford(vals_a)
        b = build_welford(vals_b)

        {n1, mean1, m2_1} = Welford.merge(a, b)
        {n2, mean2, m2_2} = Welford.merge(b, a)

        assert n1 == n2
        assert_in_delta mean1, mean2, 1.0e-8
        # Use relative tolerance for m2 — floating point accumulation
        # with large values can cause small absolute differences
        tolerance = max(abs(m2_1), abs(m2_2)) * 1.0e-10 + 1.0e-6
        assert_in_delta m2_1, m2_2, tolerance
      end
    end

    property "merge is associative" do
      check all(
              vals_a <- list_of(float(min: -1.0e4, max: 1.0e4), min_length: 1, max_length: 20),
              vals_b <- list_of(float(min: -1.0e4, max: 1.0e4), min_length: 1, max_length: 20),
              vals_c <- list_of(float(min: -1.0e4, max: 1.0e4), min_length: 1, max_length: 20)
            ) do
        a = build_welford(vals_a)
        b = build_welford(vals_b)
        c = build_welford(vals_c)

        {n1, mean1, m2_1} = Welford.merge(Welford.merge(a, b), c)
        {n2, mean2, m2_2} = Welford.merge(a, Welford.merge(b, c))

        assert n1 == n2
        assert_in_delta mean1, mean2, 1.0e-6
        assert_in_delta m2_1, m2_2, 1.0e-2
      end
    end

    property "bottom is identity" do
      check all(vals <- list_of(float(min: -1.0e6, max: 1.0e6), min_length: 1, max_length: 50)) do
        a = build_welford(vals)
        assert Welford.merge(a, Welford.bottom()) == a
        assert Welford.merge(Welford.bottom(), a) == a
      end
    end

    test "matches known variance for [2, 4, 4, 4, 5, 5, 7, 9]" do
      # Known: sample variance = 4.571428..., stddev = 2.138090...
      vals = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
      acc = build_welford(vals)

      assert_in_delta Welford.finalize(acc), 4.571428571428571, 1.0e-10
      assert_in_delta Welford.finalize_stddev(acc), 2.138089935299395, 1.0e-10
    end

    test "population variance for [2, 4, 4, 4, 5, 5, 7, 9]" do
      vals = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
      acc = build_welford(vals)

      assert_in_delta Welford.finalize_pop(acc), 4.0, 1.0e-10
      assert_in_delta Welford.finalize_stddev_pop(acc), 2.0, 1.0e-10
    end

    test "merging two partitions matches single-pass" do
      all_vals = Enum.map(1..100, &(&1 * 1.0))
      {left, right} = Enum.split(all_vals, 40)

      single_pass = build_welford(all_vals)
      merged = Welford.merge(build_welford(left), build_welford(right))

      assert_in_delta Welford.finalize(single_pass), Welford.finalize(merged), 1.0e-10
    end

    test "merging three partitions matches single-pass" do
      all_vals = Enum.map(1..100, &(&1 * 1.0))
      {p1, rest} = Enum.split(all_vals, 33)
      {p2, p3} = Enum.split(rest, 34)

      single = build_welford(all_vals)

      merged =
        build_welford(p1) |> Welford.merge(build_welford(p2)) |> Welford.merge(build_welford(p3))

      assert_in_delta Welford.finalize(single), Welford.finalize(merged), 1.0e-10
    end

    test "single value returns nil (n-1 = 0)" do
      acc = build_welford([42.0])
      assert Welford.finalize(acc) == nil
    end

    test "empty returns nil" do
      assert Welford.finalize(Welford.bottom()) == nil
    end

    test "numerically stable for large values with small variance" do
      # Classic cancellation scenario: values near 1e9 with small diffs
      vals = Enum.map(1..100, &(1.0e9 + &1 / 100))
      acc = build_welford(vals)

      # Should be close to variance of (1/100, 2/100, ..., 100/100)
      # = variance of 0.01..1.00 = 0.08417 (sample variance)
      assert Welford.finalize(acc) != nil
      assert Welford.finalize(acc) > 0
      assert_in_delta Welford.finalize(acc), 0.084175, 0.001
    end
  end

  # ---------------------------------------------------------------------------
  # Lattice.classify/1
  # ---------------------------------------------------------------------------

  describe "Lattice.classify/1" do
    test "classifies SUM" do
      assert Lattice.classify("SUM(x)") == Sum
    end

    test "classifies COUNT" do
      assert Lattice.classify("COUNT(*)") == Count
      assert Lattice.classify("COUNT(x)") == Count
    end

    test "classifies MIN" do
      assert Lattice.classify("MIN(x)") == Min
    end

    test "classifies MAX" do
      assert Lattice.classify("MAX(x)") == Max
    end

    test "classifies AVG" do
      assert Lattice.classify("AVG(x)") == Avg
    end

    test "classifies STDDEV variants" do
      assert Lattice.classify("STDDEV(x)") == Welford
      assert Lattice.classify("STDDEV_SAMP(x)") == Welford
      assert Lattice.classify("STDDEV_POP(x)") == Welford
      assert Lattice.classify("VARIANCE(x)") == Welford
      assert Lattice.classify("VAR_SAMP(x)") == Welford
      assert Lattice.classify("VAR_POP(x)") == Welford
    end

    test "classifies APPROX_COUNT_DISTINCT" do
      assert Lattice.classify("APPROX_COUNT_DISTINCT(x)") == Hll
    end

    test "returns nil for COUNT(DISTINCT ...) — not lattice-mergeable" do
      assert Lattice.classify("COUNT(DISTINCT x)") == nil
    end

    test "returns nil for unknown aggregates" do
      assert Lattice.classify("MEDIAN(x)") == nil
      assert Lattice.classify("STRING_AGG(x, ',')") == nil
      assert Lattice.classify("FIRST(x)") == nil
    end

    test "returns nil for non-string" do
      assert Lattice.classify(42) == nil
      assert Lattice.classify(nil) == nil
    end
  end

  # ---------------------------------------------------------------------------
  # PipelineSplitter lattice metadata
  # ---------------------------------------------------------------------------

  describe "PipelineSplitter lattice metadata" do
    alias Dux.Remote.PipelineSplitter

    test "SUM + COUNT pipeline is streaming compatible" do
      ops = [
        {:group_by, ["region"]},
        {:summarise, [{"total", "SUM(amount)"}, {"n", "COUNT(*)"}]}
      ]

      result = PipelineSplitter.split(ops)

      assert result.streaming_compatible? == true
      assert result.lattice_map == %{"total" => Sum, "n" => Count}
    end

    test "MIN + MAX pipeline is streaming compatible" do
      ops = [{:summarise, [{"lo", "MIN(x)"}, {"hi", "MAX(x)"}]}]
      result = PipelineSplitter.split(ops)

      assert result.streaming_compatible? == true
      assert result.lattice_map == %{"lo" => Min, "hi" => Max}
    end

    test "AVG pipeline is streaming compatible" do
      ops = [{:summarise, [{"mean", "AVG(x)"}]}]
      result = PipelineSplitter.split(ops)

      assert result.streaming_compatible? == true
      assert result.lattice_map == %{"mean" => Avg}
    end

    test "STDDEV pipeline is streaming compatible" do
      ops = [{:summarise, [{"sd", "STDDEV(x)"}]}]
      result = PipelineSplitter.split(ops)

      assert result.streaming_compatible? == true
      assert result.lattice_map == %{"sd" => Welford}
    end

    test "mixed lattice + non-lattice is not streaming compatible" do
      ops = [{:summarise, [{"total", "SUM(x)"}, {"mid", "MEDIAN(x)"}]}]
      result = PipelineSplitter.split(ops)

      assert result.streaming_compatible? == false
      # lattice_map still has the classifiable ones
      assert result.lattice_map == %{"total" => Sum}
    end

    test "COUNT(DISTINCT ...) makes pipeline non-streaming" do
      ops = [{:summarise, [{"n", "COUNT(DISTINCT x)"}]}]
      result = PipelineSplitter.split(ops)

      assert result.streaming_compatible? == false
    end

    test "no summarise → not streaming compatible, empty lattice_map" do
      ops = [{:filter, "x > 10"}, {:sort_by, [{:asc, "x"}]}]
      result = PipelineSplitter.split(ops)

      assert result.streaming_compatible? == false
      assert result.lattice_map == %{}
    end

    test "all common aggregates together are streaming compatible" do
      ops = [
        {:group_by, ["g"]},
        {:summarise,
         [
           {"total", "SUM(x)"},
           {"n", "COUNT(*)"},
           {"lo", "MIN(x)"},
           {"hi", "MAX(x)"},
           {"avg_x", "AVG(x)"},
           {"sd", "STDDEV(x)"}
         ]}
      ]

      result = PipelineSplitter.split(ops)
      assert result.streaming_compatible? == true
      assert map_size(result.lattice_map) == 6
    end

    test "existing agg_rewrites still work alongside lattice metadata" do
      ops = [
        {:group_by, ["g"]},
        {:summarise, [{"total", "SUM(x)"}, {"mean", "AVG(x)"}]}
      ]

      result = PipelineSplitter.split(ops)

      # AVG should be rewritten
      assert Map.has_key?(result.agg_rewrites, "mean")
      assert match?({:avg, _, _}, result.agg_rewrites["mean"])

      # Lattice metadata should also be present
      assert result.streaming_compatible? == true
      assert result.lattice_map["total"] == Sum
      assert result.lattice_map["mean"] == Avg
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "Welford with identical values has zero variance" do
      vals = List.duplicate(42.0, 100)
      acc = build_welford(vals)
      assert_in_delta Welford.finalize(acc), 0.0, 1.0e-10
    end

    test "Welford merge of 100 single-value partitions" do
      # Each partition has exactly one value — the most adversarial split
      vals = Enum.map(1..100, &(&1 * 1.0))

      merged =
        vals
        |> Enum.map(&build_welford([&1]))
        |> Enum.reduce(Welford.bottom(), &Welford.merge(&2, &1))

      single = build_welford(vals)
      assert_in_delta Welford.finalize(merged), Welford.finalize(single), 1.0e-10
    end

    test "Min/Max merge with extreme values" do
      assert Min.merge(-9_999_999, 9_999_999) == -9_999_999
      assert Max.merge(-9_999_999, 9_999_999) == 9_999_999
    end

    test "Avg with very uneven partition sizes" do
      # 1 value vs 10000 values — the merge must weight correctly
      small = {100.0, 1}
      large = {50_000.0, 10_000}

      merged = Avg.merge(small, large)
      # (100 + 50000) / 10001 = 5.009999...
      assert_in_delta Avg.finalize(merged), 50_100.0 / 10_001, 1.0e-10
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    property "Welford: any split of data gives same variance as single pass" do
      check all(
              vals <- list_of(float(min: -1.0e4, max: 1.0e4), min_length: 2, max_length: 100),
              split_point <- integer(1..(length(vals) - 1))
            ) do
        {left, right} = Enum.split(vals, split_point)

        single = build_welford(vals)
        merged = Welford.merge(build_welford(left), build_welford(right))

        assert_in_delta Welford.finalize(single), Welford.finalize(merged), 1.0e-6
      end
    end

    property "folding N partitions in any order gives same result" do
      check all(
              partitions <-
                list_of(
                  list_of(float(min: -1000.0, max: 1000.0), min_length: 1, max_length: 10),
                  min_length: 2,
                  max_length: 5
                )
            ) do
        accs = Enum.map(partitions, &build_welford/1)

        # Merge left-to-right
        lr = Enum.reduce(accs, Welford.bottom(), &Welford.merge(&2, &1))
        # Merge right-to-left
        rl = Enum.reduce(Enum.reverse(accs), Welford.bottom(), &Welford.merge(&2, &1))

        {n1, mean1, _} = lr
        {n2, mean2, _} = rl
        assert n1 == n2
        assert_in_delta mean1, mean2, 1.0e-8

        # Variance should match
        v1 = Welford.finalize(lr)
        v2 = Welford.finalize(rl)

        if v1 != nil and v2 != nil do
          assert_in_delta v1, v2, 1.0e-4
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp build_welford(values) do
    Enum.reduce(values, Welford.bottom(), &Welford.ingest(&2, &1))
  end
end
