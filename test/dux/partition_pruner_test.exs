defmodule Dux.Remote.PartitionPrunerTest do
  use ExUnit.Case, async: true

  alias Dux.Remote.PartitionPruner

  # ---------------------------------------------------------------------------
  # Hive partition extraction
  # ---------------------------------------------------------------------------

  describe "extract_hive_partitions/1" do
    test "extracts key=value pairs from path segments" do
      assert PartitionPruner.extract_hive_partitions(
               "s3://bucket/year=2024/month=01/data.parquet"
             ) == %{"year" => "2024", "month" => "01"}
    end

    test "handles local paths" do
      assert PartitionPruner.extract_hive_partitions(
               "/data/output/region=US/day=2024-01-15/part_0.parquet"
             ) == %{"region" => "US", "day" => "2024-01-15"}
    end

    test "returns empty map for non-partitioned paths" do
      assert PartitionPruner.extract_hive_partitions("s3://bucket/data.parquet") == %{}
      assert PartitionPruner.extract_hive_partitions("/tmp/flat/file.parquet") == %{}
    end

    test "ignores segments that aren't key=value" do
      assert PartitionPruner.extract_hive_partitions("s3://bucket/data/year=2024/file.parquet") ==
               %{"year" => "2024"}
    end

    test "handles multiple = in value" do
      # "key=val=ue" → key="val=ue" — split only on first =
      # Actually our split uses parts: 2, so this works
      assert PartitionPruner.extract_hive_partitions("/data/tag=a=b/file.parquet") ==
               %{"tag" => "a=b"}
    end
  end

  # ---------------------------------------------------------------------------
  # Pruning
  # ---------------------------------------------------------------------------

  describe "prune/2" do
    test "prunes files that don't match equality filter" do
      files = [
        "/data/year=2024/month=01/a.parquet",
        "/data/year=2024/month=02/b.parquet",
        "/data/year=2023/month=12/c.parquet"
      ]

      ops = [{:filter, "\"year\" = 2024"}]
      result = PartitionPruner.prune(files, ops)

      assert length(result) == 2
      assert Enum.all?(result, &String.contains?(&1, "year=2024"))
    end

    test "prunes on multiple partition columns" do
      files = [
        "/data/year=2024/month=01/a.parquet",
        "/data/year=2024/month=02/b.parquet",
        "/data/year=2023/month=01/c.parquet"
      ]

      ops = [{:filter, "\"year\" = 2024 AND \"month\" = '01'"}]
      result = PartitionPruner.prune(files, ops)

      assert result == ["/data/year=2024/month=01/a.parquet"]
    end

    test "handles unquoted column names" do
      files = [
        "/data/year=2024/a.parquet",
        "/data/year=2023/b.parquet"
      ]

      ops = [{:filter, "year = 2024"}]
      result = PartitionPruner.prune(files, ops)

      assert result == ["/data/year=2024/a.parquet"]
    end

    test "passes through files when filter column is not a partition column" do
      files = [
        "/data/year=2024/a.parquet",
        "/data/year=2023/b.parquet"
      ]

      # "status" is not a partition column — all files pass
      ops = [{:filter, "status = 'active'"}]
      result = PartitionPruner.prune(files, ops)

      assert result == files
    end

    test "passes through all files when no filter ops" do
      files = ["/data/year=2024/a.parquet", "/data/year=2023/b.parquet"]
      assert PartitionPruner.prune(files, []) == files
    end

    test "passes through all files when ops don't contain filters" do
      files = ["/data/year=2024/a.parquet"]
      ops = [{:mutate, "x * 2 AS doubled"}, {:select, [:x]}]
      assert PartitionPruner.prune(files, ops) == files
    end

    test "handles non-partitioned files mixed with partitioned" do
      files = [
        "/data/year=2024/a.parquet",
        "/data/flat_file.parquet",
        "/data/year=2023/b.parquet"
      ]

      ops = [{:filter, "year = 2024"}]
      result = PartitionPruner.prune(files, ops)

      # flat_file has no year partition → passes through (safe)
      assert "/data/year=2024/a.parquet" in result
      assert "/data/flat_file.parquet" in result
      assert "/data/year=2023/b.parquet" not in result
    end

    test "handles multiple separate filter ops" do
      files = [
        "/data/year=2024/month=01/a.parquet",
        "/data/year=2024/month=02/b.parquet",
        "/data/year=2023/month=01/c.parquet"
      ]

      # Two separate filter ops (as opposed to AND in one string)
      ops = [{:filter, "year = 2024"}, {:filter, "month = '01'"}]
      result = PartitionPruner.prune(files, ops)

      assert result == ["/data/year=2024/month=01/a.parquet"]
    end

    test "can prune all files" do
      files = [
        "/data/year=2023/a.parquet",
        "/data/year=2022/b.parquet"
      ]

      ops = [{:filter, "year = 2024"}]
      result = PartitionPruner.prune(files, ops)

      assert result == []
    end

    test "handles string values with single quotes" do
      files = [
        "/data/region=US/a.parquet",
        "/data/region=EU/b.parquet"
      ]

      ops = [{:filter, "region = 'US'"}]
      result = PartitionPruner.prune(files, ops)

      assert result == ["/data/region=US/a.parquet"]
    end
  end
end
