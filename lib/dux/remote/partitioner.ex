defmodule Dux.Remote.Partitioner do
  @moduledoc false

  # Assigns data partitions to workers.
  #
  # For Parquet glob sources, splits files across workers.
  # For other sources, currently sends the full source to all workers
  # (the coordinator will merge the results).

  @doc """
  Partition a source across workers. Returns a list of `{worker_pid, %Dux{}}` tuples,
  each with the source narrowed to that worker's partition.
  """
  def assign(%Dux{} = pipeline, workers, opts \\ []) do
    strategy = Keyword.get(opts, :strategy, :round_robin)
    assign_strategy(pipeline, workers, strategy)
  end

  # Parquet glob — split files across workers
  defp assign_strategy(%Dux{source: {:parquet, path, opts}} = pipeline, workers, :round_robin)
       when is_binary(path) do
    case expand_glob(path) do
      {:ok, files} when length(files) > 1 ->
        distribute_files(files, workers, pipeline, opts)

      _ ->
        replicate(pipeline, workers)
    end
  end

  # CSV with glob-like path — no splitting (CSV globs less common)
  defp assign_strategy(pipeline, workers, :round_robin) do
    replicate(pipeline, workers)
  end

  defp distribute_files(files, workers, pipeline, opts) do
    partitions = chunk_round_robin(files, length(workers))

    Enum.zip(workers, partitions)
    |> Enum.map(fn {worker, file_group} ->
      partitioned_source =
        case file_group do
          [single] -> {:parquet, single, opts}
          multiple -> {:parquet_list, multiple, opts}
        end

      {worker, %{pipeline | source: partitioned_source}}
    end)
  end

  # Replicate: every worker gets the same pipeline
  defp replicate(pipeline, workers) do
    Enum.map(workers, fn worker -> {worker, pipeline} end)
  end

  # Split a list into N groups round-robin
  defp chunk_round_robin(items, n) when n > 0 do
    items
    |> Enum.with_index()
    |> Enum.group_by(fn {_item, idx} -> rem(idx, n) end, fn {item, _idx} -> item end)
    |> Map.values()
    |> pad_to(n)
  end

  # Ensure we have exactly n groups (some may be empty if fewer items than workers)
  defp pad_to(groups, n) when length(groups) >= n, do: Enum.take(groups, n)
  defp pad_to(groups, n), do: groups ++ List.duplicate([], n - length(groups))

  # Expand a glob pattern to a list of files.
  # For local files, use Path.wildcard. For S3/HTTP, return the glob as-is
  # (DuckDB handles remote globs natively).
  defp expand_glob(path) do
    cond do
      String.starts_with?(path, "s3://") or String.starts_with?(path, "http") ->
        # Can't expand remote globs locally — let DuckDB handle it per worker
        {:ok, [path]}

      String.contains?(path, "*") or String.contains?(path, "?") ->
        files = Path.wildcard(path)

        if files == [] do
          {:ok, [path]}
        else
          {:ok, files}
        end

      true ->
        {:ok, [path]}
    end
  end
end
