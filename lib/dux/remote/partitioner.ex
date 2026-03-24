defmodule Dux.Remote.Partitioner do
  @moduledoc false

  # Assigns data partitions to workers.
  #
  # For Parquet glob sources and DuckLake file manifests, splits files
  # across workers using size-balanced bin-packing when file sizes are
  # available, falling back to round-robin otherwise.
  # For other sources, sends the full source to all workers
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
        distribute_parquet_files(files, workers, pipeline, opts)

      _ ->
        replicate(pipeline, workers)
    end
  end

  # DuckLake — resolve the file manifest and distribute as parquet reads.
  # The coordinator resolves the DuckLake catalog into a {:ducklake_files, ...} source
  # containing the list of backing Parquet paths. Workers read these directly.
  defp assign_strategy(
         %Dux{source: {:ducklake_files, files}} = pipeline,
         workers,
         :round_robin
       ) do
    distribute_parquet_files(files, workers, pipeline, [])
  end

  # Other sources — no splitting
  defp assign_strategy(pipeline, workers, :round_robin) do
    replicate(pipeline, workers)
  end

  # ---------------------------------------------------------------------------
  # File distribution (size-balanced or round-robin fallback)
  # ---------------------------------------------------------------------------

  defp distribute_parquet_files(files, workers, pipeline, opts) do
    files_with_sizes = fetch_file_sizes(files)

    assignments =
      if files_with_sizes do
        bin_pack(files_with_sizes, length(workers))
      else
        chunk_round_robin(files, length(workers))
      end

    assignments
    |> Enum.zip(workers)
    |> Enum.map(fn {file_group, worker} ->
      source =
        case file_group do
          [] -> {:parquet_list, [], opts}
          [single] -> {:parquet, single, opts}
          multiple -> {:parquet_list, multiple, opts}
        end

      {worker, %{pipeline | source: source}}
    end)
    |> Enum.reject(fn
      {_worker, %{source: {:parquet_list, [], _}}} -> true
      _ -> false
    end)
  end

  # ---------------------------------------------------------------------------
  # Size-balanced bin-packing
  # ---------------------------------------------------------------------------

  # Greedy first-fit-decreasing: sort files largest-first, assign each to
  # the worker with the smallest current total load. Produces assignments
  # within 11/9 OPT + 6/9 of optimal for the multiprocessor scheduling problem.
  defp bin_pack(files_with_sizes, n_workers) do
    sorted = Enum.sort_by(files_with_sizes, fn {_file, size} -> size end, :desc)

    # Initialize worker loads: [{total_load, worker_index, [files]}]
    initial = Enum.map(0..(n_workers - 1), fn i -> {0, i, []} end)

    bins =
      Enum.reduce(sorted, initial, fn {file, size}, bins ->
        # Find the worker with the smallest load
        [{load, idx, files} | rest] = Enum.sort_by(bins, fn {load, _, _} -> load end)
        [{load + size, idx, [file | files]} | rest]
      end)

    # Return file lists ordered by worker index
    bins
    |> Enum.sort_by(fn {_load, idx, _files} -> idx end)
    |> Enum.map(fn {_load, _idx, files} -> Enum.reverse(files) end)
  end

  # ---------------------------------------------------------------------------
  # File size fetching (tiered)
  # ---------------------------------------------------------------------------

  # Returns [{file, size}] or nil if sizes unavailable.
  # Tiered: local File.stat (instant) > nil for remote (can't stat S3 locally).
  defp fetch_file_sizes(files) do
    if all_local?(files) do
      Enum.map(files, &stat_file/1)
    else
      # Remote files (S3/HTTP) — can't get sizes locally.
      # Falls back to round-robin via the nil return.
      nil
    end
  end

  defp stat_file(file) do
    case File.stat(file) do
      {:ok, %{size: size}} -> {file, size}
      _ -> {file, 0}
    end
  end

  defp all_local?(files) do
    Enum.all?(files, fn f ->
      not String.starts_with?(f, "s3://") and not String.starts_with?(f, "http")
    end)
  end

  # ---------------------------------------------------------------------------
  # Replicate / round-robin helpers
  # ---------------------------------------------------------------------------

  # Replicate: every worker gets the same pipeline.
  # Table refs are connection-local — convert to list source for workers.
  defp replicate(%Dux{source: {:table, %Dux.TableRef{} = ref}} = pipeline, workers) do
    conn = Dux.Connection.get_conn()
    rows = Dux.Backend.table_to_rows(conn, ref)
    worker_pipeline = %{pipeline | source: {:list, rows}}
    Enum.map(workers, fn worker -> {worker, worker_pipeline} end)
  end

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
