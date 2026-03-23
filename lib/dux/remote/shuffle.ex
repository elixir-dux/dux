defmodule Dux.Remote.Shuffle do
  @moduledoc """
  Distributed shuffle join for large-large joins.

  When both sides of a join are too large to broadcast, the shuffle:
  1. Hash-partitions both sides by join key across workers
  2. Ships each partition to its assigned worker (credit-based backpressure)
  3. Each worker joins its co-located partitions locally
  4. Coordinator merges results

  ## Usage

      Dux.Remote.Shuffle.execute(large_left, large_right,
        on: :join_key,
        workers: workers
      )

  Over-partitions by 4x the worker count to absorb moderate skew.
  """

  import Dux.SQL.Helpers, only: [qi: 1]
  alias Dux.Remote.{Merger, Partitioner, Worker}

  @over_partition_factor 4
  @max_retries 3

  @doc """
  Execute a shuffle join between two large datasets.

  ## Options

    * `:on` — join column(s). Can be an atom, a list of atoms (same name both sides),
      or a list of `{left_col, right_col}` tuples for different column names.
    * `:how` — join type (default: `:inner`)
    * `:workers` — worker PIDs (default: all from `:pg`)
    * `:timeout` — per-operation timeout (default: `:infinity`)
  """
  def execute(%Dux{} = left, %Dux{} = right, opts) do
    workers = Keyword.get_lazy(opts, :workers, &Worker.list/0)
    n_workers = length(workers)
    factor = Keyword.get(opts, :over_partition_factor, @over_partition_factor)
    n_buckets = n_workers * factor

    :telemetry.span(
      [:dux, :distributed, :shuffle],
      %{n_workers: n_workers, n_buckets: n_buckets},
      fn ->
        result = execute_with_retry(left, right, opts, 0)
        {result, %{n_workers: n_workers, n_buckets: n_buckets}}
      end
    )
  end

  defp execute_with_retry(left, right, opts, attempt) do
    workers = Keyword.get_lazy(opts, :workers, &Worker.list/0)
    on = Keyword.fetch!(opts, :on)
    how = Keyword.get(opts, :how, :inner)
    timeout = Keyword.get(opts, :timeout, :infinity)

    # Normalize join columns into [{left_col, right_col}, ...]
    on_pairs = normalize_on(on)
    left_cols = Enum.map(on_pairs, fn {l, _r} -> to_string(l) end)
    right_cols = Enum.map(on_pairs, fn {_l, r} -> to_string(r) end)

    n_workers = length(workers)
    factor = Keyword.get(opts, :over_partition_factor, @over_partition_factor)
    n_buckets = n_workers * factor
    stage_id = :erlang.unique_integer([:positive])

    if workers == [] do
      raise ArgumentError, "no workers available for shuffle join"
    end

    try do
      # Phase 1: Hash-partition both sides on each worker
      # Each worker processes only its slice (1/N) of the data to avoid duplication
      left_sliced = slice_for_workers(left, workers)
      right_sliced = slice_for_workers(right, workers)

      left_partitions = hash_partition_all(left_sliced, left_cols, n_buckets, timeout)
      right_partitions = hash_partition_all(right_sliced, right_cols, n_buckets, timeout)

      # Skew detection: flag heavy buckets
      detect_skew(left_partitions, right_partitions, n_buckets)

      # Phase 2: Shuffle exchange — send each bucket to the assigned worker
      # Assign buckets to workers round-robin
      bucket_to_worker = assign_buckets(n_buckets, workers)

      exchange_partitions(
        left_partitions,
        bucket_to_worker,
        workers,
        "shuffle_l_#{stage_id}",
        timeout
      )

      exchange_partitions(
        right_partitions,
        bucket_to_worker,
        workers,
        "shuffle_r_#{stage_id}",
        timeout
      )

      # Phase 3: Local join on each worker
      join_results =
        local_join_all(
          workers,
          "shuffle_l_#{stage_id}",
          "shuffle_r_#{stage_id}",
          on_pairs,
          how,
          timeout
        )

      # Phase 4: Merge on coordinator
      result =
        if join_results == [] do
          # No matching rows — return empty Dux
          Dux.from_query("SELECT 1 WHERE false") |> Dux.compute()
        else
          Merger.merge_to_dux(join_results, %Dux{ops: []})
        end

      # Cleanup
      cleanup(workers, stage_id)

      result
    rescue
      e ->
        cleanup(workers, stage_id)

        if attempt < @max_retries do
          execute_with_retry(left, right, opts, attempt + 1)
        else
          reraise e, __STACKTRACE__
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Phase 1: Hash partition
  # ---------------------------------------------------------------------------

  # Assign each worker a slice of the data to avoid duplication for replicated sources.
  # For Parquet globs, the Partitioner already handles this. For other sources,
  # we use NTILE to give each worker 1/N of the rows.
  defp slice_for_workers(%Dux{source: {:parquet, path, _}} = pipeline, workers)
       when is_binary(path) and byte_size(path) > 0 do
    # Parquet glob: use the existing partitioner
    Partitioner.assign(pipeline, workers)
  end

  defp slice_for_workers(pipeline, workers) do
    # Table refs are connection-local — materialize to list for workers
    safe_pipeline = ensure_worker_safe(pipeline)
    n = length(workers)

    workers
    |> Enum.with_index(1)
    |> Enum.map(fn {worker, idx} ->
      sliced =
        safe_pipeline
        |> Dux.mutate_with(__slice: "NTILE(#{n}) OVER ()")
        |> Dux.filter_with("__slice = #{idx}")
        |> Dux.discard([:__slice])

      {worker, sliced}
    end)
  end

  defp ensure_worker_safe(%Dux{source: {:table, %Dux.TableRef{} = ref}} = pipeline) do
    conn = Dux.Connection.get_conn()
    rows = Dux.Backend.table_to_rows(conn, ref)
    %{pipeline | source: {:list, rows}}
  end

  defp ensure_worker_safe(pipeline), do: pipeline

  defp hash_partition_all(worker_pipelines, on_cols, n_buckets, timeout) do
    # on_cols is a list of column names (strings). For single column, Worker accepts
    # a string; for multiple, Worker accepts a list.
    partition_key = if length(on_cols) == 1, do: hd(on_cols), else: on_cols

    worker_pipelines
    |> Task.async_stream(
      fn {worker, pipeline} ->
        {:ok, buckets} =
          Worker.hash_partition(worker, pipeline, partition_key, n_buckets, timeout)

        {worker, buckets}
      end,
      timeout: timeout,
      max_concurrency: length(worker_pipelines)
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  # ---------------------------------------------------------------------------
  # Phase 2: Shuffle exchange
  # ---------------------------------------------------------------------------

  # Detect skewed buckets and emit telemetry.
  # A bucket is "heavy" if it exceeds 5x the mean bucket size.
  defp detect_skew(left_partitions, right_partitions, n_buckets) do
    left_sizes = bucket_sizes(left_partitions)
    right_sizes = bucket_sizes(right_partitions)

    left_heavy = find_heavy_buckets(left_sizes, n_buckets)
    right_heavy = find_heavy_buckets(right_sizes, n_buckets)

    if left_heavy != [] or right_heavy != [] do
      :telemetry.execute(
        [:dux, :distributed, :shuffle, :skew_detected],
        %{
          left_heavy_count: length(left_heavy),
          right_heavy_count: length(right_heavy)
        },
        %{
          left_heavy_buckets: left_heavy,
          right_heavy_buckets: right_heavy,
          n_buckets: n_buckets
        }
      )
    end
  end

  defp bucket_sizes(partitions) do
    for {_worker, buckets} <- partitions,
        {bucket_id, ipc} <- buckets,
        ipc != nil,
        reduce: %{} do
      acc -> Map.update(acc, bucket_id, byte_size(ipc), &(&1 + byte_size(ipc)))
    end
  end

  defp find_heavy_buckets(sizes, _n_buckets, threshold_factor \\ 5) do
    values = Map.values(sizes)
    mean = if values == [], do: 0, else: div(Enum.sum(values), length(values))

    if mean == 0,
      do: [],
      else: for({id, size} <- sizes, size > threshold_factor * mean, do: id)
  end

  defp assign_buckets(n_buckets, workers) do
    for bucket_id <- 0..(n_buckets - 1), into: %{} do
      worker_idx = rem(bucket_id, length(workers))
      {bucket_id, Enum.at(workers, worker_idx)}
    end
  end

  defp exchange_partitions(worker_partitions, bucket_to_worker, _workers, table_prefix, _timeout) do
    # For each source worker's partitions, send each non-empty bucket
    # to its assigned target worker
    tasks =
      for {_source_worker, buckets} <- worker_partitions,
          {bucket_id, ipc} <- buckets,
          ipc != nil do
        target_worker = Map.fetch!(bucket_to_worker, bucket_id)
        table_name = "#{table_prefix}_b#{bucket_id}"

        Task.async(fn ->
          Worker.append_chunk(target_worker, table_name, ipc)
        end)
      end

    Task.await_many(tasks, 60_000)
  end

  # ---------------------------------------------------------------------------
  # Phase 3: Local join
  # ---------------------------------------------------------------------------

  defp local_join_all(workers, left_prefix, right_prefix, on_pairs, how, timeout) do
    join_type = join_type_sql(how)
    join_condition = build_join_condition(on_pairs)

    # Each worker joins all its left buckets against all its right buckets
    workers
    |> Task.async_stream(
      fn worker ->
        # Find which buckets this worker owns
        info = Worker.info(worker)
        left_tables = info.tables |> Enum.filter(&String.starts_with?(&1, left_prefix))
        right_tables = info.tables |> Enum.filter(&String.starts_with?(&1, right_prefix))

        if left_tables == [] or right_tables == [] do
          # No data for this worker — return empty
          nil
        else
          # UNION ALL left buckets, UNION ALL right buckets, then join
          left_union =
            Enum.map_join(left_tables, " UNION ALL ", &"SELECT * FROM #{qi(&1)}")

          right_union =
            Enum.map_join(right_tables, " UNION ALL ", &"SELECT * FROM #{qi(&1)}")

          join_sql = """
            SELECT * FROM (#{left_union}) __left
            #{join_type} (#{right_union}) __right
            ON #{join_condition}
          """

          pipeline = Dux.from_query(join_sql)
          {:ok, ipc} = Worker.execute(worker, pipeline, timeout)
          ipc
        end
      end,
      timeout: timeout,
      max_concurrency: length(workers)
    )
    |> Enum.map(fn {:ok, result} -> result end)
    |> Enum.filter(& &1)
  end

  # ---------------------------------------------------------------------------
  # Cleanup
  # ---------------------------------------------------------------------------

  defp cleanup(workers, stage_id) do
    for worker <- workers do
      try do
        info = Worker.info(worker)

        info.tables
        |> Enum.filter(fn name ->
          String.contains?(name, "shuffle_l_#{stage_id}") or
            String.contains?(name, "shuffle_r_#{stage_id}")
        end)
        |> Enum.each(&Worker.drop_table(worker, &1))
      catch
        _, _ -> :ok
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp join_type_sql(:inner), do: "INNER JOIN"
  defp join_type_sql(:left), do: "LEFT JOIN"
  defp join_type_sql(:right), do: "RIGHT JOIN"
  defp join_type_sql(:cross), do: "CROSS JOIN"

  # Normalize :on into [{left_col, right_col}, ...]
  defp normalize_on(col) when is_atom(col), do: [{col, col}]
  defp normalize_on(col) when is_binary(col), do: [{col, col}]

  defp normalize_on(cols) when is_list(cols) do
    Enum.map(cols, fn
      {left, right} -> {to_string(left), to_string(right)}
      col -> {to_string(col), to_string(col)}
    end)
  end

  # Build ON condition: __left."col1" = __right."col1" AND __left."col2" = __right."col2"
  defp build_join_condition(on_pairs) do
    Enum.map_join(on_pairs, " AND ", fn {left_col, right_col} ->
      "__left.#{qi(to_string(left_col))} = __right.#{qi(to_string(right_col))}"
    end)
  end
end
