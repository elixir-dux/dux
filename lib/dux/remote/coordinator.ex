defmodule Dux.Remote.Coordinator do
  @moduledoc """
  Orchestrates distributed query execution across a cluster of Dux workers.

  The coordinator:
  1. Discovers available workers via `:pg`
  2. Partitions the data source across workers
  3. Fans out the pipeline to each worker via `Worker.execute/2`
  4. Merges partial results on the coordinator node

  ## Usage

      # Execute a pipeline across all available workers
      result = Dux.Remote.Coordinator.execute(pipeline)

      # Execute with specific workers
      result = Dux.Remote.Coordinator.execute(pipeline, workers: [w1, w2, w3])

  The result is a `%Dux{}` struct with the merged data.
  """

  alias Dux.Remote.{Merger, Partitioner, PipelineSplitter, Shuffle, StreamingMerger, Worker}
  import Dux.SQL.Helpers, only: [qi: 1]

  # Broadcast threshold: 256MB serialized Arrow IPC
  @broadcast_threshold 256 * 1024 * 1024

  @doc """
  Execute a `%Dux{}` pipeline across distributed workers.

  Partitions the source, fans out to workers, collects Arrow IPC results,
  and merges on the coordinator node.

  ## Options

    * `:workers` — list of worker PIDs (default: all workers from `:pg`)
    * `:timeout` — per-worker timeout in ms (default: `:infinity`)
    * `:strategy` — partitioning strategy (default: `:round_robin`)

  Returns a `%Dux{}` struct with the merged results.
  """
  def execute(%Dux{} = pipeline, opts \\ []) do
    workers = Keyword.get_lazy(opts, :workers, &Worker.list/0)
    timeout = Keyword.get(opts, :timeout, :infinity)
    strategy = Keyword.get(opts, :strategy, :round_robin)
    bcast_threshold = Keyword.get(opts, :broadcast_threshold, @broadcast_threshold)

    if workers == [] do
      raise ArgumentError, "no workers available for distributed execution"
    end

    # Resolve sources that can be partitioned at the coordinator level.
    # DuckLake attached sources → file manifest for direct parquet reads.
    pipeline = resolve_ducklake_source(pipeline)

    # Split pipeline: worker ops push down, coordinator ops apply post-merge
    %{
      worker_ops: worker_ops,
      coordinator_ops: coord_ops,
      agg_rewrites: rewrites,
      streaming_compatible?: streaming?
    } = split = PipelineSplitter.split(pipeline.ops)

    # Preprocess joins: broadcast/shuffle right sides that aren't worker-safe
    case preprocess_joins(worker_ops, workers, timeout, bcast_threshold) do
      {:ok, processed_ops, broadcast_tables} ->
        # All joins handled inline (broadcast or push-down)
        worker_pipeline = %{pipeline | ops: processed_ops}

        try do
          result =
            execute_fan_out(worker_pipeline, workers, strategy, timeout, streaming?, split)

          result = apply_avg_rewrites(result, rewrites)
          finalize(result, coord_ops)
        after
          cleanup_broadcast_tables(workers, broadcast_tables)
        end

      {:shuffle, ops_before, {right_computed, how, on_cols, suffix}, ops_after, broadcast_tables} ->
        # Pipeline needs a shuffle stage: execute pre-join → shuffle → post-join
        try do
          execute_with_shuffle(%{
            pipeline: pipeline,
            ops_before: ops_before,
            right: right_computed,
            how: how,
            on_cols: on_cols,
            suffix: suffix,
            ops_after: ops_after,
            coord_ops: coord_ops,
            rewrites: rewrites,
            workers: workers,
            strategy: strategy,
            timeout: timeout
          })
        after
          cleanup_broadcast_tables(workers, broadcast_tables)
        end
    end
  end

  @doc """
  Execute a pipeline across workers, returning raw Arrow IPC binaries.

  Useful when you want to handle merging yourself or stream results.
  """
  def fan_out_raw(%Dux{} = pipeline, opts \\ []) do
    workers = Keyword.get_lazy(opts, :workers, &Worker.list/0)
    timeout = Keyword.get(opts, :timeout, :infinity)

    assignments = Partitioner.assign(pipeline, workers)
    fan_out(assignments, timeout)
  end

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  # Standard distributed execution: partition → fan out → merge
  # When streaming_compatible? is true and a StreamingMerger can be created,
  # folds results incrementally. Otherwise falls back to batch merge.
  defp execute_fan_out(worker_pipeline, workers, strategy, timeout, streaming?, split) do
    assignments = Partitioner.assign(worker_pipeline, workers, strategy: strategy)
    n_workers = length(assignments)

    # Try streaming merge for lattice-compatible pipelines
    merger =
      if streaming? do
        StreamingMerger.new(split.worker_ops, n_workers)
      end

    if merger do
      execute_streaming(assignments, worker_pipeline, merger, timeout)
    else
      execute_batch(assignments, worker_pipeline, n_workers, timeout)
    end
  end

  defp execute_batch(assignments, worker_pipeline, n_workers, timeout) do
    results =
      :telemetry.span([:dux, :distributed, :fan_out], %{n_workers: n_workers}, fn ->
        r = fan_out(assignments, timeout)
        {r, %{n_workers: n_workers}}
      end)

    {successes, failures} = partition_results(results)

    if successes == [] do
      reasons = Enum.map(failures, fn {:error, reason} -> reason end)
      raise ArgumentError, "all workers failed: #{inspect(reasons)}"
    end

    :telemetry.span([:dux, :distributed, :merge], %{n_results: length(successes)}, fn ->
      merged = Merger.merge_to_dux(successes, worker_pipeline)
      {merged, %{n_results: length(successes)}}
    end)
  end

  defp execute_streaming(assignments, _worker_pipeline, merger, timeout) do
    n_workers = length(assignments)

    :telemetry.span(
      [:dux, :distributed, :fan_out],
      %{n_workers: n_workers, streaming: true},
      fn ->
        final_merger = streaming_fan_out(assignments, merger, n_workers, timeout)
        result = StreamingMerger.to_dux(final_merger)
        {result, %{n_workers: n_workers, streaming: true}}
      end
    )
  end

  defp streaming_fan_out(assignments, merger, n_workers, timeout) do
    assignments
    |> Enum.with_index()
    |> Task.async_stream(
      fn {{worker, partition_pipeline}, idx} ->
        execute_on_worker(worker, partition_pipeline, idx, n_workers, timeout)
      end,
      timeout: timeout,
      max_concurrency: n_workers,
      ordered: false
    )
    |> Enum.reduce(merger, &fold_streaming_result/2)
  end

  defp execute_on_worker(worker, pipeline, idx, n_workers, timeout) do
    start_time = System.monotonic_time()
    result = Worker.execute(worker, pipeline, timeout)

    case result do
      {:ok, ipc} ->
        :telemetry.execute(
          [:dux, :distributed, :worker, :stop],
          %{duration: System.monotonic_time() - start_time, ipc_bytes: byte_size(ipc)},
          %{worker: worker, worker_index: idx, n_workers: n_workers}
        )

      _ ->
        :ok
    end

    result
  end

  defp fold_streaming_result({:ok, {:ok, ipc}}, merger) do
    merger = StreamingMerger.fold(merger, ipc)

    :telemetry.execute(
      [:dux, :distributed, :streaming_merge],
      %{workers_complete: merger.workers_complete, workers_total: merger.workers_total},
      %{progress: StreamingMerger.progress(merger)}
    )

    merger
  end

  defp fold_streaming_result({:ok, {:error, _reason}}, merger) do
    StreamingMerger.record_failure(merger)
  end

  defp fold_streaming_result({:exit, _reason}, merger) do
    StreamingMerger.record_failure(merger)
  end

  # Multi-stage execution for shuffle joins:
  # 1. Execute ops before the join as a distributed query
  # 2. Shuffle join the result with the right side
  # 3. Apply remaining ops + coordinator ops
  defp execute_with_shuffle(%{
         pipeline: pipeline,
         ops_before: ops_before,
         right: right_computed,
         how: how,
         on_cols: on_cols,
         ops_after: ops_after,
         coord_ops: coord_ops,
         rewrites: rewrites,
         workers: workers,
         strategy: strategy,
         timeout: timeout
       }) do
    # Stage 1: execute pre-join ops distributed
    left_result =
      if ops_before == [] do
        Dux.compute(%{pipeline | ops: []})
      else
        execute_fan_out(%{pipeline | ops: ops_before}, workers, strategy, timeout, false, %{})
      end

    # Stage 2: shuffle join
    shuffle_on =
      case on_cols do
        [{col, col}] -> String.to_atom(col)
        pairs -> Enum.map(pairs, fn {l, r} -> {String.to_atom(l), String.to_atom(r)} end)
      end

    shuffle_result =
      Shuffle.execute(left_result, right_computed,
        on: shuffle_on,
        how: how,
        workers: workers,
        timeout: timeout
      )

    # Stage 3: apply remaining ops + rewrites + coordinator ops
    result =
      if ops_after == [] do
        shuffle_result
      else
        Dux.compute(%{shuffle_result | ops: ops_after})
      end

    result = apply_avg_rewrites(result, rewrites)
    finalize(result, coord_ops)
  end

  defp fan_out(assignments, timeout) do
    n_workers = length(assignments)

    assignments
    |> Enum.with_index()
    |> Task.async_stream(
      fn {{worker, partition_pipeline}, idx} ->
        start_time = System.monotonic_time()
        result = Worker.execute(worker, partition_pipeline, timeout)

        case result do
          {:ok, ipc} ->
            :telemetry.execute(
              [:dux, :distributed, :worker, :stop],
              %{
                duration: System.monotonic_time() - start_time,
                ipc_bytes: byte_size(ipc)
              },
              %{worker: worker, worker_index: idx, n_workers: n_workers}
            )

          _ ->
            :ok
        end

        result
      end,
      timeout: timeout,
      max_concurrency: n_workers,
      ordered: true
    )
    |> Enum.map(fn
      {:ok, {:ok, ipc}} -> {:ok, ipc}
      {:ok, {:error, reason}} -> {:error, reason}
      {:exit, reason} -> {:error, {:worker_crash, reason}}
    end)
  end

  defp partition_results(results) do
    Enum.split_with(results, fn
      {:ok, _} -> true
      _ -> false
    end)
    |> then(fn {ok, err} ->
      {Enum.map(ok, fn {:ok, ipc} -> ipc end), err}
    end)
  end

  # ---------------------------------------------------------------------------
  # Join preprocessing — broadcast/shuffle non-worker-safe right sides
  # ---------------------------------------------------------------------------

  # Scan worker ops for joins where the right side holds a source that
  # workers can't resolve (e.g. a local {:table, ref}).  For each such join:
  #
  # - Small right side → broadcast to workers, replace join with broadcast ref
  # - Large right side → signal a pipeline split for shuffle join
  #
  # Returns:
  #   {:ok, processed_ops, broadcast_names} — all joins handled inline
  #   {:shuffle, ops_before, join_info, ops_after, broadcast_names} — need a shuffle stage
  defp preprocess_joins(ops, workers, timeout, threshold) do
    do_preprocess(ops, [], [], workers, timeout, threshold)
  end

  # Base case: all ops processed, no shuffle needed
  defp do_preprocess([], processed, broadcast_names, _workers, _timeout, _threshold) do
    {:ok, Enum.reverse(processed), broadcast_names}
  end

  # Join op: check if right side is worker-safe
  defp do_preprocess(
         [{:join, %Dux{} = right, how, on_cols, suffix} = op | rest],
         processed,
         broadcast_names,
         workers,
         timeout,
         threshold
       ) do
    if worker_safe_source?(right.source) and Enum.all?(right.ops, &worker_safe_op?/1) do
      # Worker-safe — pass through
      do_preprocess(rest, [op | processed], broadcast_names, workers, timeout, threshold)
    else
      # Non-worker-safe — compute right side and decide broadcast vs shuffle
      conn = Dux.Connection.get_conn()
      right_computed = Dux.compute(right)
      {:table, right_ref} = right_computed.source
      right_n_rows = Dux.Backend.table_n_rows(conn, right_ref)

      preprocess_non_safe_join(%{
        conn: conn,
        right_ref: right_ref,
        right_computed: right_computed,
        right_n_rows: right_n_rows,
        how: how,
        on_cols: on_cols,
        suffix: suffix,
        rest: rest,
        processed: processed,
        broadcast_names: broadcast_names,
        workers: workers,
        timeout: timeout,
        threshold: threshold
      })
    end
  end

  # Non-join op: pass through
  defp do_preprocess([op | rest], processed, broadcast_names, workers, timeout, threshold) do
    do_preprocess(rest, [op | processed], broadcast_names, workers, timeout, threshold)
  end

  defp preprocess_non_safe_join(%{right_n_rows: 0} = ctx) do
    {names, types} = describe_for_empty(ctx.conn, ctx.right_ref)

    col_defs =
      Enum.zip(names, types)
      |> Enum.map_join(", ", fn {n, t} -> "NULL::#{t} AS #{qi(n)}" end)

    empty_right = Dux.from_query("SELECT #{col_defs} WHERE false")
    new_op = {:join, empty_right, ctx.how, ctx.on_cols, ctx.suffix}

    do_preprocess(
      ctx.rest,
      [new_op | ctx.processed],
      ctx.broadcast_names,
      ctx.workers,
      ctx.timeout,
      ctx.threshold
    )
  end

  defp preprocess_non_safe_join(ctx) do
    right_ipc = Dux.Backend.table_to_ipc(ctx.conn, ctx.right_ref)

    route_non_safe_join(%{
      right_ipc: right_ipc,
      right_ref: ctx.right_ref,
      right_computed: ctx.right_computed,
      conn: ctx.conn,
      how: ctx.how,
      on_cols: ctx.on_cols,
      suffix: ctx.suffix,
      rest: ctx.rest,
      processed: ctx.processed,
      broadcast_names: ctx.broadcast_names,
      workers: ctx.workers,
      timeout: ctx.timeout,
      threshold: ctx.threshold
    })
  end

  defp route_non_safe_join(%{right_ipc: right_ipc, threshold: threshold} = ctx) do
    if byte_size(right_ipc) <= threshold do
      broadcast_name = "__bcast_#{:erlang.unique_integer([:positive])}"
      do_broadcast(ctx.workers, broadcast_name, right_ipc, ctx.timeout)

      # Bloom filter pre-filtering: broadcast distinct join keys so workers
      # can pre-filter their left partition before the join.
      # Only for inner/left joins — anti/semi joins need the unfiltered left.
      {filter_op, extra_tables} =
        if ctx.how in [:inner, :left] do
          maybe_broadcast_keys(ctx, broadcast_name)
        else
          {nil, []}
        end

      broadcast_right = Dux.from_query("SELECT * FROM #{qi(broadcast_name)}")
      new_op = {:join, broadcast_right, ctx.how, ctx.on_cols, ctx.suffix}

      # Insert filter before join if applicable
      new_processed =
        case filter_op do
          nil -> [new_op | ctx.processed]
          op -> [new_op, op | ctx.processed]
        end

      do_preprocess(
        ctx.rest,
        new_processed,
        extra_tables ++ [broadcast_name | ctx.broadcast_names],
        ctx.workers,
        ctx.timeout,
        threshold
      )
    else
      {:shuffle, Enum.reverse(ctx.processed),
       {ctx.right_computed, ctx.how, ctx.on_cols, ctx.suffix}, ctx.rest, ctx.broadcast_names}
    end
  end

  # Broadcast distinct join keys from the right side so workers can pre-filter.
  # DuckDB optimizes `IN (SELECT ...)` as a semi-join internally.
  # Skip if the right side has too many distinct keys (>10K — filter wouldn't help).
  defp maybe_broadcast_keys(ctx, broadcast_name) do
    conn = ctx.conn

    # Extract distinct join keys from right side
    right_key_cols =
      Enum.map_join(ctx.on_cols, ", ", fn {_l, r} -> qi(r) end)

    n_keys =
      Dux.Backend.query(
        conn,
        "SELECT COUNT(*) AS n FROM (SELECT DISTINCT #{right_key_cols} FROM #{qi(ctx.right_ref.name)}) __dk"
      )
      |> then(&Dux.Backend.table_to_rows(conn, &1))
      |> hd()
      |> Map.get("n")

    if n_keys <= 10_000 do
      keys_name = "#{broadcast_name}_keys"

      keys_ref =
        Dux.Backend.query(
          conn,
          "SELECT DISTINCT #{right_key_cols} FROM #{qi(ctx.right_ref.name)}"
        )

      keys_ipc = Dux.Backend.table_to_ipc(conn, keys_ref)
      do_broadcast(ctx.workers, keys_name, keys_ipc, ctx.timeout)

      # Build a filter expression: left_key IN (SELECT right_key FROM keys_table)
      filter_conditions =
        Enum.map_join(ctx.on_cols, " AND ", fn {l, r} ->
          "#{qi(l)} IN (SELECT #{qi(r)} FROM #{qi(keys_name)})"
        end)

      filter_op = {:filter, filter_conditions}
      {filter_op, [keys_name]}
    else
      {nil, []}
    end
  end

  defp describe_for_empty(conn, %Dux.TableRef{name: name}) do
    result = Adbc.Connection.query!(conn, "DESCRIBE #{qi(name)}")
    map = Adbc.Result.to_map(result)
    {map["column_name"] || [], map["column_type"] || []}
  end

  defp do_broadcast(workers, name, ipc, timeout) do
    meta = %{table_name: name, n_workers: length(workers), ipc_bytes: byte_size(ipc)}

    :telemetry.span([:dux, :distributed, :broadcast], meta, fn ->
      broadcast_to_workers(workers, name, ipc, timeout)
      {:ok, meta}
    end)
  end

  defp broadcast_to_workers(workers, name, ipc_binary, _timeout) do
    tasks =
      Enum.map(workers, fn worker ->
        Task.async(fn -> Worker.register_table(worker, name, ipc_binary) end)
      end)

    Task.await_many(tasks, 30_000)
  end

  defp cleanup_broadcast_tables(_workers, []), do: :ok

  defp cleanup_broadcast_tables(workers, broadcast_names) do
    Enum.each(broadcast_names, fn name ->
      Enum.each(workers, fn worker ->
        try do
          Worker.drop_table(worker, name)
        catch
          _, _ -> :ok
        end
      end)
    end)
  end

  # ---------------------------------------------------------------------------
  # DuckLake source resolution
  # ---------------------------------------------------------------------------

  # Resolve a DuckLake attached source into a list of backing Parquet file paths.
  # The coordinator queries the DuckLake catalog's file manifest and replaces
  # the {:attached, ...} source with {:ducklake_files, paths} so workers can
  # read the underlying Parquet files directly — no DuckLake extension needed.
  defp resolve_ducklake_source(%Dux{source: {:attached, db_name, table_name}} = pipeline) do
    try_resolve_ducklake(pipeline, db_name, table_name)
  end

  defp resolve_ducklake_source(%Dux{source: {:attached, db_name, table_name, _opts}} = pipeline) do
    try_resolve_ducklake(pipeline, db_name, table_name)
  end

  defp resolve_ducklake_source(pipeline), do: pipeline

  defp try_resolve_ducklake(pipeline, db_name, table_name) do
    conn = Dux.Connection.get_conn()
    db_str = to_string(db_name)

    case attached_db_type(conn, db_str) do
      "ducklake" -> resolve_ducklake_files(pipeline, conn, db_str, table_name)
      _ -> pipeline
    end
  end

  defp attached_db_type(conn, db_name) do
    sql = "SELECT type FROM pragma_database_list() WHERE name = '#{db_name}'"

    case Adbc.Connection.query(conn, sql) do
      {:ok, result} -> extract_first_value(Adbc.Result.materialize(result))
      {:error, _} -> nil
    end
  end

  defp resolve_ducklake_files(pipeline, conn, catalog, table_name) do
    case list_ducklake_files(conn, catalog, table_name) do
      [] -> pipeline
      files -> %{pipeline | source: {:ducklake_files, files}}
    end
  end

  defp list_ducklake_files(conn, catalog, table_name) do
    sql = "SELECT data_file FROM ducklake_list_files('#{catalog}', '#{table_name}')"

    case Adbc.Connection.query(conn, sql) do
      {:ok, result} -> extract_column(Adbc.Result.materialize(result), "data_file")
      {:error, _} -> []
    end
  end

  defp extract_first_value(%{data: [col | _]}) do
    case Enum.to_list(col) do
      [val | _] -> val
      _ -> nil
    end
  end

  defp extract_first_value(_), do: nil

  defp extract_column(%{data: columns}, name) do
    Enum.find_value(columns, [], fn col ->
      if col.field.name == name, do: Enum.to_list(col)
    end)
  end

  # ---------------------------------------------------------------------------

  # A source is worker-safe if every worker can independently resolve it.
  # File paths, SQL queries, and inline lists are worker-safe.
  # Table refs ({:table, ResourceArc}) are node-local — only the coordinator has them.
  defp worker_safe_source?({:parquet, _}), do: true
  defp worker_safe_source?({:parquet, _, _}), do: true
  defp worker_safe_source?({:parquet_list, _, _}), do: true
  defp worker_safe_source?({:ducklake_files, _}), do: true
  defp worker_safe_source?({:csv, _, _}), do: true
  defp worker_safe_source?({:ndjson, _, _}), do: true
  defp worker_safe_source?({:sql, _}), do: true
  defp worker_safe_source?({:query, _}), do: true
  defp worker_safe_source?({:list, _}), do: true
  defp worker_safe_source?({:table, _}), do: false
  defp worker_safe_source?({:attached, _, _}), do: false
  defp worker_safe_source?({:attached, _, _, _}), do: false
  defp worker_safe_source?(_), do: false

  # Check if an op itself is worker-safe (no nested non-worker-safe sources).
  # Joins can nest other Dux structs in the right side.
  defp worker_safe_op?({:join, %Dux{} = right, _, _, _}) do
    worker_safe_source?(right.source) and Enum.all?(right.ops, &worker_safe_op?/1)
  end

  defp worker_safe_op?({:asof_join, %Dux{} = right, _, _, _, _}) do
    worker_safe_source?(right.source) and Enum.all?(right.ops, &worker_safe_op?/1)
  end

  defp worker_safe_op?({:concat_rows, others}) do
    Enum.all?(others, fn %Dux{} = other ->
      worker_safe_source?(other.source) and Enum.all?(other.ops, &worker_safe_op?/1)
    end)
  end

  defp worker_safe_op?(_), do: true

  # ---------------------------------------------------------------------------

  # Apply aggregate rewrites (AVG, STDDEV, VARIANCE, COUNT DISTINCT)
  defp apply_avg_rewrites(dux, rewrites) when map_size(rewrites) == 0, do: dux

  defp apply_avg_rewrites(dux, rewrites) do
    computed_exprs =
      Enum.map(rewrites, fn
        {name, {:avg, sum_col, count_col}} ->
          {name, "#{qi(sum_col)} / #{qi(count_col)}"}

        {name, {:stddev, :stddev_samp, n, sum_x, sum_x2}} ->
          {name, stddev_formula(n, sum_x, sum_x2, :samp, true)}

        {name, {:stddev, :stddev_pop, n, sum_x, sum_x2}} ->
          {name, stddev_formula(n, sum_x, sum_x2, :pop, true)}

        {name, {:stddev, :var_samp, n, sum_x, sum_x2}} ->
          {name, stddev_formula(n, sum_x, sum_x2, :samp, false)}

        {name, {:stddev, :var_pop, n, sum_x, sum_x2}} ->
          {name, stddev_formula(n, sum_x, sum_x2, :pop, false)}

        {name, {:count_distinct_hll, hll_col}} ->
          # HyperLogLog: workers computed approx_count_distinct, merger summed them.
          # The summed HLL is already in the column — just rename.
          {name, qi(hll_col)}
      end)

    intermediate_cols =
      Enum.flat_map(rewrites, fn
        {_, {:avg, sum_col, count_col}} ->
          [String.to_atom(sum_col), String.to_atom(count_col)]

        {_, {:stddev, _, n, sum_x, sum_x2}} ->
          [String.to_atom(n), String.to_atom(sum_x), String.to_atom(sum_x2)]

        {_, {:count_distinct_hll, hll_col}} ->
          [String.to_atom(hll_col)]
      end)

    dux
    |> Dux.mutate_with(computed_exprs)
    |> Dux.discard(intermediate_cols)
  end

  # Distributed STDDEV/VARIANCE formula.
  # Workers emit (COUNT, SUM, SUM_OF_SQUARES) — all additive.
  # Merger re-aggregates with SUM. Coordinator computes:
  #   variance = (sum_x2 - sum_x^2 / n) / divisor
  #
  # Uses DOUBLE precision throughout. This formula can suffer from catastrophic
  # cancellation when values are very large (>1e15) with tiny variance, but
  # is correct for all practical data. DuckDB uses Welford internally for
  # single-node STDDEV — the decomposition is only needed for cross-worker merge.
  defp stddev_formula(n_col, sum_col, sum2_col, pop_or_samp, sqrt?) do
    n = qi(n_col)
    sx = qi(sum_col)
    sx2 = qi(sum2_col)

    divisor = if pop_or_samp == :samp, do: "(#{n} - 1)", else: n

    variance = "GREATEST(0, (#{sx2} - #{sx} * #{sx} / #{n}::DOUBLE) / #{divisor}::DOUBLE)"

    if sqrt? do
      "CASE WHEN #{n} <= 1 AND '#{pop_or_samp}' = 'samp' THEN NULL ELSE SQRT(#{variance}) END"
    else
      "CASE WHEN #{n} <= 1 AND '#{pop_or_samp}' = 'samp' THEN NULL ELSE #{variance} END"
    end
  end

  # Finalize the Coordinator result: apply coordinator-only ops and compute
  # locally. The result must be fully materialized — if we return a %Dux{}
  # with pending ops and workers still set, compute() would re-distribute them.
  defp finalize(dux, coord_ops) do
    pipeline = Enum.reduce(coord_ops, dux, &apply_single_op(&2, &1))

    if pipeline.ops == [] do
      pipeline
    else
      local = %{pipeline | workers: nil}
      Dux.compute(local)
    end
  end

  defp apply_single_op(dux, {:slice, offset, length}), do: Dux.slice(dux, offset, length)
  defp apply_single_op(dux, {:head, n}), do: Dux.head(dux, n)
  defp apply_single_op(dux, {:sort_by, spec}), do: %{dux | ops: dux.ops ++ [{:sort_by, spec}]}
  defp apply_single_op(dux, {:distinct, cols}), do: %{dux | ops: dux.ops ++ [{:distinct, cols}]}

  defp apply_single_op(dux, {:pivot_wider, names_col, values_col, agg}) do
    Dux.pivot_wider(dux, names_col, values_col, agg: agg)
  end

  defp apply_single_op(dux, {:pivot_longer, cols, names_to, values_to}) do
    Dux.pivot_longer(dux, cols, names_to: names_to, values_to: values_to)
  end

  defp apply_single_op(dux, op) do
    # Unknown op — append directly
    %{dux | ops: dux.ops ++ [op]}
  end
end
