defmodule Dux.Remote.Worker do
  @moduledoc """
  A DuckDB worker node in a distributed Dux cluster.

  Each worker owns a DuckDB connection, registers in the `:dux_workers`
  process group via `:pg`, and executes pipelines on behalf of the
  coordinator.

  ## Starting workers

  Workers are started automatically by the application supervisor if distributed
  mode is enabled, or manually:

      {:ok, pid} = Dux.Remote.Worker.start_link([])

  ## Discovery

  The coordinator discovers workers via `:pg`:

      workers = Dux.Remote.Worker.list()

  ## Execution

  Workers receive `%Dux{}` pipelines (plain data), compile to SQL locally,
  execute against their local DuckDB, and return results as Arrow IPC.

      {:ok, ipc_binary} = Dux.Remote.Worker.execute(worker_pid, pipeline)
  """

  use GenServer

  @pg_group :dux_workers

  # ---------------------------------------------------------------------------
  # Client API
  # ---------------------------------------------------------------------------

  @doc """
  Start a worker and register it in the `:dux_workers` process group.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  List all registered workers across the cluster.
  """
  def list do
    :pg.get_members(@pg_group)
  end

  @doc """
  List workers on a specific node.
  """
  def list(node) do
    :pg.get_members(@pg_group)
    |> Enum.filter(&(node(&1) == node))
  end

  @doc """
  Execute a `%Dux{}` pipeline on a worker. Returns `{:ok, ipc_binary}` or `{:error, reason}`.

  The pipeline is compiled to SQL on the worker node and executed against
  the worker's local DuckDB. The result is serialized as Arrow IPC.
  """
  def execute(worker, %Dux{} = pipeline, timeout \\ :infinity) do
    GenServer.call(worker, {:execute, pipeline}, timeout)
  end

  @doc """
  Register an Arrow IPC binary as a named temporary table on the worker.
  Used for broadcast joins — the coordinator sends a small table to all workers.
  """
  def register_table(worker, name, ipc_binary) when is_binary(name) and is_binary(ipc_binary) do
    GenServer.call(worker, {:register_table, name, ipc_binary})
  end

  @doc """
  Drop a named temporary table on the worker.
  """
  def drop_table(worker, name) when is_binary(name) do
    GenServer.call(worker, {:drop_table, name})
  end

  @doc """
  Hash-partition a pipeline's results into `n_buckets` buckets by join key.

  Returns `%{bucket_id => ipc_binary}` — each bucket contains the rows
  whose `hash(join_key) % n_buckets == bucket_id`.
  """
  def hash_partition(worker, %Dux{} = pipeline, on, n_buckets, timeout \\ :infinity) do
    GenServer.call(worker, {:hash_partition, pipeline, on, n_buckets}, timeout)
  end

  @doc """
  Append an Arrow IPC chunk to a named temp table. Creates the table if
  it doesn't exist. Used during shuffle exchange.
  """
  def append_chunk(worker, table_name, ipc_binary) do
    GenServer.call(worker, {:append_chunk, table_name, ipc_binary})
  end

  @doc """
  Get worker info (node, connection status).
  """
  def info(worker) do
    GenServer.call(worker, :info)
  end

  # ---------------------------------------------------------------------------
  # Server callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    db =
      case Keyword.get(opts, :path) do
        nil -> Dux.Native.db_open()
        path -> Dux.Native.db_open_path(path)
      end

    :pg.join(@pg_group, self())

    {:ok, %{db: db, tables: %{}}}
  end

  @impl true
  def handle_call({:execute, %Dux{} = pipeline}, _from, %{db: db} = state) do
    result =
      try do
        # Keep source refs alive to prevent GC of temp tables during query
        source_ref = extract_source_ref(pipeline)
        {sql, source_setup} = Dux.QueryBuilder.build(pipeline, db)

        Enum.each(source_setup, fn setup_sql ->
          Dux.Native.db_execute(db, setup_sql)
        end)

        query_result =
          case Dux.Native.df_query(db, sql) do
            {:error, reason} ->
              {:error, reason}

            table_ref ->
              ipc = Dux.Native.table_to_ipc(table_ref)
              {:ok, ipc}
          end

        :erlang.phash2(source_ref, 1)
        query_result
      rescue
        e -> {:error, Exception.message(e)}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:register_table, name, ipc_binary}, _from, %{db: db} = state) do
    result =
      try do
        table_ref = Dux.Native.table_from_ipc(ipc_binary)
        Process.put(:dux_register_ref, table_ref)
        temp_name = Dux.Native.table_ensure(db, table_ref)
        escaped = escape_ident(name)

        Dux.Native.db_execute(db, "DROP TABLE IF EXISTS \"#{escaped}\"")

        Dux.Native.db_execute(
          db,
          "CREATE TEMPORARY TABLE \"#{escaped}\" AS SELECT * FROM \"#{temp_name}\""
        )

        Process.delete(:dux_register_ref)
        {:ok, name}
      rescue
        e -> {:error, Exception.message(e)}
      end

    tables =
      case result do
        {:ok, _} -> Map.put(state.tables, name, true)
        _ -> state.tables
      end

    {:reply, result, %{state | tables: tables}}
  end

  @impl true
  def handle_call({:drop_table, name}, _from, %{db: db} = state) do
    Dux.Native.db_execute(db, "DROP TABLE IF EXISTS \"#{escape_ident(name)}\"")
    {:reply, :ok, %{state | tables: Map.delete(state.tables, name)}}
  end

  @impl true
  def handle_call({:hash_partition, pipeline, on_col, n_buckets}, _from, %{db: db} = state) do
    result =
      try do
        # Build and execute the pipeline to get data
        source_ref = extract_source_ref(pipeline)
        {sql, source_setup} = Dux.QueryBuilder.build(pipeline, db)
        Enum.each(source_setup, fn s -> Dux.Native.db_execute(db, s) end)

        col = escape_ident(to_string(on_col))

        # For each bucket, extract rows where hash(key) % n == bucket_id
        buckets =
          for bucket_id <- 0..(n_buckets - 1), into: %{} do
            bucket_sql =
              "SELECT * EXCLUDE (__bucket) FROM (SELECT *, hash(\"#{col}\") % #{n_buckets} AS __bucket FROM (#{sql}) __src) WHERE __bucket = #{bucket_id}"

            case Dux.Native.df_query(db, bucket_sql) do
              {:error, _} ->
                {bucket_id, nil}

              table_ref ->
                n = Dux.Native.table_n_rows(table_ref)

                if n > 0 do
                  {bucket_id, Dux.Native.table_to_ipc(table_ref)}
                else
                  {bucket_id, nil}
                end
            end
          end

        :erlang.phash2(source_ref, 1)
        {:ok, buckets}
      rescue
        e -> {:error, Exception.message(e)}
      end

    {:reply, result, state}
  end

  @impl true
  def handle_call({:append_chunk, table_name, ipc_binary}, _from, %{db: db} = state) do
    result =
      try do
        table_ref = Dux.Native.table_from_ipc(ipc_binary)
        Process.put(:dux_append_ref, table_ref)
        temp = Dux.Native.table_ensure(db, table_ref)
        escaped = escape_ident(table_name)

        if Map.has_key?(state.tables, table_name) do
          # Append to existing table
          Dux.Native.db_execute(db, "INSERT INTO \"#{escaped}\" SELECT * FROM \"#{temp}\"")
        else
          # Create new table
          Dux.Native.db_execute(db, "DROP TABLE IF EXISTS \"#{escaped}\"")

          Dux.Native.db_execute(
            db,
            "CREATE TEMPORARY TABLE \"#{escaped}\" AS SELECT * FROM \"#{temp}\""
          )
        end

        Process.delete(:dux_append_ref)
        {:ok, table_name}
      rescue
        e -> {:error, Exception.message(e)}
      end

    tables =
      case result do
        {:ok, _} -> Map.put(state.tables, table_name, true)
        _ -> state.tables
      end

    {:reply, result, %{state | tables: tables}}
  end

  @impl true
  def handle_call(:info, _from, state) do
    info = %{
      node: node(),
      pid: self(),
      tables: Map.keys(state.tables)
    }

    {:reply, info, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :pg.leave(@pg_group, self())
    :ok
  end

  defp escape_ident(name), do: String.replace(name, ~s("), ~s(""))

  defp extract_source_ref(%Dux{source: {:table, ref}}), do: ref
  defp extract_source_ref(_), do: nil
end
