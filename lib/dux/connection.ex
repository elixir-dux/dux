defmodule Dux.Connection do
  @moduledoc false

  # Manages the ADBC DuckDB connection(s) for this node.
  #
  # Connection PIDs are stored in :persistent_term for zero-cost access
  # on the query hot path. The GenServer manages lifecycle only (start,
  # stop, extension loading).
  #
  # With pool_size > 1, multiple Adbc.Connection processes share one
  # Adbc.Database. Queries are dispatched round-robin via :atomics.

  use GenServer

  @pt_key :dux_conn
  @pt_pool_key :dux_conn_pool
  @pt_counter_key :dux_conn_counter

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  def get_conn(server \\ __MODULE__)

  def get_conn(__MODULE__) do
    case :persistent_term.get(@pt_pool_key, nil) do
      nil ->
        # Single connection mode
        :persistent_term.get(@pt_key)

      pool ->
        # Pool mode — round-robin dispatch
        counter = :persistent_term.get(@pt_counter_key)
        idx = :atomics.add_get(counter, 1, 1)
        elem(pool, rem(idx, tuple_size(pool)))
    end
  end

  def get_conn(server) do
    # Named server — fall back to GenServer call
    GenServer.call(server, :get_conn)
  end

  # Legacy — some code still calls get_db. Alias to get_conn.
  @doc false
  def get_db(server \\ __MODULE__), do: get_conn(server)

  @doc false
  def load_extension(extension, server \\ __MODULE__) when is_atom(extension) do
    conn = get_conn(server)
    ext = Atom.to_string(extension)
    Adbc.Connection.query!(conn, "INSTALL #{ext}; LOAD #{ext};")
    :ok
  end

  @doc false
  def pool_size do
    case :persistent_term.get(@pt_pool_key, nil) do
      nil -> 1
      pool -> tuple_size(pool)
    end
  end

  # --- Callbacks ---

  @impl true
  def init(opts) do
    # Ensure DuckDB driver is available
    Adbc.download_driver!(:duckdb)

    db_opts =
      case Keyword.get(opts, :path) do
        nil -> []
        path -> [path: path]
      end

    pool_size = Keyword.get(opts, :pool_size, 1)

    {:ok, db} = Adbc.Database.start_link([driver: :duckdb] ++ db_opts)

    conns =
      for _ <- 1..pool_size do
        {:ok, conn} = Adbc.Connection.start_link(database: db)
        configure_duckdb(conn, opts)
        conn
      end

    name = Keyword.get(opts, :name, __MODULE__)

    # Only store in persistent_term for the default (unnamed) instance.
    # Named instances (tests, multiple databases) use GenServer.call.
    if name == __MODULE__ do
      if pool_size == 1 do
        :persistent_term.put(@pt_key, hd(conns))
      else
        :persistent_term.put(@pt_pool_key, List.to_tuple(conns))
        :persistent_term.put(@pt_counter_key, :atomics.new(1, []))
        # Also store first conn for single-conn API callers
        :persistent_term.put(@pt_key, hd(conns))
      end
    end

    {:ok, %{db: db, conns: conns, pool_size: pool_size, name: name}}
  end

  @impl true
  def terminate(_reason, %{name: name}) do
    if name == __MODULE__ do
      :persistent_term.erase(@pt_key)
      :persistent_term.erase(@pt_pool_key)
      :persistent_term.erase(@pt_counter_key)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  def terminate(_reason, _state), do: :ok

  # Legacy compatibility — keep GenServer call interface working
  # for any code that bypasses get_conn/0
  @impl true
  def handle_call(:get_conn, _from, %{conns: [conn | _]} = state) do
    {:reply, conn, state}
  end

  @impl true
  def handle_call(:get_db, _from, %{conns: [conn | _]} = state) do
    {:reply, conn, state}
  end

  # Apply DuckDB configuration settings to a connection.
  # Shared by Connection (local) and Worker (distributed).
  @memory_limit_pattern ~r/^\d+(\.\d+)?\s*(B|KB|KiB|MB|MiB|GB|GiB|TB|TiB)$/i

  @doc false
  def configure_duckdb(conn, opts \\ []) do
    # Disable insertion order preservation — allows DuckDB to parallelize
    # CREATE TABLE AS across threads. ~5x faster for large result sets.
    Adbc.Connection.query!(conn, "SET preserve_insertion_order = false")

    # Memory limit — caps DuckDB's memory usage. Important when multiple
    # workers share a machine. Default: DuckDB's 80%-of-RAM.
    if memory_limit = Keyword.get(opts, :memory_limit) do
      unless memory_limit =~ @memory_limit_pattern do
        raise ArgumentError,
              "invalid memory_limit: #{inspect(memory_limit)}, expected format like \"2GB\" or \"512MB\""
      end

      Adbc.Connection.query!(conn, "SET memory_limit = '#{memory_limit}'")
    end

    # Temp directory for spill-to-disk. DuckDB defaults to '.tmp' (CWD-relative),
    # which often doesn't exist. We default to System.tmp_dir!/0 for reliability.
    # On read-only filesystems, users should either configure a writable mount
    # or accept in-memory-only operation.
    temp_dir = Keyword.get(opts, :temp_directory, Path.join(System.tmp_dir!(), "dux_spill"))

    if String.contains?(temp_dir, ["'", ";", "\\"]) do
      raise ArgumentError,
            "temp_directory must not contain single quotes, semicolons, or backslashes: #{inspect(temp_dir)}"
    end

    File.mkdir_p(temp_dir)
    Adbc.Connection.query!(conn, "SET temp_directory = '#{temp_dir}'")

    :ok
  end
end
