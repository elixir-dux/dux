defmodule Dux.Connection do
  @moduledoc false

  # Manages the ADBC DuckDB connection for this node.
  # ADBC's Connection is already a GenServer that serializes access,
  # so this module just holds the pid and provides a lookup API.

  use GenServer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  def get_conn(server \\ __MODULE__) do
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

  # --- Callbacks ---

  @impl true
  def init(opts) do
    # Ensure DuckDB driver is available
    Adbc.download_driver!(:duckdb)

    driver_opts =
      case Keyword.get(opts, :path) do
        nil -> []
        path -> [path: path]
      end

    {:ok, db} = Adbc.Database.start_link(driver: :duckdb, process_options: driver_opts)
    {:ok, conn} = Adbc.Connection.start_link(database: db)
    {:ok, %{db: db, conn: conn}}
  end

  @impl true
  def handle_call(:get_conn, _from, %{conn: conn} = state) do
    {:reply, conn, state}
  end

  # Legacy compatibility
  @impl true
  def handle_call(:get_db, _from, %{conn: conn} = state) do
    {:reply, conn, state}
  end
end
