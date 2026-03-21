defmodule Dux.Connection do
  @moduledoc false

  # GenServer managing a single DuckDB connection per node.
  # Serializes all DuckDB access (mutex).
  # DuckDB's morsel-driven parallelism saturates all cores internally.

  use GenServer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  def get_db(server \\ __MODULE__) do
    GenServer.call(server, :get_db)
  end

  @doc false
  def execute(sql, server \\ __MODULE__) do
    GenServer.call(server, {:execute, sql})
  end

  @doc false
  def query(sql, server \\ __MODULE__) do
    GenServer.call(server, {:query, sql}, :infinity)
  end

  @doc false
  def load_extension(extension, server \\ __MODULE__) when is_atom(extension) do
    ext = Atom.to_string(extension)
    GenServer.call(server, {:execute, "INSTALL #{ext}; LOAD #{ext};"})
  end

  # --- Callbacks ---

  @impl true
  def init(opts) do
    db =
      case Keyword.get(opts, :path) do
        nil -> Dux.Native.db_open()
        path -> Dux.Native.db_open_path(path)
      end

    case db do
      {:ok, ref} -> {:ok, %{db: ref}}
      {:error, reason} -> {:stop, reason}
      ref -> {:ok, %{db: ref}}
    end
  end

  @impl true
  def handle_call(:get_db, _from, %{db: db} = state) do
    {:reply, db, state}
  end

  @impl true
  def handle_call({:execute, sql}, _from, %{db: db} = state) do
    result = Dux.Native.db_execute(db, sql)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:query, sql}, _from, %{db: db} = state) do
    result = Dux.Native.df_query(db, sql)
    {:reply, result, state}
  end
end
