defmodule Dux.Native do
  @moduledoc false

  use Rustler,
    otp_app: :dux,
    crate: "dux",
    mode: :release

  # Database lifecycle
  def db_open(), do: :erlang.nif_error(:nif_not_loaded)
  def db_open_path(_path), do: :erlang.nif_error(:nif_not_loaded)
  def db_execute(_db, _sql), do: :erlang.nif_error(:nif_not_loaded)

  # Query execution
  def df_query(_db, _sql), do: :erlang.nif_error(:nif_not_loaded)

  # Table metadata
  def table_names(_table), do: :erlang.nif_error(:nif_not_loaded)
  def table_dtypes(_table), do: :erlang.nif_error(:nif_not_loaded)
  def table_n_rows(_table), do: :erlang.nif_error(:nif_not_loaded)

  # Table management
  def table_ensure(_db, _table), do: :erlang.nif_error(:nif_not_loaded)

  # Data extraction
  def table_to_columns(_table), do: :erlang.nif_error(:nif_not_loaded)
  def table_to_rows(_table), do: :erlang.nif_error(:nif_not_loaded)

  # Arrow IPC serialization (for distribution)
  def table_to_ipc(_table), do: :erlang.nif_error(:nif_not_loaded)
  def table_from_ipc(_binary), do: :erlang.nif_error(:nif_not_loaded)
end
