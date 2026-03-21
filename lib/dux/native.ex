defmodule Dux.Native do
  @moduledoc false

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :dux,
    crate: "dux",
    base_url: "https://github.com/elixir-dux/dux/releases/download/v#{version}",
    version: version,
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      x86_64-unknown-linux-gnu
      x86_64-unknown-linux-musl
      x86_64-pc-windows-msvc
    ),
    nif_versions: ["2.16"],
    force_build: System.get_env("DUX_BUILD") in ["1", "true"]

  # Database lifecycle
  def db_open, do: :erlang.nif_error(:nif_not_loaded)
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

  # GC sentinel (for distributed resource tracking)
  def gc_sentinel_new(_pid, _msg), do: :erlang.nif_error(:nif_not_loaded)
  def gc_sentinel_alive(_sentinel), do: :erlang.nif_error(:nif_not_loaded)
end
