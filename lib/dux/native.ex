defmodule Dux.Native do
  @moduledoc false

  # DEPRECATED — being migrated to Dux.Backend (ADBC).
  # This stub exists only during the migration period.
  # All functions raise with a migration message.

  def db_open, do: raise("Dux.Native.db_open/0 removed — use Dux.Backend")
  def db_open_path(_), do: raise("Dux.Native.db_open_path/1 removed — use Dux.Backend")
  def db_execute(_, _), do: raise("Dux.Native.db_execute/2 removed — use Dux.Backend")
  def df_query(_, _), do: raise("Dux.Native.df_query/2 removed — use Dux.Backend")
  def table_names(_), do: raise("Dux.Native.table_names/1 removed — use Dux.Backend")
  def table_dtypes(_), do: raise("Dux.Native.table_dtypes/1 removed — use Dux.Backend")
  def table_n_rows(_), do: raise("Dux.Native.table_n_rows/1 removed — use Dux.Backend")
  def table_ensure(_, _), do: raise("Dux.Native.table_ensure/2 removed — use Dux.Backend")
  def table_to_columns(_), do: raise("Dux.Native.table_to_columns/1 removed — use Dux.Backend")
  def table_to_rows(_), do: raise("Dux.Native.table_to_rows/1 removed — use Dux.Backend")
  def table_to_ipc(_), do: raise("Dux.Native.table_to_ipc/1 removed — use Dux.Backend")
  def table_from_ipc(_), do: raise("Dux.Native.table_from_ipc/1 removed — use Dux.Backend")
  def gc_sentinel_new(_, _), do: raise("Dux.Native.gc_sentinel_new/2 removed — use Dux.Backend")
  def gc_sentinel_alive(_), do: raise("Dux.Native.gc_sentinel_alive/1 removed — use Dux.Backend")
end
