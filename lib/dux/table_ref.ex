defmodule Dux.TableRef do
  @moduledoc false

  # Represents a computed result stored as a DuckDB temporary table or view.
  #
  # Fields:
  #   - name: the temp table/view name in DuckDB (e.g. "adbc_ingest_7")
  #   - gc_ref: %Adbc.IngestResult{} — prevents GC of the temp table/view
  #   - node: origin node — for remote detection (replaces node(nif_ref))
  #   - deps: list of TableRef/gc_ref values that must stay alive while this
  #           ref is alive. Used by views to keep their source tables from
  #           being GC'd before the view.

  defstruct [:name, :gc_ref, :node, deps: []]

  @type t :: %__MODULE__{
          name: String.t(),
          gc_ref: term(),
          node: node(),
          deps: [term()]
        }
end
