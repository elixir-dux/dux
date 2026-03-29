defmodule Dux.TableRef do
  @moduledoc false

  # Represents a computed result stored as a DuckDB temporary table.
  #
  # Fields:
  #   - name: the temp table name in DuckDB (e.g. "adbc_ingest_7")
  #   - gc_ref: %Adbc.IngestResult{} — prevents GC of the temp table
  #   - node: origin node — for remote detection (replaces node(nif_ref))

  defstruct [:name, :gc_ref, :node, deps: []]

  @type t :: %__MODULE__{
          name: String.t(),
          gc_ref: term(),
          node: node(),
          deps: [term()]
        }
end
