defmodule Dux.Remote do
  @moduledoc """
  Distributed query execution across BEAM nodes.

  Dux distributes work by partitioning data across workers, each running
  its own DuckDB instance. Workers execute pipeline fragments independently
  and return results as Arrow IPC binaries. The coordinator merges partial
  results on the calling node.

  ## Architecture

  ```mermaid
  graph LR
      P[Pipeline] -->|distribute| C[Coordinator]
      C -->|partition| W1[Worker 1<br/>DuckDB]
      C -->|partition| W2[Worker 2<br/>DuckDB]
      C -->|partition| W3[Worker 3<br/>DuckDB]
      W1 -->|IPC| M[Merger]
      W2 -->|IPC| M
      W3 -->|IPC| M
      M -->|result| R[%Dux{}]
  ```

  ## Components

  - `Dux.Remote.Worker` — a DuckDB instance on a BEAM node, discoverable via `:pg`
  - `Dux.Remote.Coordinator` — partitions work, fans out to workers, merges results
  - `Dux.Remote.Broadcast` — broadcasts small tables to all workers for star-schema joins

  ## Usage

  Users interact with the distributed system through `Dux.distribute/2`:

      workers = Dux.Remote.Worker.list()

      Dux.from_parquet("s3://data/**/*.parquet")
      |> Dux.distribute(workers)
      |> Dux.filter(amount > 100)
      |> Dux.group_by(:region)
      |> Dux.summarise(total: sum(amount))
      |> Dux.to_rows()

  The Coordinator handles everything automatically — partitioning parquet globs
  so each worker reads its own files directly from S3, re-aggregating partial
  results (SUM→SUM, MIN→MIN, AVG→SUM/COUNT), and routing joins through
  broadcast (small right side) or shuffle (large-large).

  ## Data flow

  For **parquet/CSV sources**, each worker reads its assigned partition directly.
  No data flows through the coordinator on the read path.

  For **computed table sources** (`%Dux{source: {:table, _}}`), the coordinator
  materializes the data and distributes it as rows to workers.

  All cross-node data transfer uses **Arrow IPC serialization** — compact binary
  format that preserves types and schema.

  ## FLAME integration

  See `Dux.Flame` for elastic compute — spin up ephemeral cloud machines with
  DuckDB on demand via FLAME.
  """
end
