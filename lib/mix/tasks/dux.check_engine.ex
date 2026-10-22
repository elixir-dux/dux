defmodule Mix.Tasks.Dux.CheckEngine do
  use Mix.Task

  @shortdoc "Checks a DuckDB driver's version and Arrow compatibility"
  @moduledoc """
  Runs semantic smoke checks against a fresh in-memory DuckDB database.

      mix dux.check_engine
      mix dux.check_engine --driver /path/to/libduckdb.so --expect-version v2.0.0

  Without `--driver`, downloads and checks Dux's pinned DuckDB driver.
  `--driver` loads exactly the supplied local shared library, using DuckDB's
  ADBC entry point. It never falls back to the default driver.

  Reports the actual engine version and checks primitive values, NULL row
  counts, nested NULLs, lambda SQL, and Arrow IPC round trips. Any mismatch or
  query error makes the task fail. These checks supplement the test suite;
  passing them does not establish compatibility for every Dux operation.

  The task starts only ADBC, creates no file-backed databases, and does not
  change the driver used by Dux connections or remote workers.
  """

  @impl true
  def run(args) do
    {opts, positional, invalid} =
      OptionParser.parse(args, strict: [driver: :string, expect_version: :string])

    if positional != [] or invalid != [] or Enum.any?(opts, fn {_, value} -> value == "" end) do
      Mix.raise("Usage: mix dux.check_engine [--driver PATH] [--expect-version VERSION]")
    end

    Mix.Task.run("compile")
    {:ok, _} = Application.ensure_all_started(:adbc)
    driver_opts = driver_options(opts[:driver])

    case Adbc.Database.start_link(driver_opts) do
      {:ok, db} ->
        try do
          check_database(db, opts[:expect_version])
        after
          GenServer.stop(db)
        end

      {:error, reason} ->
        Mix.raise("Could not open DuckDB driver: #{inspect(reason)}")
    end
  end

  defp driver_options(nil), do: Dux.DuckDB.database_options()

  defp driver_options(path) do
    path = Path.expand(path)
    unless File.regular?(path), do: Mix.raise("Driver is not a regular file: #{path}")
    [driver: path, entrypoint: "duckdb_adbc_init"]
  end

  defp check_database(db, expected_version) do
    {:ok, conn} = Adbc.Connection.start_link(database: db)

    try do
      %{"version" => [version]} = query(conn, "SELECT version() AS version")
      Mix.shell().info("DuckDB #{version} via ADBC #{Application.spec(:adbc, :vsn)}")

      failures =
        if expected_version && expected_version != version do
          Mix.shell().error("FAIL version: expected #{expected_version}, got #{version}")
          ["version"]
        else
          []
        end

      failures = failures ++ check_values(conn)

      if failures != [] do
        Mix.raise("DuckDB #{version} compatibility checks failed: #{Enum.join(failures, ", ")}")
      end

      Mix.shell().info("DuckDB compatibility checks passed.")
      :ok
    after
      GenServer.stop(conn)
    end
  end

  defp check_values(conn) do
    checks = [
      {"primitive Arrow values",
       fn ->
         query(conn, """
         SELECT 42::BIGINT AS integer, 1.5::DOUBLE AS real, true AS flag,
                'quotes '' and 雪' AS label, DATE '2026-10-21' AS day
         """)
       end,
       %{
         "integer" => [42],
         "real" => [1.5],
         "flag" => [true],
         "label" => ["quotes ' and 雪"],
         "day" => [~D[2026-10-21]]
       }},
      {"NULL row count", fn -> query(conn, "SELECT NULL AS value FROM range(3)") end,
       %{"value" => [nil, nil, nil]}},
      {"nested NULL values",
       fn -> query(conn, "SELECT {'value': NULL} AS item FROM range(3)") end,
       %{"item" => List.duplicate(%{"value" => nil}, 3)}},
      {"lambda SQL",
       fn -> query(conn, "SELECT list_transform([1, NULL, 3], lambda x: x * 2) AS values") end,
       %{"values" => [[2, nil, 6]]}},
      {"Arrow IPC round trip", fn -> ipc_round_trip(conn) end,
       %{"value" => Enum.to_list(0..1000)}}
    ]

    Enum.flat_map(checks, fn {name, fun, expected} ->
      case check(fun, expected) do
        :ok ->
          Mix.shell().info("PASS #{name}")
          []

        {:error, message} ->
          Mix.shell().error("FAIL #{name}: #{message}")
          [name]
      end
    end)
  end

  defp check(fun, expected) do
    case fun.() do
      ^expected ->
        :ok

      actual ->
        {:error, "expected #{inspect(expected, limit: 5)}, got #{inspect(actual, limit: 5)}"}
    end
  rescue
    error -> {:error, Exception.message(error)}
  end

  defp query(conn, sql), do: Adbc.Connection.query!(conn, sql) |> Adbc.Result.to_map()

  defp ipc_round_trip(conn) do
    {:ok, ipc} =
      Adbc.Connection.query_pointer(
        conn,
        "SELECT range AS value FROM range(1001) ORDER BY range",
        fn stream ->
          Adbc.StreamResult.to_ipc_stream(stream)
        end
      )

    ipc |> Adbc.Result.from_ipc_stream!() |> Adbc.Result.to_map()
  end
end
