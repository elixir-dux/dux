# Start distribution for :peer-based distributed tests.
# :peer needs the parent node to be alive for dist-connected peers.
unless Node.alive?() do
  sname = :"dux_test_#{:erlang.unique_integer([:positive])}"

  case Node.start(sname, :shortnames) do
    {:ok, _} ->
      :ok

    {:error, reason} ->
      IO.puts("⚠ Distribution not available (#{inspect(reason)}) — excluding :distributed tests")
  end
end

open_file_limit =
  case System.cmd("sh", ["-lc", "ulimit -n"], stderr_to_stdout: true) do
    {limit, 0} ->
      case Integer.parse(String.trim(limit)) do
        {value, _} -> value
        :error -> nil
      end

    _ ->
      nil
  end

distributed_available? = Node.alive?() and (is_nil(open_file_limit) or open_file_limit >= 1024)

if Node.alive?() and not distributed_available? do
  IO.puts(
    "⚠ Open file limit too low for peer tests (#{inspect(open_file_limit)}) — excluding :distributed tests"
  )
end

# Start testcontainers for integration tests (requires Docker)
docker_available? =
  System.get_env("DOCKER_HOST") ||
    Enum.any?(
      [
        "/var/run/docker.sock",
        Path.expand("~/.docker/run/docker.sock"),
        Path.expand("~/.docker/desktop/docker.sock")
      ],
      &File.exists?/1
    )

docker_available? =
  if docker_available? do
    case Testcontainers.start_link() do
      {:ok, _} ->
        true

      {:error, _} ->
        IO.puts("⚠ Docker not available — excluding :container tests")
        false
    end
  else
    IO.puts("⚠ Docker not available — excluding :container tests")
    false
  end

fts_available? =
  case System.cmd("sh", [
         "-lc",
         "command -v curl >/dev/null && curl -IfsSL --max-time 5 https://extensions.duckdb.org >/dev/null"
       ]) do
    {_, 0} ->
      true

    _ ->
      IO.puts("⚠ DuckDB extension repository not reachable — excluding :fts tests")
      false
  end

excludes =
  [:distributed, :container, :fts]
  |> Enum.reject(fn
    :distributed -> distributed_available?
    :container -> docker_available?
    :fts -> fts_available?
  end)

max_cases =
  if is_integer(open_file_limit) and open_file_limit < 1024 do
    1
  else
    ExUnit.configuration()[:max_cases]
  end

ExUnit.start(exclude: excludes, max_cases: max_cases)
