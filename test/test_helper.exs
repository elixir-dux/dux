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

# Start testcontainers for integration tests (requires Docker)
case Testcontainers.start_link() do
  {:ok, _} -> :ok
  {:error, _} -> IO.puts("⚠ Docker not available — excluding :container tests")
end

if Node.alive?() do
  ExUnit.start()
else
  ExUnit.start(exclude: [:distributed])
end
