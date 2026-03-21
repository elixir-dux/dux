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

if Node.alive?() do
  ExUnit.start()
else
  ExUnit.start(exclude: [:distributed])
end
