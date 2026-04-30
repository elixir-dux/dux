# Start distribution for :peer-based distributed tests.
# :peer needs the parent node to be alive for dist-connected peers.
distributed_exclusion =
  if Node.alive?() do
    []
  else
    sname = :"dux_test_#{:erlang.unique_integer([:positive])}"

    case Node.start(sname, :shortnames) do
      {:ok, _} ->
        []

      {:error, reason} ->
        IO.puts(
          "⚠ Distribution not available (#{inspect(reason)}) — excluding :distributed tests"
        )

        :distributed
    end
  end

# Start testcontainers for integration tests (requires Docker)
old = Process.flag(:trap_exit, true)

container_exclusion =
  case Testcontainers.start_link() do
    {:ok, _} ->
      []

    {:error, reason} ->
      IO.puts("⚠ Test container not available (#{inspect(reason)}) — excluding :container tests")
      :container
  end

Process.flag(:trap_exit, old)


  case List.flatten([distributed_exclusion, container_exclusion]) do
    [] ->
      ExUnit.start()

    exclusions ->
      ExUnit.start(exclude: exclusions)
  end
