defmodule Dux.SortingPeerTest do
  use ExUnit.Case, async: false

  alias Dux.Remote.Worker

  @moduletag :distributed
  @moduletag timeout: 120_000

  test "parallel workers preserve sorted rows through materialization and cross-node IPC" do
    n = 122_881

    pipeline =
      Dux.from_query("SELECT x AS a FROM range(#{n}) t(x)")
      |> Dux.head(n)
      |> Dux.sort_by(desc: :a)

    expected = %{"a" => Enum.to_list((n - 1)..0//-1)}
    assert Dux.to_columns(pipeline) == expected

    for name <- [:sorting_peer1, :sorting_peer2] do
      {peer, peer_node} = start_peer(name)

      try do
        {:ok, worker} =
          :erpc.call(peer_node, DynamicSupervisor, :start_child, [
            Dux.DynamicSupervisor,
            %{id: Worker, start: {Worker, :start_link, [[]]}, restart: :temporary}
          ])

        :ok = Worker.execute_sqls(worker, ["SET threads = 4"])
        {:ok, ipc} = Worker.execute(worker, pipeline)
        conn = Dux.Connection.get_conn()
        ref = Dux.Backend.table_from_ipc(conn, ipc)

        try do
          assert Dux.Backend.table_to_columns(conn, ref) == expected
        after
          Dux.Backend.execute(conn, ~s(DROP TABLE IF EXISTS "#{ref.name}"))
        end
      after
        :peer.stop(peer)
      end
    end
  end

  defp start_peer(name) do
    pa_args = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)
    {:ok, peer, peer_node} = :peer.start(%{name: name, args: pa_args})
    {:ok, _apps} = :erpc.call(peer_node, Application, :ensure_all_started, [:dux])
    {peer, peer_node}
  end
end
