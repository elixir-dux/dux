defmodule Dux.RemoteTest do
  use ExUnit.Case, async: false

  alias Dux.Remote.{Holder, LocalGC}

  # ---------------------------------------------------------------------------
  # GC Sentinel unit tests (single-node)
  # Skipped — gc_sentinel_new is a Rust NIF from Phase 4, not yet ported to ADBC.
  # ---------------------------------------------------------------------------

  describe "GC sentinel" do
    @tag :skip
    test "sentinel is alive after creation" do
      sentinel = Dux.Native.gc_sentinel_new(self(), {:test, :msg})
      assert Dux.Native.gc_sentinel_alive(sentinel)
    end

    @tag :skip
    test "sentinel fires message when reference is collected" do
      test_pid = self()

      spawn(fn ->
        _sentinel = Dux.Native.gc_sentinel_new(test_pid, {:gc_test, :hello})
      end)

      assert_receive {:gc_test, :hello}, 5000
    end

    @tag :skip
    test "sentinel fires correct message content" do
      test_pid = self()
      ref = make_ref()

      spawn(fn ->
        _sentinel = Dux.Native.gc_sentinel_new(test_pid, {:gc, ref, 42})
      end)

      assert_receive {:gc, ^ref, 42}, 5000
    end

    @tag :skip
    test "multiple sentinels fire independently" do
      test_pid = self()

      for i <- 1..5 do
        spawn(fn ->
          _sentinel = Dux.Native.gc_sentinel_new(test_pid, {:gc_multi, i})
        end)
      end

      received =
        Enum.map(1..5, fn _ ->
          receive do
            {:gc_multi, i} -> i
          after
            5000 -> :timeout
          end
        end)
        |> Enum.sort()

      assert received == [1, 2, 3, 4, 5]
    end
  end

  # ---------------------------------------------------------------------------
  # LocalGC unit tests
  # ---------------------------------------------------------------------------

  describe "LocalGC" do
    test "is running" do
      assert is_pid(LocalGC.pid())
    end

    test "forwards gc messages" do
      test_pid = self()
      ref = make_ref()

      # Simulate what a GC sentinel would send
      send(LocalGC.pid(), {:gc, test_pid, ref})

      # LocalGC should forward {:gc, ref} to us
      assert_receive {:gc, ^ref}, 1000
    end
  end

  # ---------------------------------------------------------------------------
  # Holder unit tests
  # ---------------------------------------------------------------------------

  describe "Holder" do
    test "starts via DynamicSupervisor" do
      # Use a fake resource ref (a regular reference is fine for testing)
      fake_ref = make_ref()
      {:ok, holder_pid} = Holder.start_child(fake_ref, self())
      assert Process.alive?(holder_pid)
    end

    test "stops when receiving :gc message" do
      fake_ref = make_ref()
      {:ok, holder_pid} = Holder.start_child(fake_ref, self())
      monitor_ref = Process.monitor(holder_pid)

      send(holder_pid, {:gc, fake_ref})

      assert_receive {:DOWN, ^monitor_ref, :process, ^holder_pid, :normal}, 1000
    end

    test "stops when monitored process dies" do
      fake_ref = make_ref()

      # Start a process that the Holder will monitor
      {:ok, monitored_pid} = Agent.start(fn -> :ok end)
      {:ok, holder_pid} = Holder.start_child(fake_ref, monitored_pid)
      holder_monitor = Process.monitor(holder_pid)

      # Kill the monitored process
      Agent.stop(monitored_pid)

      assert_receive {:DOWN, ^holder_monitor, :process, ^holder_pid, :normal}, 1000
    end
  end

  # ---------------------------------------------------------------------------
  # Remote.place/1 unit tests (single-node — no remote refs to track)
  # ---------------------------------------------------------------------------

  describe "place/1 on local data" do
    test "local lazy Dux passes through unchanged" do
      dux = Dux.from_query("SELECT 1 AS x")
      assert Dux.Remote.place(dux) == dux
    end

    test "local computed Dux passes through (same node)" do
      dux = Dux.from_query("SELECT 1 AS x") |> Dux.compute()
      placed = Dux.Remote.place(dux)
      # Local ref — no remote tracking needed
      assert placed.remote == nil
    end

    test "place traverses lists" do
      duxes = [
        Dux.from_query("SELECT 1 AS x"),
        Dux.from_query("SELECT 2 AS x")
      ]

      placed = Dux.Remote.place(duxes)
      assert length(placed) == 2
    end

    test "place traverses tuples" do
      tuple = {Dux.from_query("SELECT 1"), :other}
      {placed_dux, :other} = Dux.Remote.place(tuple)
      assert %Dux{} = placed_dux
    end

    test "place traverses maps" do
      map = %{data: Dux.from_query("SELECT 1"), meta: "info"}
      placed = Dux.Remote.place(map)
      assert %Dux{} = placed.data
    end

    test "scalars pass through" do
      assert Dux.Remote.place(42) == 42
      assert Dux.Remote.place("hello") == "hello"
      assert Dux.Remote.place(nil) == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: GC sentinel → LocalGC → Holder lifecycle
  # ---------------------------------------------------------------------------

  describe "single-node GC lifecycle" do
    @tag :skip
    test "full cycle: sentinel → LocalGC → Holder cleanup" do
      # Create a fake resource ref and holder
      fake_ref = make_ref()
      {:ok, holder_pid} = Holder.start_child(fake_ref, LocalGC.pid())
      holder_monitor = Process.monitor(holder_pid)

      # Create a GC sentinel that will notify LocalGC when collected
      spawn(fn ->
        _sentinel =
          Dux.Native.gc_sentinel_new(
            LocalGC.pid(),
            {:gc, holder_pid, fake_ref}
          )

        # Process exits — sentinel fires
      end)

      # Holder should receive the forwarded :gc and stop
      assert_receive {:DOWN, ^holder_monitor, :process, ^holder_pid, :normal}, 5000
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    @tag :skip
    test "rapid sentinel creation and collection" do
      test_pid = self()

      for i <- 1..50 do
        spawn(fn ->
          _sentinel = Dux.Native.gc_sentinel_new(test_pid, {:rapid, i})
        end)
      end

      # Collect all messages
      received =
        Enum.reduce_while(1..50, [], fn _, acc ->
          receive do
            {:rapid, i} -> {:cont, [i | acc]}
          after
            5000 -> {:halt, acc}
          end
        end)
        |> Enum.sort()

      assert received == Enum.to_list(1..50)
    end

    test "holder survives unexpected messages" do
      fake_ref = make_ref()
      {:ok, holder_pid} = Holder.start_child(fake_ref, self())

      # Send random junk — should not crash
      send(holder_pid, :random)
      send(holder_pid, {:unexpected, 1, 2, 3})
      send(holder_pid, "string message")

      Process.sleep(50)
      assert Process.alive?(holder_pid)

      # Clean shutdown
      send(holder_pid, {:gc, fake_ref})
    end
  end
end
