defmodule Mix.Tasks.Dux.CheckEngineTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Dux.CheckEngine

  setup do
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(shell) end)
    :ok
  end

  test "checks the actual default engine and semantic Arrow results" do
    assert :ok = CheckEngine.run([])
    assert_received {:mix_shell, :info, [version]}
    assert version =~ ~r/^DuckDB v\d+\.\d+\.\d+.* via ADBC /

    for name <- [
          "primitive Arrow values",
          "NULL row count",
          "nested NULL values",
          "lambda SQL",
          "Arrow IPC round trip"
        ] do
      message = "PASS #{name}"
      assert_received {:mix_shell, :info, [^message]}
    end

    refute_received {:mix_shell, :error, _}
  end

  test "fails when the actual engine differs from the requested version" do
    assert_raise Mix.Error, ~r/compatibility checks failed: version/, fn ->
      CheckEngine.run(["--expect-version", "v0.0.0"])
    end

    assert_received {:mix_shell, :error, [message]}
    assert message =~ "expected v0.0.0, got v"
  end

  test "rejects unknown, missing, empty and positional arguments" do
    for args <- [
          ["--unknown"],
          ["--driver"],
          ["--expect-version"],
          ["--driver", ""],
          ["--expect-version", ""],
          ["important.duckdb"]
        ] do
      assert_raise Mix.Error, ~r/Usage: mix dux.check_engine/, fn -> CheckEngine.run(args) end
    end
  end

  test "rejects nonexistent drivers and directories instead of falling back" do
    for path <- [
          Path.join(System.tmp_dir!(), "missing_#{System.unique_integer([:positive])}.so"),
          System.tmp_dir!()
        ] do
      assert_raise Mix.Error, ~r/Driver is not a regular file/, fn ->
        CheckEngine.run(["--driver", path])
      end
    end
  end

  test "reports failure for a file that is not a shared library" do
    path = Path.join(System.tmp_dir!(), "invalid_driver_#{System.unique_integer([:positive])}.so")
    File.write!(path, "not a shared library")
    on_exit(fn -> File.rm(path) end)

    assert_raise Mix.Error, ~r/Could not open DuckDB driver/, fn ->
      CheckEngine.run(["--driver", path])
    end
  end
end
