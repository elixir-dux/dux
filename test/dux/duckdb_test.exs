defmodule Dux.DuckDBTest do
  use ExUnit.Case, async: true

  describe "released driver selection" do
    for {os, architecture, asset} <- [
          {{:unix, :linux}, "x86_64-pc-linux-gnu", "linux-amd64"},
          {{:unix, :linux}, "aarch64-unknown-linux-gnu", "linux-arm64"},
          {{:unix, :linux}, "x86_64-pc-linux-musl", "linux-amd64-musl"},
          {{:unix, :linux}, "aarch64-unknown-linux-musl", "linux-arm64-musl"},
          {{:unix, :darwin}, "aarch64-apple-darwin24.0", "osx-universal"},
          {{:unix, :darwin}, "x86_64-apple-darwin24.0", "osx-universal"},
          {{:win32, :nt}, "AMD64", "windows-amd64"},
          {{:win32, :nt}, "ARM64", "windows-arm64"}
        ] do
      test "selects #{asset} for #{architecture}" do
        assert Dux.DuckDB.download_options(unquote(Macro.escape(os)), unquote(architecture)) == [
                 version: "2.0.0",
                 url:
                   "https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-shared-libs-#{unquote(asset)}.tar.gz"
               ]
      end
    end

    test "rejects unsupported operating systems and architectures" do
      for {os, arch} <- [{{:unix, :freebsd}, "x86_64"}, {{:unix, :linux}, "riscv64"}] do
        assert_raise ArgumentError, ~r/DuckDB 2.0.0 driver is not supported/, fn ->
          Dux.DuckDB.download_options(os, arch)
        end
      end
    end
  end
end
