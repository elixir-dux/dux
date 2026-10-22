defmodule Dux.DuckDB do
  @moduledoc false

  # ADBC's bundled driver version can lag DuckDB releases. Keep download and
  # database selection together so every local connection and worker uses it.
  @version "2.0.0"
  @release_url "https://github.com/duckdb/duckdb/releases/download/v#{@version}"

  def database_options do
    Adbc.download_driver!(:duckdb, download_options())
    [driver: :duckdb, version: @version]
  end

  def download_options do
    architecture =
      case :os.type() do
        {:win32, _} -> System.get_env("PROCESSOR_ARCHITECTURE", "")
        _ -> to_string(:erlang.system_info(:system_architecture))
      end

    download_options(:os.type(), architecture)
  end

  def download_options(os, architecture) do
    arch = architecture |> String.downcase() |> String.split("-") |> hd()
    platform = platform(os, arch, String.contains?(architecture, "musl"))

    [version: @version, url: "#{@release_url}/duckdb-shared-libs-#{platform}.tar.gz"]
  end

  defp platform({:unix, :darwin}, arch, _) when arch in ["aarch64", "x86_64"],
    do: "osx-universal"

  defp platform({:unix, :linux}, "x86_64", true), do: "linux-amd64-musl"
  defp platform({:unix, :linux}, "aarch64", true), do: "linux-arm64-musl"
  defp platform({:unix, :linux}, "x86_64", false), do: "linux-amd64"
  defp platform({:unix, :linux}, "aarch64", false), do: "linux-arm64"
  defp platform({:win32, _}, "amd64", _), do: "windows-amd64"
  defp platform({:win32, _}, "arm64", _), do: "windows-arm64"

  defp platform(os, arch, _) do
    raise ArgumentError, "DuckDB #{@version} driver is not supported on #{inspect(os)} / #{arch}"
  end
end
