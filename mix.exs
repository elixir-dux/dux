defmodule Dux.MixProject do
  use Mix.Project

  @version "0.1.0-dev"

  def project do
    [
      app: :dux,
      name: "Dux",
      description: "DuckDB-native dataframe library for Elixir",
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Dux.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ~w(lib test/support)
  defp elixirc_paths(_), do: ~w(lib)

  defp deps do
    [
      {:rustler, "~> 0.37.3", runtime: false},
      {:nx, "~> 0.9", optional: true},
      {:benchee, "~> 1.3", only: [:dev, :test]},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.36", only: :dev, runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end

  defp aliases do
    [
      "rust.lint": ["cmd cargo clippy --manifest-path=native/dux/Cargo.toml -- -Dwarnings"],
      "rust.fmt": ["cmd cargo fmt --manifest-path=native/dux/Cargo.toml --all"]
    ]
  end
end
