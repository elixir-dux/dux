defmodule Dux.MixProject do
  use Mix.Project

  @version "0.1.2-dev"
  @source_url "https://github.com/elixir-dux/dux"

  def project do
    [
      app: :dux,
      name: "Dux",
      description: "DuckDB-native dataframe library for Elixir",
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases(),
      package: package(),
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url
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
      {:adbc, github: "livebook-dev/adbc"},
      {:telemetry, "~> 1.0"},
      {:flame, "~> 0.5", optional: true},
      {:nx, "~> 0.9", optional: true},
      {:benchee, "~> 1.3", only: [:dev, :test]},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.36", only: :dev, runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end

  defp package do
    [
      files: [
        "lib",
        "mix.exs",
        "README.md",
        "CHANGELOG.md",
        "LICENSE-APACHE",
        "LICENSE-MIT"
      ],
      licenses: ["Apache-2.0", "MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/v#{@version}/CHANGELOG.md"
      },
      maintainers: ["Christopher Grainger"]
    ]
  end

  defp docs do
    [
      main: "Dux",
      source_ref: "v#{@version}",
      extras: [
        "guides/getting-started.livemd",
        "guides/distributed-queries.livemd",
        "guides/graph-analytics.livemd",
        "guides/cheatsheet.cheatmd",
        "CHANGELOG.md"
      ],
      groups_for_modules: [
        Core: [Dux],
        Query: [Dux.Query, Dux.Query.Compiler],
        Graph: [Dux.Graph],
        Distributed: [
          Dux.Remote,
          Dux.Remote.Coordinator,
          Dux.Remote.Worker,
          Dux.Remote.Broadcast
        ]
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/
      ]
    ]
  end

  defp aliases do
    []
  end
end
