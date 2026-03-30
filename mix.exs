defmodule Dux.MixProject do
  use Mix.Project

  @version "0.2.1-dev"
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

  def cli do
    [preferred_envs: [check: :test]]
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
      {:adbc, "~> 0.11.0"},
      {:telemetry, "~> 1.0"},
      {:flame, "~> 0.5", optional: true},
      {:nx, "~> 0.9", optional: true},
      {:benchee, "~> 1.3", only: [:dev, :test]},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.36", only: :dev, runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:testcontainers, "~> 2.0", only: :test}
    ]
  end

  defp package do
    [
      files: [
        "lib",
        "priv/datasets",
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
      extra_section: "GUIDES",
      extras: [
        "guides/getting-started.livemd",
        "guides/data-io.livemd",
        "guides/transformations.livemd",
        "guides/joins-and-reshape.livemd",
        "guides/distributed.md",
        "guides/flame-clusters.livemd",
        "guides/graph-analytics.livemd",
        "guides/cheatsheet.cheatmd",
        "CHANGELOG.md"
      ],
      groups_for_modules: [
        "Core API": [Dux, Dux.Query, Dux.Datasets],
        Graph: [Dux.Graph],
        Distribution: [
          Dux.Remote,
          Dux.Remote.Coordinator,
          Dux.Remote.Worker,
          Dux.Remote.Broadcast,
          Dux.Remote.Shuffle
        ],
        Integrations: [Dux.Telemetry, Dux.Flame]
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*\.(livemd|md)$/,
        Cheatsheets: ~r/.*\.cheatmd/
      ],
      groups_for_docs: [
        Constructors: &(&1[:group] == :constructors),
        Transforms: &(&1[:group] == :transforms),
        Aggregation: &(&1[:group] == :aggregation),
        Joins: &(&1[:group] == :joins),
        Reshape: &(&1[:group] == :reshape),
        Sorting: &(&1[:group] == :sorting),
        IO: &(&1[:group] == :io),
        Materialization: &(&1[:group] == :materialization),
        Distribution: &(&1[:group] == :distribution)
      ],
      nest_modules_by_prefix: [Dux.Remote],
      before_closing_body_tag: &before_closing_body_tag/1
    ]
  end

  defp before_closing_body_tag(:html) do
    """
    <script defer src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
    <script>
      let mermaidInitialized = false;

      window.addEventListener("exdoc:loaded", function () {
        if (!mermaidInitialized) {
          mermaid.initialize({
            startOnLoad: false,
            theme: document.body.className.includes("dark") ? "dark" : "default"
          });
          mermaidInitialized = true;
        }

        let id = 0;
        for (const codeEl of document.querySelectorAll("pre code.mermaid")) {
          const preEl = codeEl.parentElement;
          const graphDefinition = codeEl.textContent;
          const graphEl = document.createElement("div");
          const graphId = "mermaid-graph-" + id++;
          mermaid.render(graphId, graphDefinition).then(function(result) {
            graphEl.innerHTML = result.svg;
            if (result.bindFunctions) result.bindFunctions(graphEl);
            preEl.insertAdjacentElement("afterend", graphEl);
            preEl.remove();
          });
        }
      });
    </script>
    """
  end

  defp before_closing_body_tag(_), do: ""

  defp aliases do
    [
      check: [
        "format --check-formatted",
        "deps.compile",
        "compile --warnings-as-errors",
        "test --exclude distributed",
        "credo --strict"
      ]
    ]
  end
end
