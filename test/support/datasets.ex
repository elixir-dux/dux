defmodule Dux.Test.Datasets do
  @moduledoc false

  # Provides access to embedded test datasets.
  # All datasets are CC0 / public domain except Karate Club (factual data, CC BY 4.0).

  @data_dir Path.expand("../data", __DIR__)

  def data_dir, do: @data_dir

  # -- Tabular --

  def penguins, do: Dux.from_csv(Path.join(@data_dir, "penguins.csv"), nullstr: "NA")
  def gapminder, do: Dux.from_csv(Path.join(@data_dir, "gapminder.csv"))

  # -- Star schema (nycflights13, Jan 1-7 2013) --

  def flights, do: Dux.from_csv(Path.join(@data_dir, "flights.csv"), nullstr: "NA")
  def airlines, do: Dux.from_csv(Path.join(@data_dir, "airlines.csv"))
  def airports, do: Dux.from_csv(Path.join(@data_dir, "airports.csv"))
  def planes, do: Dux.from_csv(Path.join(@data_dir, "planes.csv"))

  # -- Graph --

  def karate_club do
    # The CSV has undirected edges (src < dst). Graph algorithms need both directions.
    # Use raw SQL to produce both directions in one query, avoiding UNION ALL column order issues.
    path = Path.join(@data_dir, "karate_club.csv") |> String.replace("'", "''")

    edges =
      Dux.from_query("""
        SELECT src, dst FROM read_csv('#{path}')
        UNION ALL
        SELECT dst AS src, src AS dst FROM read_csv('#{path}')
      """)

    vertices = Dux.from_list(Enum.map(1..34, &%{id: &1}))
    Dux.Graph.new(vertices: vertices, edges: edges, edge_src: :src, edge_dst: :dst)
  end
end
