defmodule Dux.Datasets do
  @moduledoc """
  Embedded datasets for learning and testing.

  All datasets are CC0 (public domain) unless noted.

  ## Available datasets

  | Dataset | Rows | Description |
  |---------|------|-------------|
  | `penguins/0` | 344 | Palmer penguins — species, measurements, island |
  | `gapminder/0` | 1,704 | Country-level life expectancy, population, GDP |
  | `flights/0` | 6,099 | NYC flights (Jan 1-7 2013) — the fact table |
  | `airlines/0` | 16 | Carrier code → name lookup |
  | `airports/0` | 1,458 | Airport code → name, lat/lon |
  | `planes/0` | 3,322 | Tail number → manufacturer, model, seats |

  ## Examples

      require Dux

      Dux.Datasets.penguins()
      |> Dux.filter(species == "Gentoo")
      |> Dux.group_by(:island)
      |> Dux.summarise(avg_mass: avg(body_mass_g))
      |> Dux.to_rows()

      # Star schema join
      Dux.Datasets.flights()
      |> Dux.join(Dux.Datasets.airlines(), on: :carrier)
      |> Dux.group_by(:name)
      |> Dux.summarise_with(n: "COUNT(*)")
      |> Dux.sort_by(desc: :n)
      |> Dux.to_rows()
  """

  @datasets_dir Application.app_dir(:dux, "priv/datasets")

  @doc "Palmer penguins dataset (344 rows). CC0."
  @doc group: :datasets
  def penguins, do: Dux.from_csv(Path.join(@datasets_dir, "penguins.csv"), nullstr: "NA")

  @doc "Gapminder excerpt — country, continent, year, lifeExp, pop, gdpPercap (1,704 rows). CC0."
  @doc group: :datasets
  def gapminder, do: Dux.from_csv(Path.join(@datasets_dir, "gapminder.csv"))

  @doc "NYC flights, Jan 1-7 2013 (6,099 rows). CC0."
  @doc group: :datasets
  def flights, do: Dux.from_csv(Path.join(@datasets_dir, "flights.csv"), nullstr: "NA")

  @doc "Airline carrier codes and names (16 rows). CC0."
  @doc group: :datasets
  def airlines, do: Dux.from_csv(Path.join(@datasets_dir, "airlines.csv"))

  @doc "US airport codes, names, and coordinates (1,458 rows). CC0."
  @doc group: :datasets
  def airports, do: Dux.from_csv(Path.join(@datasets_dir, "airports.csv"))

  @doc "Aircraft tail numbers, manufacturers, models (3,322 rows). CC0."
  @doc group: :datasets
  def planes, do: Dux.from_csv(Path.join(@datasets_dir, "planes.csv"), nullstr: "NA")

  @doc """
  Zachary's Karate Club graph (34 nodes, 78 undirected edges). CC BY 4.0.

  Returns a `Dux.Graph` with bidirectional edges (156 directed edges).
  The classic social network dataset from a 1977 study of friendships
  in a university karate club.
  """
  @doc group: :datasets
  def karate_club do
    path = Path.join(@datasets_dir, "karate_club.csv")

    edges =
      Dux.from_query("""
        SELECT src, dst FROM read_csv('#{String.replace(path, "'", "''")}')
        UNION ALL
        SELECT dst AS src, src AS dst FROM read_csv('#{String.replace(path, "'", "''")}')
      """)

    vertices = Dux.from_list(Enum.map(1..34, &%{id: &1}))
    Dux.Graph.new(vertices: vertices, edges: edges, edge_src: :src, edge_dst: :dst)
  end
end
