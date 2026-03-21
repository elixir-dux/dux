defmodule Dux.DatasetE2ETest do
  use ExUnit.Case, async: true
  require Dux

  alias Dux.Test.Datasets

  # ---------------------------------------------------------------------------
  # Penguins — filter, group_by, summarise, nulls, pivot
  # ---------------------------------------------------------------------------

  describe "penguins" do
    test "load and inspect shape" do
      df = Datasets.penguins() |> Dux.compute()
      assert Dux.n_rows(df) == 344
      assert "species" in df.names
      assert "body_mass_g" in df.names
    end

    test "filter by species" do
      result =
        Datasets.penguins()
        |> Dux.filter_with("species = 'Adelie'")
        |> Dux.to_columns()

      assert length(result["species"]) == 152
      assert Enum.all?(result["species"], &(&1 == "Adelie"))
    end

    test "group by species, summarise mean body mass" do
      result =
        Datasets.penguins()
        |> Dux.drop_nil([:body_mass_g])
        |> Dux.group_by(:species)
        |> Dux.summarise_with(
          avg_mass: "AVG(body_mass_g)",
          count: "COUNT(*)"
        )
        |> Dux.sort_by(:species)
        |> Dux.to_rows()

      assert length(result) == 3

      adelie = Enum.find(result, &(&1["species"] == "Adelie"))
      gentoo = Enum.find(result, &(&1["species"] == "Gentoo"))

      # Gentoo penguins are heavier than Adelie
      assert gentoo["avg_mass"] > adelie["avg_mass"]
      # Known: Adelie ~3700g, Gentoo ~5076g
      assert_in_delta adelie["avg_mass"], 3700, 100
      assert_in_delta gentoo["avg_mass"], 5076, 100
    end

    test "group by species + sex with null handling" do
      result =
        Datasets.penguins()
        |> Dux.drop_nil([:sex])
        |> Dux.group_by([:species, :sex])
        |> Dux.summarise_with(n: "COUNT(*)", avg_bill: "AVG(bill_length_mm)")
        |> Dux.sort_by([:species, :sex])
        |> Dux.to_rows()

      # 3 species × 2 sexes = 6 groups
      assert length(result) == 6
      assert Enum.all?(result, &(&1["n"] > 0))
    end

    test "pivot: mean body mass by species × island" do
      result =
        Datasets.penguins()
        |> Dux.drop_nil([:body_mass_g])
        |> Dux.group_by([:species, :island])
        |> Dux.summarise_with(avg_mass: "ROUND(AVG(body_mass_g), 0)")
        |> Dux.pivot_wider(:island, :avg_mass)
        |> Dux.sort_by(:species)
        |> Dux.to_rows()

      # Gentoo only on Biscoe
      gentoo = Enum.find(result, &(&1["species"] == "Gentoo"))
      assert gentoo["Biscoe"] != nil
      assert gentoo["Dream"] == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Gapminder — window functions, time series, larger cardinality
  # ---------------------------------------------------------------------------

  describe "gapminder" do
    test "load and verify shape" do
      df = Datasets.gapminder() |> Dux.compute()
      assert Dux.n_rows(df) == 1704
      assert "country" in df.names
      assert "gdpPercap" in df.names
    end

    test "filter continent, group by year" do
      result =
        Datasets.gapminder()
        |> Dux.filter_with("continent = 'Europe'")
        |> Dux.group_by(:year)
        |> Dux.summarise_with(
          avg_life: "AVG(lifeExp)",
          total_pop: "SUM(pop)"
        )
        |> Dux.sort_by(:year)
        |> Dux.to_rows()

      # 12 time points (1952-2007, every 5 years)
      assert length(result) == 12

      # Life expectancy should increase over time
      first = hd(result)["avg_life"]
      last = List.last(result)["avg_life"]
      assert last > first
    end

    test "top 5 countries by GDP per capita in 2007" do
      result =
        Datasets.gapminder()
        |> Dux.filter_with("year = 2007")
        |> Dux.sort_by(desc: :gdpPercap)
        |> Dux.head(5)
        |> Dux.to_rows()

      assert length(result) == 5
      # Norway is typically #1 or #2 in 2007
      top_countries = Enum.map(result, & &1["country"])
      assert "Norway" in top_countries
    end

    test "pivot longer: continent population by year" do
      result =
        Datasets.gapminder()
        |> Dux.group_by([:continent, :year])
        |> Dux.summarise_with(total_pop: "SUM(pop)")
        |> Dux.pivot_wider(:continent, :total_pop)
        |> Dux.sort_by(:year)
        |> Dux.to_rows()

      assert length(result) == 12
      assert Map.has_key?(hd(result), "Asia")
      assert Map.has_key?(hd(result), "Europe")

      # Asia population should be largest
      assert hd(result)["Asia"] > hd(result)["Europe"]
    end
  end

  # ---------------------------------------------------------------------------
  # nycflights13 — star schema joins
  # ---------------------------------------------------------------------------

  describe "nycflights13 star schema" do
    test "flights shape" do
      df = Datasets.flights() |> Dux.compute()
      assert Dux.n_rows(df) == 6099
      assert "carrier" in df.names
      assert "origin" in df.names
      assert "dest" in df.names
      assert "tailnum" in df.names
    end

    test "join flights → airlines (carrier name lookup)" do
      result =
        Datasets.flights()
        |> Dux.join(Datasets.airlines(), on: :carrier)
        |> Dux.group_by(:name)
        |> Dux.summarise_with(n_flights: "COUNT(*)")
        |> Dux.sort_by(desc: :n_flights)
        |> Dux.head(5)
        |> Dux.to_rows()

      assert length(result) == 5
      # United and JetBlue are major carriers at NYC airports
      carrier_names = Enum.map(result, & &1["name"])
      assert Enum.any?(carrier_names, &String.contains?(&1, "United"))
    end

    test "join flights → airports (origin airport details)" do
      result =
        Datasets.flights()
        |> Dux.join(Datasets.airports(), on: [{:origin, :faa}])
        |> Dux.group_by(:name)
        |> Dux.summarise_with(n: "COUNT(*)")
        |> Dux.sort_by(desc: :n)
        |> Dux.to_rows()

      # 3 NYC airports: EWR, JFK, LGA
      assert length(result) == 3
      names = Enum.map(result, & &1["name"])
      assert Enum.any?(names, &String.contains?(&1, "John F Kennedy"))
      assert Enum.any?(names, &String.contains?(&1, "La Guardia"))
    end

    test "join flights → planes (aircraft info)" do
      result =
        Datasets.flights()
        |> Dux.drop_nil([:tailnum])
        |> Dux.join(Datasets.planes(), on: :tailnum)
        |> Dux.group_by(:manufacturer)
        |> Dux.summarise_with(n: "COUNT(*)", avg_seats: "AVG(seats)")
        |> Dux.sort_by(desc: :n)
        |> Dux.head(5)
        |> Dux.to_rows()

      assert length(result) == 5
      # Boeing and Airbus/Embraer should be top manufacturers
      manufacturers = Enum.map(result, & &1["manufacturer"])
      assert "BOEING" in manufacturers
    end

    test "full star schema: flights + airlines + airports" do
      result =
        Datasets.flights()
        |> Dux.join(Datasets.airlines(), on: :carrier)
        |> Dux.join(Datasets.airports(), on: [{:dest, :faa}])
        |> Dux.head(10)
        |> Dux.to_rows()

      assert length(result) == 10
      # Should have airline name and airport name columns
      row = hd(result)
      assert Map.has_key?(row, "carrier")
      assert Map.has_key?(row, "dest")
    end

    test "average delay by carrier" do
      result =
        Datasets.flights()
        |> Dux.drop_nil([:dep_delay])
        |> Dux.group_by(:carrier)
        |> Dux.summarise_with(
          avg_delay: "AVG(dep_delay)",
          n: "COUNT(*)"
        )
        |> Dux.sort_by(desc: :avg_delay)
        |> Dux.to_rows()

      assert length(result) > 5
      assert Enum.all?(result, &is_number(&1["avg_delay"]))
    end
  end

  # ---------------------------------------------------------------------------
  # Karate Club graph — PageRank, connected components, triangles
  # ---------------------------------------------------------------------------

  describe "karate club graph" do
    test "graph shape" do
      graph = Datasets.karate_club()
      assert Dux.Graph.vertex_count(graph) == 34
      # 78 undirected edges × 2 directions = 156 directed edges
      assert Dux.Graph.edge_count(graph) == 156
    end

    test "pagerank: node 34 is highest ranked" do
      result =
        Datasets.karate_club()
        |> Dux.Graph.pagerank(iterations: 20)
        |> Dux.sort_by(desc: :rank)
        |> Dux.head(5)
        |> Dux.to_rows()

      top_ids = Enum.map(result, & &1["id"])
      # Node 34 (Officer) has the most connections (degree 17)
      assert hd(top_ids) == 34
      # All should have positive rank
      assert Enum.all?(result, &(&1["rank"] > 0))
    end

    test "connected components: single component" do
      result =
        Datasets.karate_club()
        |> Dux.Graph.connected_components()
        |> Dux.to_columns()

      # All 34 nodes should be in one component
      components = result["component"] |> Enum.uniq()
      assert length(components) == 1
    end

    test "triangle count: 45 triangles" do
      graph = Datasets.karate_club()
      count = Dux.Graph.triangle_count(graph)
      # Known: 45 triangles in Zachary's Karate Club
      assert count == 45
    end

    test "degree distribution: nodes 34 and 1 have highest degree" do
      result =
        Datasets.karate_club()
        |> Dux.Graph.out_degree()
        |> Dux.sort_by(desc: :out_degree)
        |> Dux.head(5)
        |> Dux.to_rows()

      top_ids = Enum.map(result, & &1["id"])
      # Node 34 (degree 17) and node 1 (degree 16) are the two leaders
      assert 34 in top_ids
      assert 1 in top_ids
    end
  end
end
