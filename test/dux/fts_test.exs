defmodule Dux.FTSTest do
  use ExUnit.Case, async: false
  @moduletag :fts
  @moduletag timeout: 120_000

  # DuckDB's FTS extension can crash when multiple FTS indexes exist on
  # different temp tables in the same connection. Each test creates and
  # drops its own index to avoid interference.

  defp create_and_search(rows, id_col, text_cols, query, opts \\ []) do
    index_opts = Keyword.get(opts, :index_opts, [])
    search_opts = Keyword.delete(opts, :index_opts)

    df = Dux.from_list(rows) |> Dux.compute()
    {:table, ref} = df.source
    conn = Dux.Connection.get_conn()

    indexed = Dux.FTS.create_index(df, id_col, text_cols, index_opts)
    result = Dux.FTS.search(indexed, query, search_opts) |> Dux.to_rows()

    try do
      Adbc.Connection.query!(conn, "PRAGMA drop_fts_index('#{ref.name}')")
    catch
      _, _ -> :ok
    end

    result
  end

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "create_index + search" do
    test "basic BM25 search returns matching rows" do
      results =
        create_and_search(
          [
            %{id: 1, title: "Machine learning basics", body: "An intro to ML"},
            %{id: 2, title: "Cooking recipes", body: "How to make pasta"},
            %{id: 3, title: "Deep learning models", body: "Transformers"}
          ],
          :id,
          [:title, :body],
          "machine learning"
        )

      assert results != []
      assert Enum.all?(results, &Map.has_key?(&1, "fts_score"))
      ids = Enum.map(results, & &1["id"])
      assert 1 in ids
    end

    test "search with limit" do
      results =
        create_and_search(
          [
            %{id: 1, title: "ML basics", body: "learning intro"},
            %{id: 2, title: "DL models", body: "deep learning"},
            %{id: 3, title: "Cooking", body: "no learning here"}
          ],
          :id,
          [:title, :body],
          "learning",
          limit: 1
        )

      assert length(results) == 1
    end

    test "search with field restriction" do
      results =
        create_and_search(
          [
            %{id: 1, title: "Cooking tips", body: "learning to cook"},
            %{id: 2, title: "ML learning", body: "something else"}
          ],
          :id,
          [:title, :body],
          "cooking",
          fields: [:title]
        )

      assert results != []
      assert hd(results)["id"] == 1
    end

    test "multi-column search" do
      results =
        create_and_search(
          [
            %{id: 1, title: "Alpha", body: "contains beta"},
            %{id: 2, title: "Beta", body: "contains alpha"}
          ],
          :id,
          [:title, :body],
          "alpha"
        )

      assert length(results) == 2
    end

    test "stemmer matches word variants" do
      results =
        create_and_search(
          [
            %{id: 1, text: "running in the park"},
            %{id: 2, text: "the runs were fast"},
            %{id: 3, text: "cooking dinner"}
          ],
          :id,
          [:text],
          "running",
          index_opts: [stemmer: "english"]
        )

      ids = Enum.map(results, & &1["id"]) |> MapSet.new()
      assert MapSet.member?(ids, 1)
      assert MapSet.member?(ids, 2)
      refute MapSet.member?(ids, 3)
    end
  end

  # ---------------------------------------------------------------------------
  # Sad path
  # ---------------------------------------------------------------------------

  describe "sad path" do
    test "create_index on non-computed dataframe raises" do
      df = Dux.from_list([%{id: 1, text: "hello"}])

      assert_raise ArgumentError, ~r/computed dataframe/, fn ->
        Dux.FTS.create_index(df, :id, [:text])
      end
    end

    test "search without index raises" do
      df = Dux.from_list([%{id: 1, text: "hello"}]) |> Dux.compute()

      assert_raise ArgumentError, ~r/no FTS index/, fn ->
        Dux.FTS.search(df, "hello") |> Dux.to_rows()
      end
    end

    test "search with no matches returns empty" do
      results =
        create_and_search(
          [%{id: 1, title: "hello world", body: "some text"}],
          :id,
          [:title, :body],
          "xyznonexistent"
        )

      assert results == []
    end
  end

  # ---------------------------------------------------------------------------
  # Adversarial
  # ---------------------------------------------------------------------------

  describe "adversarial" do
    test "search composes with downstream filter" do
      df =
        Dux.from_list([
          %{id: 1, text: "learning machine"},
          %{id: 2, text: "learning deep"},
          %{id: 3, text: "cooking food"}
        ])
        |> Dux.compute()

      {:table, ref} = df.source
      conn = Dux.Connection.get_conn()

      indexed = Dux.FTS.create_index(df, :id, [:text])

      result =
        Dux.FTS.search(indexed, "learning")
        |> Dux.filter_with("id > 1")
        |> Dux.to_rows()

      try do
        Adbc.Connection.query!(conn, "PRAGMA drop_fts_index('#{ref.name}')")
      catch
        _, _ -> :ok
      end

      assert Enum.all?(result, &(&1["id"] > 1))
    end

    test "50-row dataset indexing and search" do
      # Note: DuckDB 1.4.1 FTS crashes with SIGBUS after repeated index
      # create/drop cycles on larger datasets. Limit to 50 rows here.
      rows = for i <- 1..50, do: %{id: i, text: "document #{i} about topic #{rem(i, 10)}"}
      results = create_and_search(rows, :id, [:text], "topic", limit: 20)
      assert length(results) == 20
    end

    test "custom BM25 parameters" do
      results =
        create_and_search(
          [
            %{id: 1, text: "machine learning"},
            %{id: 2, text: "deep learning"}
          ],
          :id,
          [:text],
          "learning",
          k: 1.5,
          b: 0.5
        )

      assert results != []
    end
  end

  # ---------------------------------------------------------------------------
  # Wicked
  # ---------------------------------------------------------------------------

  describe "wicked" do
    test "search → join with external data" do
      df =
        Dux.from_list([
          %{id: 1, text: "machine learning"},
          %{id: 2, text: "cooking"},
          %{id: 3, text: "deep learning"}
        ])
        |> Dux.compute()

      {:table, ref} = df.source
      conn = Dux.Connection.get_conn()

      indexed = Dux.FTS.create_index(df, :id, [:text])

      metadata =
        Dux.from_list([
          %{id: 1, category: "tech"},
          %{id: 2, category: "food"},
          %{id: 3, category: "tech"}
        ])

      result =
        Dux.FTS.search(indexed, "learning")
        |> Dux.join(metadata, on: :id)
        |> Dux.to_rows()

      try do
        Adbc.Connection.query!(conn, "PRAGMA drop_fts_index('#{ref.name}')")
      catch
        _, _ -> :ok
      end

      assert Enum.all?(result, &(&1["category"] == "tech"))
    end
  end
end
