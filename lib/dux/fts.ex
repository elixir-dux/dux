defmodule Dux.FTS do
  @moduledoc """
  Full-text search with BM25 scoring.

  DuckDB's FTS extension autoloads and provides inverted index creation
  and BM25-ranked search. Indexes are created on computed (materialized)
  dataframes — since Dux temp tables are immutable, the FTS limitation
  of not auto-updating on INSERT is not a concern.

  ## Usage

      df = Dux.from_list([
        %{id: 1, title: "Machine learning basics", body: "An intro to ML..."},
        %{id: 2, title: "Cooking recipes", body: "How to make pasta..."},
        %{id: 3, title: "Deep learning models", body: "Neural networks..."}
      ]) |> Dux.compute()

      indexed = Dux.FTS.create_index(df, :id, [:title, :body], stemmer: "english")
      results = Dux.FTS.search(indexed, "machine learning", limit: 10)
      Dux.to_rows(results)

  ## Supported stemmers

  arabic, basque, catalan, danish, dutch, english, finnish, french, german,
  greek, hindi, hungarian, indonesian, irish, italian, lithuanian, nepali,
  norwegian, porter, portuguese, romanian, russian, serbian, spanish,
  swedish, tamil, turkish.
  """

  import Dux.SQL.Helpers, only: [qi: 1]

  @doc """
  Create a full-text search index on a materialized dataframe.

  The dataframe must be computed first (`Dux.compute/1`) so it has a
  backing temp table. The `id_column` uniquely identifies rows.
  `text_columns` are the columns to index.

  ## Options

    * `:stemmer` — stemming language (default: `"english"`)
    * `:stopwords` — stopword list (default: `"english"`)
    * `:strip_accents` — remove accents (default: `true`)
    * `:lower` — lowercase before indexing (default: `true`)

  Returns the same `%Dux{}` with FTS metadata attached for use with `search/3`.
  """
  def create_index(dux, id_column, text_columns, opts \\ [])

  def create_index(%Dux{source: {:table, ref}} = dux, id_column, text_columns, opts) do
    stemmer = Keyword.get(opts, :stemmer, "english")
    stopwords = Keyword.get(opts, :stopwords, "english")
    strip_accents = if Keyword.get(opts, :strip_accents, true), do: 1, else: 0
    lower = if Keyword.get(opts, :lower, true), do: 1, else: 0

    conn = Dux.Connection.get_conn()

    # Materialize to a SQL-created temp table. FTS indexes crash (SIGBUS) on
    # tables created via ADBC ingest in DuckDB 1.4.1. Copying via
    # CREATE TEMP TABLE AS SELECT avoids this by creating a standard catalog entry.
    fts_table = "__dux_fts_#{:erlang.unique_integer([:positive])}"

    Adbc.Connection.query!(
      conn,
      ~s(CREATE TEMP TABLE "#{fts_table}" AS SELECT * FROM "#{ref.name}")
    )

    # DuckDB defaults to the HTTP extension repository. In locked-down
    # environments HTTPS is often the only allowed transport, so pin the
    # core extension repository explicitly before installing FTS.
    Adbc.Connection.query!(
      conn,
      "SET custom_extension_repository = 'https://extensions.duckdb.org'"
    )

    Adbc.Connection.query!(conn, "INSTALL fts FROM 'https://extensions.duckdb.org'; LOAD fts;")

    col_args = Enum.map_join(text_columns, ", ", &"'#{to_string(&1)}'")

    Adbc.Connection.query!(conn, """
    PRAGMA create_fts_index(
      '#{escape(fts_table)}', '#{escape(to_string(id_column))}', #{col_args},
      stemmer='#{escape(stemmer)}', stopwords='#{escape(stopwords)}',
      strip_accents=#{strip_accents}, lower=#{lower}
    )
    """)

    fts_meta = %{
      table_name: fts_table,
      source_ref: ref,
      id_column: to_string(id_column),
      text_columns: Enum.map(text_columns, &to_string/1)
    }

    Process.put({:dux_fts, fts_table}, fts_meta)

    # Return a Dux backed by the FTS table (SQL source, not table ref,
    # so it doesn't get GC'd and FTS internal tables survive)
    %{dux | source: {:sql, ~s(SELECT * FROM "#{fts_table}")}}
  end

  def create_index(%Dux{}, _id, _cols, _opts) do
    raise ArgumentError,
          "Dux.FTS.create_index requires a computed dataframe. Call Dux.compute/1 first."
  end

  @doc """
  Search a full-text indexed dataframe.

  Returns a `%Dux{}` with an `fts_score` column, ordered by relevance
  (highest score first). Only matching rows are included.

  ## Options

    * `:limit` — maximum number of results (default: no limit)
    * `:fields` — restrict search to specific columns (default: all indexed)
    * `:k` — BM25 k1 parameter (default: DuckDB default, typically 1.2)
    * `:b` — BM25 b parameter (default: DuckDB default, typically 0.75)

  ## Examples

      Dux.FTS.search(indexed_df, "machine learning", limit: 10)
      Dux.FTS.search(indexed_df, "neural networks", fields: [:title], limit: 5)
  """
  def search(%Dux{} = dux, query_text, opts \\ []) do
    limit = Keyword.get(opts, :limit)
    fields = Keyword.get(opts, :fields)
    k = Keyword.get(opts, :k)
    b = Keyword.get(opts, :b)

    fts_table = find_fts_table(dux)

    fts_meta =
      Process.get({:dux_fts, fts_table}) ||
        raise ArgumentError, "no FTS index found. Call Dux.FTS.create_index/4 first."

    table_name = fts_meta.table_name
    id_col = fts_meta.id_column

    macro_name = "fts_main_#{table_name}.match_bm25"
    extra_params = build_bm25_params(fields, k, b)
    escaped_query = escape(query_text)
    limit_clause = if limit, do: " LIMIT #{limit}", else: ""

    search_sql = """
    SELECT *, #{macro_name}(#{qi(id_col)}, '#{escaped_query}'#{extra_params}) AS fts_score
    FROM #{qi(table_name)}
    WHERE fts_score IS NOT NULL
    ORDER BY fts_score#{limit_clause}
    """

    Dux.from_query(search_sql)
  end

  defp find_fts_table(%Dux{source: {:table, ref}}), do: ref.name

  defp find_fts_table(%Dux{source: {:sql, sql}}) do
    # Extract table name from "SELECT * FROM \"table_name\""
    case Regex.run(~r/FROM\s+"([^"]+)"/, sql) do
      [_, name] -> name
      _ -> raise ArgumentError, "no FTS index found. Call Dux.FTS.create_index/4 first."
    end
  end

  defp find_fts_table(_) do
    raise ArgumentError, "no FTS index found. Call Dux.FTS.create_index/4 first."
  end

  defp build_bm25_params(fields, k, b) do
    params = []

    params =
      if fields do
        field_str = Enum.map_join(fields, ",", &to_string/1)
        params ++ ["fields := '#{escape(field_str)}'"]
      else
        params
      end

    params = if k, do: params ++ ["k := #{k}"], else: params
    params = if b, do: params ++ ["b := #{b}"], else: params

    if params == [], do: "", else: ", " <> Enum.join(params, ", ")
  end

  defp escape(str), do: String.replace(str, "'", "''")
end
