defmodule Dux.QueryBuilder do
  @moduledoc false

  # Walks a %Dux{} ops list and emits CTE-based SQL.
  # Each operation becomes a CTE: __s0, __s1, __s2, ...
  # DuckDB handles all pushdown optimization across the CTEs.

  @doc """
  Build a SQL query from a %Dux{} struct.

  Returns `{sql_string, setup_sqls}` where:
  - `sql_string` is the final SELECT query (possibly with CTEs)
  - `setup_sqls` is a list of SQL statements to run first (e.g. creating temp tables for list sources)
  """
  def build(%Dux{source: source, ops: ops}, db) do
    {source_sql, setup} = source_to_sql(source, db)

    case ops do
      [] ->
        {"SELECT * FROM (#{source_sql}) __src", setup}

      ops ->
        {ctes, _counter, _groups} = build_ctes(ops, source_sql, 0, [])
        last_cte = "__s#{length(ctes) - 1}"

        cte_clauses =
          ctes
          |> Enum.with_index()
          |> Enum.map_join(",\n  ", fn {sql, i} -> "__s#{i} AS (#{sql})" end)

        final = "WITH\n  #{cte_clauses}\nSELECT * FROM #{last_cte}"
        {final, setup}
    end
  end

  # ---------------------------------------------------------------------------
  # Source SQL generation
  # ---------------------------------------------------------------------------

  defp source_to_sql({:sql, sql}, _db) do
    {sql, []}
  end

  defp source_to_sql({:table, ref}, db) do
    table_name = Dux.Native.table_ensure(db, ref)
    {~s(SELECT * FROM "#{table_name}"), []}
  end

  defp source_to_sql({:list, rows}, _db) when is_list(rows) do
    if rows == [] do
      {"SELECT WHERE false", []}
    else
      values = Enum.map_join(rows, " UNION ALL ", &row_to_select/1)
      {values, []}
    end
  end

  defp source_to_sql({:parquet, path, opts}, _db) do
    escaped = escape_sql_string(path)
    options = parquet_read_options(opts)
    {"SELECT * FROM read_parquet('#{escaped}'#{options})", []}
  end

  # Legacy arity-2 tuple (from old code paths)
  defp source_to_sql({:parquet, path}, _db) do
    {"SELECT * FROM read_parquet('#{escape_sql_string(path)}')", []}
  end

  # Parquet list — multiple files (from distributed partitioning)
  defp source_to_sql({:parquet_list, files, opts}, _db) do
    file_list = Enum.map_join(files, ", ", &"'#{escape_sql_string(&1)}'")
    options = parquet_read_options(opts)
    {"SELECT * FROM read_parquet([#{file_list}]#{options})", []}
  end

  defp source_to_sql({:csv, path, opts}, _db) do
    escaped = escape_sql_string(path)
    options = csv_read_options(opts)
    {"SELECT * FROM read_csv('#{escaped}'#{options})", []}
  end

  defp source_to_sql({:ndjson, path, _opts}, _db) do
    {"SELECT * FROM read_json_auto('#{escape_sql_string(path)}')", []}
  end

  # ---------------------------------------------------------------------------
  # CTE building — each op becomes a CTE
  # ---------------------------------------------------------------------------

  defp build_ctes([], source_sql, counter, _groups) do
    {["SELECT * FROM (#{source_sql}) __src"], counter + 1, []}
  end

  defp build_ctes(ops, source_sql, counter, groups) do
    prev = "(#{source_sql}) __src"

    {ctes, counter, groups} =
      Enum.reduce(ops, {[], counter, groups}, fn op, {ctes, n, groups} ->
        prev_ref = if ctes == [], do: prev, else: "__s#{n - 1}"
        {cte_sql, new_groups} = op_to_sql(op, prev_ref, groups)
        {ctes ++ [cte_sql], n + 1, new_groups}
      end)

    {ctes, counter, groups}
  end

  # ---------------------------------------------------------------------------
  # Operation → SQL
  # ---------------------------------------------------------------------------

  defp op_to_sql({:select, cols}, prev, groups) do
    quoted = Enum.map_join(cols, ", ", &quote_ident/1)
    {"SELECT #{quoted} FROM #{prev}", groups}
  end

  defp op_to_sql({:discard, cols}, prev, groups) do
    excludes = Enum.map_join(cols, ", ", &quote_ident/1)
    {"SELECT * EXCLUDE (#{excludes}) FROM #{prev}", groups}
  end

  defp op_to_sql({:filter, expr}, prev, groups) do
    {"SELECT * FROM #{prev} WHERE #{expr}", groups}
  end

  defp op_to_sql({:head, n}, prev, groups) do
    {"SELECT * FROM #{prev} LIMIT #{n}", groups}
  end

  defp op_to_sql({:slice, offset, length}, prev, groups) do
    {"SELECT * FROM #{prev} LIMIT #{length} OFFSET #{offset}", groups}
  end

  defp op_to_sql({:distinct, nil}, prev, groups) do
    {"SELECT DISTINCT * FROM #{prev}", groups}
  end

  defp op_to_sql({:distinct, cols}, prev, groups) do
    quoted = Enum.map_join(cols, ", ", &quote_ident/1)
    {"SELECT DISTINCT ON (#{quoted}) * FROM #{prev}", groups}
  end

  defp op_to_sql({:drop_nil, cols}, prev, groups) do
    conditions = Enum.map_join(cols, " AND ", &"#{quote_ident(&1)} IS NOT NULL")
    {"SELECT * FROM #{prev} WHERE #{conditions}", groups}
  end

  defp op_to_sql({:mutate, assignments}, prev, groups) do
    extra =
      Enum.map_join(assignments, ", ", fn {name, expr} ->
        "(#{expr}) AS #{quote_ident(name)}"
      end)

    {"SELECT *, #{extra} FROM #{prev}", groups}
  end

  defp op_to_sql({:rename, pairs}, prev, groups) do
    # DuckDB COLUMNS(*) with rename: SELECT COLUMNS(c -> IF(c='old', 'new', c))
    # Simpler approach: use column aliases explicitly
    renames =
      Enum.map_join(pairs, ", ", fn {old, new} ->
        "#{quote_ident(old)} AS #{quote_ident(new)}"
      end)

    excludes = Enum.map_join(pairs, ", ", fn {old, _new} -> quote_ident(old) end)

    {"SELECT * EXCLUDE (#{excludes}), #{renames} FROM #{prev}", groups}
  end

  defp op_to_sql({:sort_by, specs}, prev, groups) do
    order =
      Enum.map_join(specs, ", ", fn
        {:asc, col} -> "#{quote_ident(col)} ASC"
        {:desc, col} -> "#{quote_ident(col)} DESC"
      end)

    {"SELECT * FROM #{prev} ORDER BY #{order}", groups}
  end

  defp op_to_sql({:group_by, cols}, prev, _groups) do
    # group_by doesn't emit a CTE itself — it sets state for the next summarise
    {"SELECT * FROM #{prev}", cols}
  end

  defp op_to_sql({:ungroup}, prev, _groups) do
    {"SELECT * FROM #{prev}", []}
  end

  defp op_to_sql({:summarise, aggs}, prev, groups) do
    group_cols = Enum.map_join(groups, ", ", &quote_ident/1)

    agg_cols =
      Enum.map_join(aggs, ", ", fn {name, expr} ->
        "(#{expr}) AS #{quote_ident(name)}"
      end)

    select_cols =
      if groups == [] do
        agg_cols
      else
        "#{group_cols}, #{agg_cols}"
      end

    group_clause = if groups == [], do: "", else: " GROUP BY #{group_cols}"

    {"SELECT #{select_cols} FROM #{prev}#{group_clause}", []}
  end

  defp op_to_sql({:join, right, how, on_cols, _suffix}, prev, groups) do
    # The right side is inlined as a subquery
    right_db = Dux.Connection.get_db()
    {right_sql, _setup} = source_to_sql(right.source, right_db)
    right_ref = "(#{right_sql}) __right"

    # Wrap prev in a named subquery so ON conditions can reference it
    left_ref = "(SELECT * FROM #{prev}) __left"

    join_type = join_type_sql(how)

    join_clause = build_join_clause(join_type, right_ref, on_cols, "__left")
    {"SELECT * FROM #{left_ref} #{join_clause}", groups}
  end

  defp op_to_sql({:concat_rows, others}, prev, groups) do
    # UNION ALL the current result with each other Dux
    union_parts =
      Enum.map(others, fn %Dux{source: source, ops: ops} ->
        db = Dux.Connection.get_db()
        {sql, _setup} = source_to_sql(source, db)

        case ops do
          [] -> "(#{sql}) __src"
          _ops -> "(#{sql}) __src"
        end
      end)

    parts =
      Enum.map_join(union_parts, " UNION ALL ", fn ref ->
        "SELECT * FROM #{ref}"
      end)

    {"SELECT * FROM #{prev} UNION ALL #{parts}", groups}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp build_join_clause(join_type, right_ref, nil, _prev) do
    "#{join_type} #{right_ref}"
  end

  defp build_join_clause(join_type, right_ref, cols, prev) do
    all_same? = Enum.all?(cols, fn {l, r} -> l == r end)

    if all_same? do
      using = Enum.map_join(cols, ", ", fn {l, _} -> quote_ident(l) end)
      "#{join_type} #{right_ref} USING (#{using})"
    else
      on_cond =
        Enum.map_join(cols, " AND ", fn {l, r} ->
          "#{prev}.#{quote_ident(l)} = __right.#{quote_ident(r)}"
        end)

      "#{join_type} #{right_ref} ON #{on_cond}"
    end
  end

  defp row_to_select(row) do
    cols =
      row
      |> Enum.map(fn {k, v} -> {to_string(k), v} end)
      |> Enum.sort_by(fn {k, _v} -> k end)
      |> Enum.map_join(", ", fn {k, v} -> "#{encode_value(v)} AS #{quote_ident(k)}" end)

    "SELECT #{cols}"
  end

  defp quote_ident(name) when is_atom(name), do: quote_ident(to_string(name))

  defp quote_ident(name) do
    escaped = String.replace(name, ~s("), ~s(""))
    ~s("#{escaped}")
  end

  defp join_type_sql(:inner), do: "INNER JOIN"
  defp join_type_sql(:left), do: "LEFT JOIN"
  defp join_type_sql(:right), do: "RIGHT JOIN"
  defp join_type_sql(:cross), do: "CROSS JOIN"
  defp join_type_sql(:anti), do: "ANTI JOIN"
  defp join_type_sql(:semi), do: "SEMI JOIN"

  defp encode_value(nil), do: "NULL"
  defp encode_value(v) when is_integer(v), do: Integer.to_string(v)
  defp encode_value(v) when is_float(v), do: Float.to_string(v)
  defp encode_value(true), do: "true"
  defp encode_value(false), do: "false"

  defp encode_value(v) when is_binary(v) do
    "'#{escape_sql_string(v)}'"
  end

  defp escape_sql_string(s), do: String.replace(s, "'", "''")

  defp csv_read_options([]), do: ""

  defp csv_read_options(opts) do
    parts =
      Enum.flat_map(opts, fn
        {:delimiter, d} -> ["delim = '#{escape_sql_string(d)}'"]
        {:header, true} -> ["header = true"]
        {:header, false} -> ["header = false"]
        {:skip, n} -> ["skip = #{n}"]
        {:null_padding, true} -> ["null_padding = true"]
        {:auto_detect, false} -> ["auto_detect = false"]
        _ -> []
      end)

    case parts do
      [] -> ""
      _ -> ", " <> Enum.join(parts, ", ")
    end
  end

  defp parquet_read_options([]), do: ""

  defp parquet_read_options(opts) do
    parts =
      Enum.flat_map(opts, fn
        {:hive_partitioning, true} -> ["hive_partitioning = true"]
        {:union_by_name, true} -> ["union_by_name = true"]
        _ -> []
      end)

    case parts do
      [] -> ""
      _ -> ", " <> Enum.join(parts, ", ")
    end
  end
end
