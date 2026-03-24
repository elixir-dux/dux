defmodule Dux.Query do
  alias Dux.Query.Compiler

  @moduledoc """
  Macro-based query expressions for Dux.

  Inside a query, bare identifiers (like `x`, `name`) refer to column names.
  Use `^` to interpolate Elixir variables — these become parameter bindings
  in the generated SQL, preventing SQL injection by construction.

  Queries are used in `Dux.filter/2`, `Dux.mutate/2`, `Dux.summarise/2`,
  and `Dux.sort_by/2`.

  ## Operators

  | Elixir | SQL |
  |--------|-----|
  | `==`, `!=`, `>`, `>=`, `<`, `<=` | `=`, `!=`, `>`, `>=`, `<`, `<=` |
  | `+`, `-`, `*`, `/` | `+`, `-`, `*`, `/` |
  | `and`, `or`, `not` | `AND`, `OR`, `NOT` |
  | `<>` | `\|\|` (string concatenation) |
  | `in [...]` | `IN (...)` |

  ## Conditional expressions

  Elixir's `cond` maps to SQL `CASE WHEN`:

      Dux.mutate(df,
        tier: cond do
          amount > 1000 -> "gold"
          amount > 100 -> "silver"
          true -> "bronze"
        end
      )

  Elixir's `if/else` for simple two-branch conditionals:

      Dux.mutate(df, label: if(amount > 0, do: "positive", else: "negative"))

  ## Membership test

  Elixir's `in` works with literal and pinned lists:

      Dux.filter(df, status in ["active", "pending"])

      allowed = ["active", "pending"]
      Dux.filter(df, status in ^allowed)

  ## Interpolation

  Use `^` to interpolate Elixir values as parameter bindings:

      min_val = 10
      Dux.filter(df, x > ^min_val)

  ## Aggregation functions

  `sum`, `avg`/`mean`, `min`, `max`, `count`, `count_distinct`, `std`, `variance`

  ## DuckDB functions

  **All DuckDB functions work in queries.** Function calls pass through
  to DuckDB unchanged. A few examples by category:

  **Date/time:**

      Dux.mutate(df, y: year(d), m: month(d), trunc: date_trunc("month", d))
      Dux.mutate(df, age_days: date_diff("day", created_at, current_date()))

  **String:**

      Dux.mutate(df, low: lower(name), parts: string_split(path, "/"))
      Dux.filter(df, starts_with(name, "A"))
      Dux.mutate(df, clean: trim(replace(name, "  ", " ")))

  **Regex:**

      Dux.filter(df, regexp_matches(email, ".*@gmail\\\\.com"))
      Dux.mutate(df, domain: regexp_extract(email, "@(.+)", 1))

  **List/Array:**

      Dux.mutate(df, first: list_extract(tags, 1), n: len(tags))

  **Struct:**

      Dux.mutate(df, city: struct_extract(address, "city"))

  **Math:**

      Dux.mutate(df, log_amt: ln(amount), pct: round(ratio * 100, 2))

  **Null handling:**

      Dux.mutate(df, safe: coalesce(nullable_col, 0))

  **Type casting:**

      Dux.mutate(df, as_text: cast(id, "VARCHAR"), as_int: cast(amount, "INTEGER"))

  For the full list, see the
  [DuckDB Functions reference](https://duckdb.org/docs/sql/functions/overview).

  For anything the macro doesn't support (window functions, subqueries),
  use the `_with` variants (`mutate_with/2`, `filter_with/2`) which accept
  raw DuckDB SQL strings.

  ## Column references

  `col("name")` for columns with spaces or special characters:

      Dux.filter(df, col("Total Amount") > 100)

  ## Examples

      # Filter
      Dux.filter(df, age > 18 and status == "active")

      # Mutate with conditional
      Dux.mutate(df,
        revenue: price * quantity,
        tier: cond do
          price > 100 -> "premium"
          true -> "standard"
        end
      )

      # Summarise
      Dux.summarise(df, total: sum(amount), n: count(id))

      # Sort
      Dux.sort_by(df, desc: total)
  """

  @doc """
  Compiles a query expression into a `{sql_string, params}` tuple.

  This is the entry point used by `Dux.filter/2` and friends.
  """
  defmacro query(expression) do
    {ast, pins} = traverse(expression, [])

    quote do
      pins = unquote(Enum.reverse(pins))
      Compiler.to_sql(unquote(Macro.escape(ast)), pins)
    end
  end

  @doc """
  Compiles a keyword list of query expressions (for mutate/summarise).

  Returns a list of `{name, {sql_string, params}}` tuples.
  """
  defmacro query_pairs(pairs) do
    compiled =
      Enum.map(pairs, fn {name, expression} ->
        {ast, pins} = traverse(expression, [])

        quote do
          {unquote(to_string(name)),
           (fn ->
              pins = unquote(Enum.reverse(pins))
              Compiler.to_sql(unquote(Macro.escape(ast)), pins)
            end).()}
        end
      end)

    compiled
  end

  # ---------------------------------------------------------------------------
  # AST traversal — converts Elixir AST to Dux query AST
  # ---------------------------------------------------------------------------

  @doc false
  def traverse_public(expression, pins), do: traverse(expression, pins)

  # Pin: ^expr — interpolate an Elixir value
  defp traverse({:^, _meta, [expr]}, pins) do
    idx = length(pins)
    {{:pin, idx}, [expr | pins]}
  end

  # col("name") — explicit column reference
  defp traverse({:col, _meta, [name]}, pins) when is_binary(name) do
    {{:column, name}, pins}
  end

  defp traverse({:col, _meta, [{:^, _, [expr]}]}, pins) do
    idx = length(pins)
    {{:dynamic_column, idx}, [expr | pins]}
  end

  # Bare variable — becomes a column reference
  defp traverse({var, _meta, ctx}, pins) when is_atom(var) and is_atom(ctx) do
    {{:column, to_string(var)}, pins}
  end

  # Binary operators
  defp traverse({op, _meta, [left, right]}, pins)
       when op in [:==, :!=, :>, :>=, :<, :<=, :+, :-, :*, :/, :and, :or] do
    {l_ast, pins} = traverse(left, pins)
    {r_ast, pins} = traverse(right, pins)
    dux_op = translate_op(op)
    {{dux_op, l_ast, r_ast}, pins}
  end

  # Unary not
  defp traverse({:not, _meta, [expr]}, pins) do
    {ast, pins} = traverse(expr, pins)
    {{:not, ast}, pins}
  end

  # Unary minus
  defp traverse({:-, _meta, [expr]}, pins) when not is_number(expr) do
    {ast, pins} = traverse(expr, pins)
    {{:negate, ast}, pins}
  end

  # cond → CASE WHEN ... THEN ... ELSE ... END
  defp traverse({:cond, _meta, [[do: clauses]]}, pins) do
    {pairs, else_expr, pins} =
      Enum.reduce(clauses, {[], nil, pins}, fn {:->, _m, [[condition], result]},
                                               {pairs, _else, pins} ->
        {cond_ast, pins} = traverse(condition, pins)
        {result_ast, pins} = traverse(result, pins)

        case cond_ast do
          {:lit, true} ->
            # `true -> expr` becomes the ELSE branch
            {pairs, result_ast, pins}

          _ ->
            {pairs ++ [{cond_ast, result_ast}], nil, pins}
        end
      end)

    {{:case_when, pairs, else_expr}, pins}
  end

  # if/else → CASE WHEN cond THEN then_expr ELSE else_expr END
  defp traverse({:if, _meta, [condition, [do: then_expr, else: else_expr]]}, pins) do
    {cond_ast, pins} = traverse(condition, pins)
    {then_ast, pins} = traverse(then_expr, pins)
    {else_ast, pins} = traverse(else_expr, pins)
    {{:case_when, [{cond_ast, then_ast}], else_ast}, pins}
  end

  # if without else → CASE WHEN cond THEN then_expr ELSE NULL END
  defp traverse({:if, _meta, [condition, [do: then_expr]]}, pins) do
    {cond_ast, pins} = traverse(condition, pins)
    {then_ast, pins} = traverse(then_expr, pins)
    {{:case_when, [{cond_ast, then_ast}], {:lit, nil}}, pins}
  end

  # in operator → SQL IN
  defp traverse({:in, _meta, [left, right]}, pins) do
    {l_ast, pins} = traverse(left, pins)
    {r_ast, pins} = traverse(right, pins)
    {{:in, l_ast, r_ast}, pins}
  end

  # String concatenation: <>
  defp traverse({:<>, _meta, [left, right]}, pins) do
    {l_ast, pins} = traverse(left, pins)
    {r_ast, pins} = traverse(right, pins)
    {{:concat, l_ast, r_ast}, pins}
  end

  # Function calls — aggregations and other functions
  defp traverse({func, _meta, args}, pins) when is_atom(func) and is_list(args) do
    {arg_asts, pins} =
      Enum.map_reduce(args, pins, fn arg, pins -> traverse(arg, pins) end)

    {{:call, func, arg_asts}, pins}
  end

  # Keyword pairs (for sort_by: [asc: :col, desc: :col])
  defp traverse({key, value}, pins) when is_atom(key) do
    {v_ast, pins} = traverse(value, pins)
    {{key, v_ast}, pins}
  end

  # Literals
  defp traverse(value, pins) when is_number(value) do
    {{:lit, value}, pins}
  end

  defp traverse(value, pins) when is_binary(value) do
    {{:lit, value}, pins}
  end

  defp traverse(value, pins) when is_boolean(value) do
    {{:lit, value}, pins}
  end

  defp traverse(nil, pins) do
    {{:lit, nil}, pins}
  end

  defp traverse(value, pins) when is_atom(value) do
    {{:column, to_string(value)}, pins}
  end

  # Lists (e.g. for sort_by)
  defp traverse(list, pins) when is_list(list) do
    Enum.map_reduce(list, pins, &traverse/2)
  end

  # ---------------------------------------------------------------------------
  # Operator translation
  # ---------------------------------------------------------------------------

  defp translate_op(:==), do: :eq
  defp translate_op(:!=), do: :neq
  defp translate_op(:>), do: :gt
  defp translate_op(:>=), do: :gte
  defp translate_op(:<), do: :lt
  defp translate_op(:<=), do: :lte
  defp translate_op(:+), do: :add
  defp translate_op(:-), do: :sub
  defp translate_op(:*), do: :mul
  defp translate_op(:/), do: :div
  defp translate_op(:and), do: :and
  defp translate_op(:or), do: :or
end
