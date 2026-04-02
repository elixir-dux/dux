defmodule Dux.Query.Compiler do
  @moduledoc false

  # Compiles Dux query AST nodes into {sql_string, params} tuples.
  # The AST is produced by Dux.Query macros at compile time.
  # Pins become $N parameter bindings — values never appear in SQL strings.

  @doc """
  Compile a Dux query AST node into `{sql_string, params_list}`.

  The `pins` list contains the runtime values for `^` interpolations,
  in the order they were encountered during macro expansion.
  """
  def to_sql(ast, pins) do
    {sql, params, _idx} = compile(ast, pins, 0)
    {sql, params}
  end

  # --- Literals ---

  defp compile({:lit, nil}, _pins, idx) do
    {"NULL", [], idx}
  end

  defp compile({:lit, value}, _pins, idx) when is_integer(value) do
    {Integer.to_string(value), [], idx}
  end

  defp compile({:lit, value}, _pins, idx) when is_float(value) do
    {Float.to_string(value), [], idx}
  end

  defp compile({:lit, value}, _pins, idx) when is_binary(value) do
    escaped = String.replace(value, "'", "''")
    {"'#{escaped}'", [], idx}
  end

  defp compile({:lit, true}, _pins, idx), do: {"true", [], idx}
  defp compile({:lit, false}, _pins, idx), do: {"false", [], idx}

  # --- Column references ---

  defp compile({:column, name}, _pins, idx) do
    {quote_ident(name), [], idx}
  end

  defp compile({:dynamic_column, pin_idx}, pins, idx) do
    name = Enum.at(pins, pin_idx)
    {quote_ident(name), [], idx}
  end

  # --- Pin (interpolated value) ---

  defp compile({:pin, pin_idx}, pins, idx) do
    value = Enum.at(pins, pin_idx)
    {"$#{idx + 1}", [value], idx + 1}
  end

  # --- Binary operators ---

  @binary_ops %{
    eq: "=",
    neq: "!=",
    gt: ">",
    gte: ">=",
    lt: "<",
    lte: "<=",
    add: "+",
    sub: "-",
    mul: "*",
    div: "/",
    and: "AND",
    or: "OR"
  }

  for {op, sql_op} <- @binary_ops do
    defp compile({unquote(op), left, right}, pins, idx) do
      {l_sql, l_params, idx} = compile(left, pins, idx)
      {r_sql, r_params, idx} = compile(right, pins, idx)
      {"(#{l_sql} #{unquote(sql_op)} #{r_sql})", l_params ++ r_params, idx}
    end
  end

  # --- Unary operators ---

  defp compile({:not, expr}, pins, idx) do
    {sql, params, idx} = compile(expr, pins, idx)
    {"(NOT #{sql})", params, idx}
  end

  defp compile({:negate, expr}, pins, idx) do
    {sql, params, idx} = compile(expr, pins, idx)
    {"(- #{sql})", params, idx}
  end

  # --- String concatenation ---

  defp compile({:concat, left, right}, pins, idx) do
    {l_sql, l_params, idx} = compile(left, pins, idx)
    {r_sql, r_params, idx} = compile(right, pins, idx)
    {"(#{l_sql} || #{r_sql})", l_params ++ r_params, idx}
  end

  # --- Function calls (aggregations, DuckDB functions) ---

  @known_aggregations ~w(sum avg mean min max count count_distinct std variance)a
  @known_functions ~w(abs round ceil floor length lower upper trim
                      cast coalesce nullif greatest least
                      year month day hour minute second
                      date_trunc date_part epoch
                      regexp_matches regexp_replace regexp_extract
                      string_split list_value list_sort)a

  defp compile({:call, :mean, args}, pins, idx) do
    # mean is AVG in SQL
    compile({:call, :avg, args}, pins, idx)
  end

  defp compile({:call, :std, args}, pins, idx) do
    compile({:call, :stddev_samp, args}, pins, idx)
  end

  defp compile({:call, :count_distinct, [arg]}, pins, idx) do
    {arg_sql, arg_params, idx} = compile(arg, pins, idx)
    {"COUNT(DISTINCT #{arg_sql})", arg_params, idx}
  end

  defp compile({:call, func, args}, pins, idx)
       when func in @known_aggregations or func in @known_functions or true do
    # Generic function call — pass through to DuckDB
    sql_name = func |> to_string() |> String.upcase()

    {arg_sqls, all_params, idx} =
      Enum.reduce(args, {[], [], idx}, fn arg, {sqls, params, idx} ->
        {sql, new_params, idx} = compile(arg, pins, idx)
        {sqls ++ [sql], params ++ new_params, idx}
      end)

    {"#{sql_name}(#{Enum.join(arg_sqls, ", ")})", all_params, idx}
  end

  # --- Window functions: OVER (PARTITION BY ... ORDER BY ...) ---

  defp compile({:over, expr, partition_by, order_by, frame}, pins, idx) do
    {expr_sql, expr_params, idx} = compile(expr, pins, idx)
    {partition_clause, partition_params, idx} = compile_partition_by(partition_by, pins, idx)
    {order_clause, order_params, idx} = compile_order_by(order_by, pins, idx)
    frame_clause = compile_frame(frame)

    window_parts =
      [partition_clause, order_clause, frame_clause]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join(" ")

    {"#{expr_sql} OVER (#{window_parts})", expr_params ++ partition_params ++ order_params, idx}
  end

  # --- CASE WHEN ---

  defp compile({:case_when, pairs, else_expr}, pins, idx) do
    {when_clauses, all_params, idx} =
      Enum.reduce(pairs, {[], [], idx}, fn {condition, result}, {clauses, params, idx} ->
        {cond_sql, cond_params, idx} = compile(condition, pins, idx)
        {result_sql, result_params, idx} = compile(result, pins, idx)
        clause = "WHEN #{cond_sql} THEN #{result_sql}"
        {clauses ++ [clause], params ++ cond_params ++ result_params, idx}
      end)

    {else_clause, else_params, idx} =
      case else_expr do
        nil ->
          {"", [], idx}

        expr ->
          {sql, params, idx} = compile(expr, pins, idx)
          {" ELSE #{sql}", params, idx}
      end

    sql = "(CASE #{Enum.join(when_clauses, " ")}#{else_clause} END)"
    {sql, all_params ++ else_params, idx}
  end

  # --- IN operator ---

  defp compile({:in, left, {:pin, pin_idx}}, pins, idx) do
    {l_sql, l_params, idx} = compile(left, pins, idx)
    values = Enum.at(pins, pin_idx)
    # Pinned list — expand to individual parameter bindings
    {placeholders, idx} =
      Enum.reduce(values, {[], idx}, fn _v, {phs, idx} ->
        {phs ++ ["$#{idx + 1}"], idx + 1}
      end)

    {"(#{l_sql} IN (#{Enum.join(placeholders, ", ")}))", l_params ++ values, idx}
  end

  defp compile({:in, left, right}, pins, idx) when is_list(right) do
    {l_sql, l_params, idx} = compile(left, pins, idx)

    {val_sqls, val_params, idx} =
      Enum.reduce(right, {[], [], idx}, fn item, {sqls, params, idx} ->
        {sql, new_params, idx} = compile(item, pins, idx)
        {sqls ++ [sql], params ++ new_params, idx}
      end)

    {"(#{l_sql} IN (#{Enum.join(val_sqls, ", ")}))", l_params ++ val_params, idx}
  end

  # --- Sort direction markers ---

  defp compile({:asc, expr}, pins, idx) do
    {sql, params, idx} = compile(expr, pins, idx)
    {"#{sql} ASC", params, idx}
  end

  defp compile({:desc, expr}, pins, idx) do
    {sql, params, idx} = compile(expr, pins, idx)
    {"#{sql} DESC", params, idx}
  end

  # --- Frame clause compilation ---

  defp compile_frame(nil), do: ""
  defp compile_frame(s) when is_binary(s), do: s

  defp compile_frame({type, start_bound, end_bound}) when type in [:rows, :range, :groups] do
    type_str = type |> to_string() |> String.upcase()
    "#{type_str} BETWEEN #{frame_bound(start_bound, :start)} AND #{frame_bound(end_bound, :end)}"
  end

  defp compile_frame({type, start_bound, end_bound, opts})
       when type in [:rows, :range, :groups] and is_list(opts) do
    base = compile_frame({type, start_bound, end_bound})

    case Keyword.get(opts, :exclude) do
      nil -> base
      :current -> "#{base} EXCLUDE CURRENT ROW"
      :group -> "#{base} EXCLUDE GROUP"
      :ties -> "#{base} EXCLUDE TIES"
      :no_others -> "#{base} EXCLUDE NO OTHERS"
    end
  end

  # Frame bounds: negative = PRECEDING, positive = FOLLOWING, 0 = CURRENT ROW.
  # :unbounded in start position = UNBOUNDED PRECEDING
  # :unbounded in end position = UNBOUNDED FOLLOWING
  # We compile start and end separately to handle :unbounded direction.
  defp frame_bound(n, _position) when is_integer(n) and n < 0, do: "#{abs(n)} PRECEDING"
  defp frame_bound(n, _position) when is_integer(n) and n > 0, do: "#{n} FOLLOWING"
  defp frame_bound(0, _position), do: "CURRENT ROW"
  defp frame_bound(:current, _position), do: "CURRENT ROW"
  defp frame_bound(:unbounded, :start), do: "UNBOUNDED PRECEDING"
  defp frame_bound(:unbounded, :end), do: "UNBOUNDED FOLLOWING"

  # --- Window function helpers ---

  defp compile_partition_by([], _pins, idx), do: {"", [], idx}

  defp compile_partition_by(cols, pins, idx) do
    {col_sqls, all_params, idx} = compile_list(cols, pins, idx)
    {"PARTITION BY #{Enum.join(col_sqls, ", ")}", all_params, idx}
  end

  defp compile_order_by([], _pins, idx), do: {"", [], idx}

  defp compile_order_by(specs, pins, idx) do
    {spec_sqls, all_params, idx} =
      Enum.reduce(specs, {[], [], idx}, fn {dir, col_ast}, {sqls, params, idx} ->
        {sql, new_params, idx} = compile(col_ast, pins, idx)
        dir_str = if dir == :desc, do: "DESC", else: "ASC"
        {sqls ++ ["#{sql} #{dir_str}"], params ++ new_params, idx}
      end)

    {"ORDER BY #{Enum.join(spec_sqls, ", ")}", all_params, idx}
  end

  defp compile_list(items, pins, idx) do
    Enum.reduce(items, {[], [], idx}, fn item, {sqls, params, idx} ->
      {sql, new_params, idx} = compile(item, pins, idx)
      {sqls ++ [sql], params ++ new_params, idx}
    end)
  end

  # --- Helpers ---

  defp quote_ident(name) do
    escaped = String.replace(name, ~s("), ~s(""))
    ~s("#{escaped}")
  end
end
