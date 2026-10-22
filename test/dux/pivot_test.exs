defmodule Dux.PivotTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  test "resolves filtered and aggregated input before downstream operations" do
    df =
      Dux.from_list([
        %{region: "EU", kind: "a", amount: 2},
        %{region: "EU", kind: "a", amount: 3},
        %{region: "US", kind: "b", amount: 7}
      ])
      |> Dux.filter_with("amount > 0")
      |> Dux.group_by([:region, :kind])
      |> Dux.summarise_with(amount: "SUM(amount)")
      |> Dux.pivot_wider(:kind, :amount)
      |> Dux.sort_by(:region)

    expected = [
      %{"region" => "EU", "a" => 5, "b" => nil},
      %{"region" => "US", "a" => nil, "b" => 7}
    ]

    assert Dux.to_rows(df) == expected
    assert df |> Dux.compute() |> Dux.to_rows() == expected
  end

  test "repeated pivots retain their complete input scopes" do
    result =
      Dux.from_list([
        %{region: "EU", kind: "a", amount: 2},
        %{region: "US", kind: "b", amount: 7}
      ])
      |> Dux.filter_with("amount > 0")
      |> Dux.pivot_wider(:kind, :amount)
      |> Dux.pivot_longer([:a, :b], names_to: "kind", values_to: "amount")
      |> Dux.pivot_wider(:kind, :amount)
      |> Dux.sort_by(:region)
      |> Dux.to_rows()

    assert result == [
             %{"region" => "EU", "a" => 2, "b" => nil},
             %{"region" => "US", "a" => nil, "b" => 7}
           ]
  end

  test "preserves quoted category names from a materialized source" do
    result =
      Dux.from_list([%{kind: "quotes ' and 雪", amount: 2}])
      |> Dux.compute()
      |> Dux.filter_with("amount > 0")
      |> Dux.pivot_wider(:kind, :amount)
      |> Dux.to_columns()

    assert result == %{"quotes ' and 雪" => [2]}
  end

  test "reports missing pivot columns" do
    assert_raise ArgumentError, ~r/missing/, fn ->
      Dux.from_list([%{kind: "a", amount: 2}])
      |> Dux.filter_with("amount > 0")
      |> Dux.pivot_wider(:missing, :amount)
      |> Dux.to_columns()
    end
  end

  property "pivot preserves the sum across categories" do
    check all(values <- list_of(integer(-100..100), min_length: 1, max_length: 30), max_runs: 20) do
      rows =
        values
        |> Enum.with_index()
        |> Enum.map(fn {value, i} ->
          %{kind: if(rem(i, 2) == 0, do: "a", else: "b"), amount: value}
        end)

      [pivot] =
        rows
        |> Dux.from_list()
        |> Dux.filter_with("amount IS NOT NULL")
        |> Dux.pivot_wider(:kind, :amount)
        |> Dux.to_rows()

      assert pivot |> Map.values() |> Enum.sum() == Enum.sum(values)
    end
  end
end
