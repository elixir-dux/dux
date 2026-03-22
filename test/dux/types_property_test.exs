defmodule Dux.TypesPropertyTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  # The Connection is started by the application supervisor.
  # We use it directly for these tests.

  setup_all do
    conn = Dux.Connection.get_conn()
    %{conn: conn}
  end

  describe "integer round-trip" do
    property "integers survive query → extract", %{conn: conn} do
      check all(i <- integer(-1_000_000..1_000_000)) do
        ref = Dux.Backend.query(conn, "SELECT #{i}::BIGINT AS val")
        %{"val" => [result]} = Dux.Backend.table_to_columns(conn, ref)
        assert result == i
      end
    end
  end

  describe "float round-trip" do
    property "finite floats survive query → extract", %{conn: conn} do
      check all(f <- float(min: -1.0e10, max: 1.0e10)) do
        ref = Dux.Backend.query(conn, "SELECT #{f}::DOUBLE AS val")
        %{"val" => [result]} = Dux.Backend.table_to_columns(conn, ref)
        assert_in_delta result, f, abs(f) * 1.0e-10 + 1.0e-15
      end
    end
  end

  describe "string round-trip" do
    property "strings survive query → extract", %{conn: conn} do
      # min_length: 1 because ADBC/DuckDB converts empty string to nil
      check all(s <- string(:printable, min_length: 1, max_length: 200)) do
        # Escape single quotes for SQL
        escaped = String.replace(s, "'", "''")
        ref = Dux.Backend.query(conn, "SELECT '#{escaped}'::VARCHAR AS val")
        %{"val" => [result]} = Dux.Backend.table_to_columns(conn, ref)
        assert result == s
      end
    end
  end

  describe "boolean round-trip" do
    property "booleans survive query → extract", %{conn: conn} do
      check all(b <- boolean()) do
        sql_bool = if b, do: "true", else: "false"
        ref = Dux.Backend.query(conn, "SELECT #{sql_bool}::BOOLEAN AS val")
        %{"val" => [result]} = Dux.Backend.table_to_columns(conn, ref)
        assert result == b
      end
    end
  end

  describe "null handling" do
    property "nulls are preserved across all types", %{conn: conn} do
      types = ["BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN", "DATE"]

      check all(type <- member_of(types)) do
        ref = Dux.Backend.query(conn, "SELECT NULL::#{type} AS val")
        %{"val" => [result]} = Dux.Backend.table_to_columns(conn, ref)
        assert result == nil
      end
    end
  end

  describe "Arrow IPC round-trip" do
    property "integer data survives IPC encode → decode", %{conn: conn} do
      check all(values <- list_of(integer(-10_000..10_000), min_length: 1, max_length: 100)) do
        unions = Enum.map_join(values, " UNION ALL ", &"SELECT #{&1}::BIGINT AS val")
        ref = Dux.Backend.query(conn, unions)

        ipc = Dux.Backend.table_to_ipc(conn, ref)
        restored = Dux.Backend.table_from_ipc(conn, ipc)

        original_cols = Dux.Backend.table_to_columns(conn, ref)
        restored_cols = Dux.Backend.table_to_columns(conn, restored)

        assert original_cols == restored_cols
      end
    end

    property "string data survives IPC encode → decode", %{conn: conn} do
      check all(
              values <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 50),
                  min_length: 1,
                  max_length: 50
                )
            ) do
        unions =
          Enum.map_join(values, " UNION ALL ", fn s ->
            escaped = String.replace(s, "'", "''")
            "SELECT '#{escaped}'::VARCHAR AS val"
          end)

        ref = Dux.Backend.query(conn, unions)
        ipc = Dux.Backend.table_to_ipc(conn, ref)
        restored = Dux.Backend.table_from_ipc(conn, ipc)

        assert Dux.Backend.table_to_columns(conn, ref) ==
                 Dux.Backend.table_to_columns(conn, restored)
      end
    end
  end

  describe "dtype mapping" do
    test "all core DuckDB types map to known Dux dtypes", %{conn: conn} do
      ref =
        Dux.Backend.query(conn, """
          SELECT
            1::TINYINT AS ti,
            1::SMALLINT AS si,
            1::INTEGER AS i,
            1::BIGINT AS bi,
            1::UTINYINT AS uti,
            1::USMALLINT AS usi,
            1::UINTEGER AS ui,
            1::UBIGINT AS ubi,
            1.0::FLOAT AS f32,
            1.0::DOUBLE AS f64,
            true AS b,
            'hello'::VARCHAR AS s,
            '2025-01-01'::DATE AS d,
            NULL::BLOB AS blob
        """)

      dtypes = Dux.Backend.table_dtypes(conn, ref)
      dtype_map = Map.new(dtypes)

      assert dtype_map["ti"] == {:s, 8}
      assert dtype_map["si"] == {:s, 16}
      assert dtype_map["i"] == {:s, 32}
      assert dtype_map["bi"] == {:s, 64}
      assert dtype_map["uti"] == {:u, 8}
      assert dtype_map["usi"] == {:u, 16}
      assert dtype_map["ui"] == {:u, 32}
      assert dtype_map["ubi"] == {:u, 64}
      assert dtype_map["f32"] == {:f, 32}
      assert dtype_map["f64"] == {:f, 64}
      assert dtype_map["b"] == :boolean
      assert dtype_map["s"] == :string
      assert dtype_map["d"] == :date
      assert dtype_map["blob"] == :binary
    end
  end
end
