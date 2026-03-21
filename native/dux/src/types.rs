use duckdb::arrow::datatypes::DataType as ArrowDataType;
use rustler::{Atom, Encoder, Env, Term};

mod atoms {
    rustler::atoms! {
        s,
        u,
        f,
        boolean,
        string,
        date,
        time,
        binary,
        null,
        list,
        r#struct = "struct",
        naive_datetime,
        datetime,
        duration,
        decimal,
        unknown,
        millisecond,
        microsecond,
        nanosecond,
    }
}

/// Convert an Arrow DataType to a Dux dtype term.
pub fn arrow_dtype_to_term<'a>(env: Env<'a>, dt: &ArrowDataType) -> Term<'a> {
    match dt {
        ArrowDataType::Int8 => (atoms::s(), 8i64).encode(env),
        ArrowDataType::Int16 => (atoms::s(), 16i64).encode(env),
        ArrowDataType::Int32 => (atoms::s(), 32i64).encode(env),
        ArrowDataType::Int64 => (atoms::s(), 64i64).encode(env),
        ArrowDataType::UInt8 => (atoms::u(), 8i64).encode(env),
        ArrowDataType::UInt16 => (atoms::u(), 16i64).encode(env),
        ArrowDataType::UInt32 => (atoms::u(), 32i64).encode(env),
        ArrowDataType::UInt64 => (atoms::u(), 64i64).encode(env),
        ArrowDataType::Float32 => (atoms::f(), 32i64).encode(env),
        ArrowDataType::Float64 => (atoms::f(), 64i64).encode(env),
        ArrowDataType::Boolean => atoms::boolean().encode(env),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => atoms::string().encode(env),
        ArrowDataType::Date32 | ArrowDataType::Date64 => atoms::date().encode(env),
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => atoms::time().encode(env),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => atoms::binary().encode(env),
        ArrowDataType::Null => atoms::null().encode(env),
        ArrowDataType::Timestamp(unit, None) => {
            (atoms::naive_datetime(), time_unit_to_atom(unit)).encode(env)
        }
        ArrowDataType::Timestamp(unit, Some(tz)) => {
            (atoms::datetime(), time_unit_to_atom(unit), tz.as_ref()).encode(env)
        }
        ArrowDataType::Duration(unit) => (atoms::duration(), time_unit_to_atom(unit)).encode(env),
        ArrowDataType::Decimal128(precision, scale) => {
            (atoms::decimal(), *precision as i64, *scale as i64).encode(env)
        }
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
            let inner = arrow_dtype_to_term(env, field.data_type());
            (atoms::list(), inner).encode(env)
        }
        ArrowDataType::Struct(fields) => {
            let field_map: Vec<(String, Term<'a>)> = fields
                .iter()
                .map(|f| (f.name().clone(), arrow_dtype_to_term(env, f.data_type())))
                .collect();
            (atoms::r#struct(), field_map).encode(env)
        }
        _ => (atoms::unknown(), format!("{dt:?}")).encode(env),
    }
}

fn time_unit_to_atom(unit: &duckdb::arrow::datatypes::TimeUnit) -> Atom {
    use duckdb::arrow::datatypes::TimeUnit;
    match unit {
        TimeUnit::Millisecond => atoms::millisecond(),
        TimeUnit::Microsecond => atoms::microsecond(),
        TimeUnit::Nanosecond => atoms::nanosecond(),
        TimeUnit::Second => atoms::millisecond(),
    }
}

/// Convert an Arrow DataType to a DuckDB SQL type string.
pub fn arrow_type_to_duckdb_sql(dt: &ArrowDataType) -> String {
    match dt {
        ArrowDataType::Boolean => "BOOLEAN".to_string(),
        ArrowDataType::Int8 => "TINYINT".to_string(),
        ArrowDataType::Int16 => "SMALLINT".to_string(),
        ArrowDataType::Int32 => "INTEGER".to_string(),
        ArrowDataType::Int64 => "BIGINT".to_string(),
        ArrowDataType::UInt8 => "UTINYINT".to_string(),
        ArrowDataType::UInt16 => "USMALLINT".to_string(),
        ArrowDataType::UInt32 => "UINTEGER".to_string(),
        ArrowDataType::UInt64 => "UBIGINT".to_string(),
        ArrowDataType::Float32 => "FLOAT".to_string(),
        ArrowDataType::Float64 => "DOUBLE".to_string(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => "VARCHAR".to_string(),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => "BLOB".to_string(),
        ArrowDataType::Date32 | ArrowDataType::Date64 => "DATE".to_string(),
        ArrowDataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        ArrowDataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".to_string(),
        ArrowDataType::Duration(_) => "INTERVAL".to_string(),
        ArrowDataType::Decimal128(p, s) => format!("DECIMAL({p}, {s})"),
        ArrowDataType::List(field) => {
            format!("{}[]", arrow_type_to_duckdb_sql(field.data_type()))
        }
        _ => "VARCHAR".to_string(), // fallback
    }
}
