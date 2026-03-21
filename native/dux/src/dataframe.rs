use duckdb::arrow::array::{Array, RecordBatch};
use duckdb::arrow::compute;
use duckdb::arrow::datatypes::SchemaRef;
use duckdb::vtab::arrow::ArrowVTab;
use duckdb::vtab::arrow_recordbatch_to_query_params;
use rustler::{Encoder, Env, Error as NifError, Resource, ResourceArc, Term};
use std::sync::Mutex;

use crate::database::{DuxDb, DuxDbRef};
use crate::error::DuxError;
use crate::types;

mod atoms {
    rustler::atoms! {
        ok,
        nil,
        nan,
        infinity,
        neg_infinity,
    }
}

/// Cached temp table: (table_name, connection_id, connection_ref_for_drop)
type CachedTable = (String, u64, ResourceArc<DuxDbRef>);

/// Holds a materialized result as Arrow RecordBatches.
/// Includes a connection-scoped temp table cache with auto-cleanup on Drop.
///
/// This is the ResourceArc that %Dux{source: {:table, ref}} holds.
/// When the last Elixir reference is GC'd, Drop fires and the temp table
/// is cleaned up automatically.
pub struct DuxTableRef {
    pub batches: Vec<RecordBatch>,
    pub schema: SchemaRef,
    cached_table: Mutex<Option<CachedTable>>,
}

impl std::panic::RefUnwindSafe for DuxTableRef {}

#[rustler::resource_impl]
impl Resource for DuxTableRef {}

impl Drop for DuxTableRef {
    fn drop(&mut self) {
        if let Ok(guard) = self.cached_table.lock() {
            if let Some((ref table_name, _, ref db_ref)) = *guard {
                if let Ok(conn) = db_ref.conn.lock() {
                    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"));
                }
            }
        }
    }
}

pub struct DuxTable {
    pub resource: ResourceArc<DuxTableRef>,
}

impl DuxTable {
    pub fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self {
            resource: ResourceArc::new(DuxTableRef {
                batches,
                schema,
                cached_table: Mutex::new(None),
            }),
        }
    }
}

impl Encoder for DuxTable {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.resource.encode(env)
    }
}

impl<'a> rustler::Decoder<'a> for DuxTable {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        let resource: ResourceArc<DuxTableRef> = term.decode()?;
        Ok(DuxTable { resource })
    }
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

/// Execute a SQL query and return the results as a DuxTable (Arrow RecordBatches).
#[rustler::nif(schedule = "DirtyCpu")]
fn df_query(db: DuxDb, sql: String) -> Result<DuxTable, NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuxError::Other(format!("lock error: {e}")))?;

    let mut stmt = conn.prepare(&sql).map_err(DuxError::DuckDB)?;
    let arrow_result = stmt.query_arrow([]).map_err(DuxError::DuckDB)?;
    let schema = arrow_result.get_schema();
    let batches: Vec<RecordBatch> = arrow_result.collect();

    Ok(DuxTable::new(batches, schema))
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// Get column names from a DuxTable.
#[rustler::nif]
fn table_names(table: DuxTable) -> Vec<String> {
    table
        .resource
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Get column dtypes from a DuxTable.
#[rustler::nif]
fn table_dtypes(env: Env, table: DuxTable) -> Vec<(String, Term)> {
    table
        .resource
        .schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), types::arrow_dtype_to_term(env, f.data_type())))
        .collect()
}

/// Get the number of rows in a DuxTable.
#[rustler::nif]
fn table_n_rows(table: DuxTable) -> u64 {
    table
        .resource
        .batches
        .iter()
        .map(|b| b.num_rows() as u64)
        .sum()
}

// ---------------------------------------------------------------------------
// Temp table management
// ---------------------------------------------------------------------------

/// Ensure a DuxTable has a registered temp table in DuckDB.
/// Returns the table name. Cached per connection — instant on subsequent calls.
/// The table is auto-dropped when the DuxTable NIF resource is GC'd.
#[rustler::nif(schedule = "DirtyCpu")]
fn table_ensure(db: DuxDb, table: DuxTable) -> Result<String, NifError> {
    let conn_id = db.conn_id();

    // Check cache — only reuse if same connection
    if let Ok(guard) = table.resource.cached_table.lock() {
        if let Some((ref name, ref cached_conn_id, _)) = *guard {
            if *cached_conn_id == conn_id {
                return Ok(name.clone());
            }
        }
    }

    // Generate UUID-namespaced table name
    let table_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let table_name = format!("__dux_{conn_id}_{table_id}");

    register_table_impl(&db, &table, &table_name)?;

    // Cache it
    if let Ok(mut guard) = table.resource.cached_table.lock() {
        *guard = Some((table_name.clone(), conn_id, db.resource.clone()));
    }

    Ok(table_name)
}

/// Register a DuxTable as a temporary table in DuckDB.
/// Uses Arrow vtab zero-copy for small batches, falls back to Appender for larger ones.
fn register_table_impl(db: &DuxDb, table: &DuxTable, table_name: &str) -> Result<(), NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuxError::Other(format!("lock error: {e}")))?;

    let schema = &table.resource.schema;

    // Drop if exists
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {table_name}"))
        .map_err(DuxError::DuckDB)?;

    if table.resource.batches.is_empty() {
        // Empty — create table from schema only
        let col_defs: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                format!(
                    "\"{}\" {}",
                    f.name(),
                    types::arrow_type_to_duckdb_sql(f.data_type())
                )
            })
            .collect();
        conn.execute_batch(&format!(
            "CREATE TEMPORARY TABLE {table_name} ({})",
            col_defs.join(", ")
        ))
        .map_err(DuxError::DuckDB)?;
        return Ok(());
    }

    let total_rows: usize = table.resource.batches.iter().map(|b| b.num_rows()).sum();

    if total_rows <= 2048 {
        // Arrow zero-copy path (ArrowVTab)
        let _ = conn.register_table_function::<ArrowVTab>("arrow");

        let all_arrays: Result<Vec<_>, _> = (0..schema.fields().len())
            .map(|col_idx| {
                let arrays: Vec<&dyn Array> = table
                    .resource
                    .batches
                    .iter()
                    .map(|b| b.column(col_idx).as_ref())
                    .collect();
                compute::concat(&arrays)
            })
            .collect();

        let all_arrays =
            all_arrays.map_err(|e| DuxError::Other(format!("concat arrays: {e}")))?;

        let combined = RecordBatch::try_new(schema.clone(), all_arrays)
            .map_err(|e| DuxError::Other(format!("concat: {e}")))?;

        let params = arrow_recordbatch_to_query_params(combined);
        let sql = format!("CREATE TEMPORARY TABLE {table_name} AS SELECT * FROM arrow($1, $2)");
        conn.prepare(&sql)
            .map_err(DuxError::DuckDB)?
            .execute(params)
            .map_err(DuxError::DuckDB)?;
    } else {
        // Appender path for larger datasets
        let col_defs: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                format!(
                    "\"{}\" {}",
                    f.name(),
                    types::arrow_type_to_duckdb_sql(f.data_type())
                )
            })
            .collect();

        conn.execute_batch(&format!(
            "CREATE TEMPORARY TABLE {table_name} ({})",
            col_defs.join(", ")
        ))
        .map_err(DuxError::DuckDB)?;

        let mut appender = conn
            .appender(table_name)
            .map_err(DuxError::DuckDB)?;

        for batch in &table.resource.batches {
            appender
                .append_record_batch(batch.clone())
                .map_err(DuxError::DuckDB)?;
        }
        appender.flush().map_err(DuxError::DuckDB)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Data extraction — materializing to Elixir terms
// ---------------------------------------------------------------------------

/// Convert a DuxTable to a map of column_name => [values].
#[rustler::nif]
fn table_to_columns<'a>(env: Env<'a>, table: DuxTable) -> Result<Term<'a>, NifError> {
    let schema = &table.resource.schema;
    let mut entries: Vec<(Term<'a>, Term<'a>)> = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let arrays: Vec<&dyn Array> = table
            .resource
            .batches
            .iter()
            .map(|b| b.column(col_idx).as_ref())
            .collect();

        let concatenated = if arrays.is_empty() {
            duckdb::arrow::array::new_empty_array(field.data_type())
        } else {
            compute::concat(&arrays).map_err(DuxError::Arrow)?
        };

        let list = array_to_list(env, concatenated.as_ref());
        entries.push((field.name().as_str().encode(env), list));
    }

    Term::map_from_pairs(env, &entries)
        .map_err(|_| DuxError::Other("failed to create map".to_string()).into())
}

/// Convert a DuxTable to a list of rows (list of maps).
#[rustler::nif]
fn table_to_rows<'a>(env: Env<'a>, table: DuxTable) -> Result<Term<'a>, NifError> {
    let schema = &table.resource.schema;
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut rows: Vec<Term<'a>> = Vec::new();

    for batch in &table.resource.batches {
        for row_idx in 0..batch.num_rows() {
            let mut map_entries: Vec<(Term<'a>, Term<'a>)> = Vec::new();
            for (col_idx, name) in names.iter().enumerate() {
                let col = batch.column(col_idx);
                let value = array_value_to_term(env, col.as_ref(), row_idx);
                map_entries.push((name.encode(env), value));
            }
            let map = Term::map_from_pairs(env, &map_entries)
                .map_err(|_| DuxError::Other("failed to create map".to_string()))?;
            rows.push(map);
        }
    }

    Ok(rows.encode(env))
}

// ---------------------------------------------------------------------------
// Arrow IPC serialization (for distribution)
// ---------------------------------------------------------------------------

/// Serialize a DuxTable to Arrow IPC streaming format.
#[rustler::nif(schedule = "DirtyCpu")]
fn table_to_ipc<'a>(env: Env<'a>, table: DuxTable) -> Result<rustler::Binary<'a>, NifError> {
    use duckdb::arrow::ipc::writer::StreamWriter;

    let schema = &table.resource.schema;
    let mut buf = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| DuxError::Other(format!("IPC writer: {e}")))?;

        for batch in &table.resource.batches {
            writer
                .write(batch)
                .map_err(|e| DuxError::Other(format!("IPC write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| DuxError::Other(format!("IPC finish: {e}")))?;
    }

    let mut binary = rustler::OwnedBinary::new(buf.len())
        .ok_or_else(|| DuxError::Other("alloc failed".to_string()))?;
    binary.as_mut_slice().copy_from_slice(&buf);
    Ok(binary.release(env))
}

/// Deserialize a DuxTable from Arrow IPC streaming format.
#[rustler::nif(schedule = "DirtyCpu")]
fn table_from_ipc(binary: rustler::Binary) -> Result<DuxTable, NifError> {
    use duckdb::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(binary.as_slice());
    let reader =
        StreamReader::try_new(cursor, None).map_err(|e| DuxError::Other(format!("IPC read: {e}")))?;

    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .filter_map(|r| r.ok())
        .collect();

    Ok(DuxTable::new(batches, schema))
}

// ---------------------------------------------------------------------------
// Value conversion helpers
// ---------------------------------------------------------------------------

/// Convert an Arrow array to an Elixir list term.
fn array_to_list<'a>(env: Env<'a>, array: &dyn Array) -> Term<'a> {
    let values: Vec<Term<'a>> = (0..array.len())
        .map(|i| array_value_to_term(env, array, i))
        .collect();
    values.encode(env)
}

/// Convert a single Arrow value to an Elixir term.
pub fn array_value_to_term<'a>(env: Env<'a>, array: &dyn Array, idx: usize) -> Term<'a> {
    use duckdb::arrow::array::*;
    use duckdb::arrow::datatypes::DataType;

    if array.is_null(idx) {
        return atoms::nil().encode(env);
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = arr.value(idx);
            if v.is_nan() {
                atoms::nan().encode(env)
            } else if v.is_infinite() && v > 0.0 {
                atoms::infinity().encode(env)
            } else if v.is_infinite() {
                atoms::neg_infinity().encode(env)
            } else {
                v.encode(env)
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(idx);
            if v.is_nan() {
                atoms::nan().encode(env)
            } else if v.is_infinite() && v > 0.0 {
                atoms::infinity().encode(env)
            } else if v.is_infinite() {
                atoms::neg_infinity().encode(env)
            } else {
                v.encode(env)
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            arr.value(idx).encode(env)
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = arr.value(idx);
            let mut binary = rustler::OwnedBinary::new(bytes.len()).unwrap();
            binary.as_mut_slice().copy_from_slice(bytes);
            binary.release(env).encode(env)
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(idx);
            // Return as {date, {year, month, day}} or just the days since epoch
            // For now return days — Elixir side will convert
            days.encode(env)
        }
        DataType::Timestamp(_, _) => {
            let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>();
            if let Some(arr) = arr {
                arr.value(idx).encode(env)
            } else {
                // Try nanosecond
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                arr.value(idx).encode(env)
            }
        }
        DataType::List(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let inner = arr.value(idx);
            array_to_list(env, inner.as_ref()).encode(env)
        }
        DataType::Struct(_) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let fields = arr.fields();
            let mut map_entries: Vec<(Term<'a>, Term<'a>)> = Vec::new();
            for (i, field) in fields.iter().enumerate() {
                let col = arr.column(i);
                let value = array_value_to_term(env, col.as_ref(), idx);
                map_entries.push((field.name().as_str().encode(env), value));
            }
            Term::map_from_pairs(env, &map_entries)
                .unwrap_or_else(|_| atoms::nil().encode(env))
        }
        _ => {
            // Fallback: encode as string representation
            format!("{:?}", array).encode(env)
        }
    }
}
