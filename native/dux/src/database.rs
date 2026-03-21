use duckdb::Connection;
use rustler::{Encoder, Env, Error as NifError, Resource, ResourceArc, Term};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::error::DuxError;

/// Global connection ID counter
static CONN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Wraps a DuckDB connection. DuckDB connections are not thread-safe,
/// so we protect them with a Mutex. Each connection has a unique ID
/// for table namespace isolation.
pub struct DuxDbRef {
    pub conn: Mutex<Connection>,
    pub id: u64,
}

#[rustler::resource_impl]
impl Resource for DuxDbRef {}

pub struct DuxDb {
    pub resource: ResourceArc<DuxDbRef>,
}

impl DuxDb {
    pub fn new(conn: Connection) -> Self {
        let id = CONN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            resource: ResourceArc::new(DuxDbRef {
                conn: Mutex::new(conn),
                id,
            }),
        }
    }

    pub fn conn_id(&self) -> u64 {
        self.resource.id
    }
}

impl Encoder for DuxDb {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.resource.encode(env)
    }
}

impl<'a> rustler::Decoder<'a> for DuxDb {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        let resource: ResourceArc<DuxDbRef> = term.decode()?;
        Ok(DuxDb { resource })
    }
}

/// Open an in-memory DuckDB database.
#[rustler::nif(schedule = "DirtyCpu")]
fn db_open() -> Result<DuxDb, NifError> {
    let conn = Connection::open_in_memory().map_err(DuxError::DuckDB)?;
    Ok(DuxDb::new(conn))
}

/// Open a DuckDB database at the given path.
#[rustler::nif(schedule = "DirtyCpu")]
fn db_open_path(path: String) -> Result<DuxDb, NifError> {
    let conn = Connection::open(&path).map_err(DuxError::DuckDB)?;
    Ok(DuxDb::new(conn))
}

/// Execute a SQL statement that returns no results.
#[rustler::nif(schedule = "DirtyCpu")]
fn db_execute(db: DuxDb, sql: String) -> Result<(), NifError> {
    let conn = db
        .resource
        .conn
        .lock()
        .map_err(|e| DuxError::Other(format!("lock error: {e}")))?;
    conn.execute_batch(&sql).map_err(DuxError::DuckDB)?;
    Ok(())
}
