use rustler::Error as NifError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DuxError {
    #[error("DuckDB error: {0}")]
    DuckDB(#[from] duckdb::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] duckdb::arrow::error::ArrowError),

    #[error("{0}")]
    Other(String),
}

impl From<DuxError> for NifError {
    fn from(err: DuxError) -> NifError {
        NifError::Term(Box::new(format!("{err}")))
    }
}
