// Re-export duckdb's arrow to avoid version conflicts
pub use duckdb::arrow;

mod database;
mod dataframe;
mod error;
mod types;

pub use database::{DuxDb, DuxDbRef};
pub use dataframe::{DuxTable, DuxTableRef};
pub use error::DuxError;

rustler::init!("Elixir.Dux.Native");
