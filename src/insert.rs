use crate::{InsertOpt, open_connection};
use odbc_api::Environment;
use anyhow::{bail, Error};

/// Read the content of a parquet file and insert it into a table.
pub fn insert(odbc_env: &Environment, insert_opt: &InsertOpt) -> Result<(), Error> {
    let InsertOpt {
        input,
        connect_opts,
        table,
    } = insert_opt;

    let odbc_conn = open_connection(odbc_env, connect_opts)?;

    bail!("Not implemented yet")
}