use std::fs::File;

use anyhow::Error;
use log::info;
use odbc_api::Environment;
use parquet::{column::reader::ColumnReader, file::reader::{FileReader, SerializedFileReader}};

use crate::{InsertOpt, open_connection};

/// Read the content of a parquet file and insert it into a table.
pub fn insert(odbc_env: &Environment, insert_opt: &InsertOpt) -> Result<(), Error> {
    let InsertOpt {
        input,
        connect_opts,
        table,
    } = insert_opt;

    let odbc_conn = open_connection(odbc_env, connect_opts)?;

    let file = File::open(&input)?;
    let reader = SerializedFileReader::new(file)?;

    let parquet_metadata = reader.metadata();
    let schema_desc = parquet_metadata.file_metadata().schema_descr();
    let num_columns = schema_desc.num_columns();

    let column_descs : Vec<_> = (0..num_columns).map(|i| schema_desc.column(i)).collect();
    let column_names : Vec<&str> = (0..num_columns).map(|i| column_descs[i].name()).collect();
    let insert_statement = insert_statement_text(&table, &column_names);

    let mut statement = odbc_conn.prepare(&insert_statement)?;

    let num_row_groups = reader.num_row_groups();

    for row_group_index in 0..num_row_groups {
        info!("Insert rowgroup {} of {}.", row_group_index, num_row_groups);
        let row_group_reader = reader.get_row_group(row_group_index)?;

        for column_index in 0..num_columns {
            let column_reader = row_group_reader.get_column_reader(column_index)?;

            match column_reader {
                ColumnReader::BoolColumnReader(_) => {}
                ColumnReader::Int32ColumnReader(_) => {}
                ColumnReader::Int64ColumnReader(_) => {}
                ColumnReader::Int96ColumnReader(_) => {}
                ColumnReader::FloatColumnReader(_) => {}
                ColumnReader::DoubleColumnReader(_) => {}
                ColumnReader::ByteArrayColumnReader(_) => {}
                ColumnReader::FixedLenByteArrayColumnReader(_) => {}
            }
        }

        statement.execute(())?;
    }

    Ok(())
}

fn insert_statement_text(table: &str, column_names: &[&str]) -> String{
    // Generate statement text from table name and headline
    let columns = column_names.join(", ");
    let values = column_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let statement_text = format!("INSERT INTO {} ({}) VALUES ({});", table, columns, values);
    info!("Insert statement Text: {}", statement_text);
    statement_text
}