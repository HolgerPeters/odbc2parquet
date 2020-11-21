use std::{convert::TryInto, fs::File};

use anyhow::{bail, Error};
use log::info;
use odbc_api::{
    buffers::{BufferDescription, BufferKind, ColumnarRowSet},
    Environment,
};
use parquet::{
    basic::{LogicalType, Type as PhysicalType},
    column::reader::ColumnReader,
    file::reader::{FileReader, SerializedFileReader},
    schema::types::ColumnDescriptor,
};

use crate::{InsertOpt, open_connection, parquet_buffer::ParquetBuffer};

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

    let column_descs: Vec<_> = (0..num_columns).map(|i| schema_desc.column(i)).collect();
    let column_names: Vec<&str> = column_descs
        .iter()
        .map(|col_desc| col_desc.name())
        .collect();
    let column_buf_desc: Vec<_> = column_descs
        .iter()
        .map(|col_desc| parquet_type_to_odbc_buffer_desc(col_desc))
        .collect::<Result<_, _>>()?;
    let insert_statement = insert_statement_text(&table, &column_names);

    let mut statement = odbc_conn.prepare(&insert_statement)?;

    let num_row_groups = reader.num_row_groups();

    let batch_size = 500; // Todo: Max row group size?
    let mut odbc_buffer = ColumnarRowSet::new(batch_size, column_buf_desc.into_iter());
    let mut pb = ParquetBuffer::new(batch_size.try_into().unwrap());

    for row_group_index in 0..num_row_groups {
        info!("Insert rowgroup {} of {}.", row_group_index, num_row_groups);
        let row_group_reader = reader.get_row_group(row_group_index)?;
        let num_rows = row_group_reader.metadata().num_rows();
        odbc_buffer.set_num_rows(num_rows.try_into().unwrap());

        for column_index in 0..num_columns {
            let column_reader = row_group_reader.get_column_reader(column_index)?;

            match column_reader {
                ColumnReader::BoolColumnReader(_) => {}
                ColumnReader::Int32ColumnReader(_) => {}
                ColumnReader::Int64ColumnReader(reader) => {
                    // reader.read_batch(num_rows, def_levels, rep_levels, values)
                }
                ColumnReader::Int96ColumnReader(_) => {}
                ColumnReader::FloatColumnReader(_) => {}
                ColumnReader::DoubleColumnReader(_) => {}
                ColumnReader::ByteArrayColumnReader(_) => {}
                ColumnReader::FixedLenByteArrayColumnReader(_) => {}
            }
        }

        statement.execute(&odbc_buffer)?;
    }

    Ok(())
}

fn insert_statement_text(table: &str, column_names: &[&str]) -> String {
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

fn parquet_type_to_odbc_buffer_desc(
    col_desc: &ColumnDescriptor,
) -> Result<BufferDescription, Error> {
    // Todo: better error message indicating column name.
    if !col_desc.self_type().is_primitive() {
        bail!("Only primitive parquet types are supported.");
    }
    let nullable = col_desc.self_type().is_optional();

    let lt = col_desc.logical_type();
    let pt = col_desc.physical_type();

    let kind = match (lt, pt) {
        // Todo: We'll rebind the buffer if we encounter larger values in the file.
        (LogicalType::UTF8, PhysicalType::BYTE_ARRAY) => BufferKind::Text { max_str_len : 128 },
        (LogicalType::UTF8, PhysicalType::FIXED_LEN_BYTE_ARRAY) => {
            let max_str_len = dbg!(col_desc.type_length()).try_into().unwrap();
            BufferKind::Text { max_str_len }
        }
        (LogicalType::INT_64, PhysicalType::INT64) => {
            BufferKind::I64
        }
        (LogicalType::UTF8, _) => {
            panic!("Unexpected combination of logical and physical parquet type.")
        }
        (LogicalType::NONE, _)
        | (LogicalType::MAP, _)
        | (LogicalType::MAP_KEY_VALUE, _)
        | (LogicalType::LIST, _)
        | (LogicalType::ENUM, _)
        | (LogicalType::DECIMAL, _)
        | (LogicalType::DATE, _)
        | (LogicalType::TIME_MILLIS, _)
        | (LogicalType::TIME_MICROS, _)
        | (LogicalType::TIMESTAMP_MILLIS, _)
        | (LogicalType::TIMESTAMP_MICROS, _)
        | (LogicalType::UINT_8, _)
        | (LogicalType::UINT_16, _)
        | (LogicalType::UINT_32, _)
        | (LogicalType::UINT_64, _)
        | (LogicalType::INT_8, _)
        | (LogicalType::INT_16, _)
        | (LogicalType::INT_32, _)
        | (LogicalType::INT_64, _)
        | (LogicalType::JSON, _)
        | (LogicalType::BSON, _)
        | (LogicalType::INTERVAL, _) => todo!(),
    };

    Ok(BufferDescription { kind, nullable })
}
