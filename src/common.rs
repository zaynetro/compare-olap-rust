use std::time::Instant;

use anyhow::Result;
use datafusion::prelude::SessionContext;

pub fn exec_sqlite(conn: &rusqlite::Connection, query: &str) -> Result<()> {
    let now = Instant::now();
    let mut stmt = conn.prepare(query)?;

    let column_len = {
        let columns = stmt.column_names();
        print_column_names(&columns);
        columns.len()
    };

    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        for i in 0..column_len {
            let v: rusqlite::types::Value = row.get(i)?;
            print!("| {:<20} ", fmt_sql_value(v));
        }
        println!("|");
    }

    print_divider(column_len);
    println!("SQLite took {}ms", now.elapsed().as_millis());
    println!();
    Ok(())
}

pub fn exec_duck(conn: &duckdb::Connection, query: &str, columns: Vec<&str>) -> Result<()> {
    do_exec_duck("DuckDB", conn, query, columns)
}

pub fn exec_duck_typed(conn: &duckdb::Connection, query: &str, columns: Vec<&str>) -> Result<()> {
    do_exec_duck("DuckDB (Typed)", conn, query, columns)
}

fn do_exec_duck(
    label: &str,
    conn: &duckdb::Connection,
    query: &str,
    columns: Vec<&str>,
) -> Result<()> {
    let now = Instant::now();
    let mut stmt = conn.prepare(query)?;

    let column_len = {
        // This panics
        // let columns = stmt.column_names();
        print_column_names(&columns);
        columns.len()
    };

    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        for i in 0..column_len {
            let v: duckdb::types::Value = row.get(i)?;
            print!("| {:<20} ", fmt_duck_value(v));
        }
        println!("|");
    }

    print_divider(column_len);
    println!("{} took {}ms", label, now.elapsed().as_millis());
    println!();
    Ok(())
}

pub async fn exec_df(ctx: &SessionContext, query: &str) -> Result<()> {
    let now = Instant::now();
    let df = ctx.sql(query).await?;
    df.show().await?;
    println!("DataFusions took {}ms", now.elapsed().as_millis());
    println!();
    Ok(())
}

fn print_divider(column_len: usize) {
    for _ in 0..column_len {
        print!("+{:-<22}", "");
    }
    println!("+");
}

fn print_column_names<I>(names: &[I])
where
    I: std::fmt::Display,
{
    print_divider(names.len());
    for column in names {
        print!("| {:<20} ", column);
    }
    println!("|");
    print_divider(names.len());
}

fn fmt_sql_value(v: rusqlite::types::Value) -> String {
    match v {
        rusqlite::types::Value::Null => "null".into(),
        rusqlite::types::Value::Integer(n) => format!("{n}"),
        rusqlite::types::Value::Real(n) => format!("{n}"),
        rusqlite::types::Value::Text(t) => t,
        rusqlite::types::Value::Blob(b) => format!("Blob(len={})", b.len()),
    }
}

fn fmt_duck_value(v: duckdb::types::Value) -> String {
    match v {
        duckdb::types::Value::Null => format!("null"),
        duckdb::types::Value::Boolean(b) => format!("{b}"),
        duckdb::types::Value::TinyInt(n) => format!("{n}"),
        duckdb::types::Value::SmallInt(n) => format!("{n}"),
        duckdb::types::Value::Int(n) => format!("{n}"),
        duckdb::types::Value::BigInt(n) => format!("{n}"),
        duckdb::types::Value::HugeInt(n) => format!("{n}"),
        duckdb::types::Value::UTinyInt(n) => format!("{n}"),
        duckdb::types::Value::USmallInt(n) => format!("{n}"),
        duckdb::types::Value::UInt(n) => format!("{n}"),
        duckdb::types::Value::UBigInt(n) => format!("{n}"),
        duckdb::types::Value::Float(n) => format!("{n}"),
        duckdb::types::Value::Double(n) => format!("{n}"),
        duckdb::types::Value::Decimal(n) => format!("{n}"),
        duckdb::types::Value::Timestamp(u, t) => format!("{t}{:?}", u),
        duckdb::types::Value::Text(t) => t,
        duckdb::types::Value::Blob(b) => format!("Blob(len={})", b.len()),
        duckdb::types::Value::Date32(d) => format!("{d}"),
        duckdb::types::Value::Time64(u, t) => format!("{t}{:?}", u),
    }
}
