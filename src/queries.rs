use std::{env, time::Instant};

use datafusion::prelude::{ParquetReadOptions, SessionContext};
use polars::{
    lazy::dsl::{avg, col, count, lit},
    prelude::{DataType, JoinType, LazyFrame, SortOptions},
};
use tracing_subscriber::EnvFilter;

mod common;

use common::{exec_duck, exec_sqlite};

use crate::common::{exec_df, exec_duck_typed};

#[tokio::main]
async fn main() {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "info,compare-olap-rust=debug");
    }
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let sqlite_conn = rusqlite::Connection::open("./eventsqlite.db").unwrap();
    let duck_conn = duckdb::Connection::open("./eventsduck.db").unwrap();
    let duck_typed_conn = duckdb::Connection::open("./eventsduck-typed.db").unwrap();
    let pdf = LazyFrame::scan_parquet("./events-typed.parquet", Default::default()).unwrap();
    println!("Polar schema: {:?}", pdf.schema());
    let dfctx = SessionContext::new();
    dfctx
        .register_parquet(
            "events",
            "./events-typed.parquet",
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

    println!();
    println!("========================================================================");
    println!("Count by event_type");
    println!("========================================================================");
    println!();

    exec_sqlite(
        &sqlite_conn,
        r#"
SELECT event_type, count(*) as count
  FROM events
 GROUP BY event_type
 ORDER BY count DESC
"#,
    )
    .unwrap();
    exec_duck(
        &duck_conn,
        r#"
SELECT event_type, count(*) as count
  FROM events
 GROUP BY event_type
 ORDER BY count DESC
"#,
        vec!["event_type", "count"],
    )
    .unwrap();
    exec_duck_typed(
        &duck_typed_conn,
        r#"
SELECT event_type, count(*) as count
  FROM events
 GROUP BY event_type
 ORDER BY count DESC
"#,
        vec!["event_type", "count"],
    )
    .unwrap();

    {
        let pdf2 = pdf.clone();
        let now = Instant::now();
        let pres = pdf2
            .groupby([col("event_type")])
            .agg([count().alias("count")])
            .sort(
                "count",
                SortOptions {
                    descending: true,
                    ..Default::default()
                },
            )
            .collect()
            .unwrap();
        println!("{:?}", pres);
        println!("Polars took {}ms", now.elapsed().as_millis());
        println!();
    }

    exec_df(
        &dfctx,
        r#"
SELECT event_type, count(*) as count
  FROM events
 GROUP BY event_type
 ORDER BY count DESC
"#,
    )
    .await
    .unwrap();

    println!();
    println!("========================================================================");
    println!("Average page loads per session");
    println!("========================================================================");
    println!();

    exec_sqlite(
        &sqlite_conn,
        r#"
WITH session_loads AS (
  SELECT session_id, count(*) as count
    FROM events
   WHERE event_type = 'page_load'
   GROUP BY session_id
)
SELECT AVG(count), MIN(count), MAX(count) FROM session_loads
"#,
    )
    .unwrap();
    exec_duck(
        &duck_conn,
        r#"
WITH session_loads AS (
  SELECT session_id, count(*) as count
    FROM events
   WHERE event_type = 'page_load'
   GROUP BY session_id
)
SELECT AVG(count), MIN(count), MAX(count) FROM session_loads
"#,
        vec!["average", "min", "max"],
    )
    .unwrap();
    exec_duck_typed(
        &duck_typed_conn,
        r#"
WITH session_loads AS (
  SELECT session_id, count(*) as count
    FROM events
   WHERE event_type = 'page_load'
   GROUP BY session_id
)
SELECT AVG(count), MIN(count), MAX(count) FROM session_loads
"#,
        vec!["average", "min", "max"],
    )
    .unwrap();

    {
        let pdf2 = pdf.clone();
        let now = Instant::now();
        let pres = pdf2
            // First part
            .filter(col("event_type").eq(lit("page_load")))
            .groupby([col("session_id")])
            .agg([count().alias("count")])
            // Second part
            .select([
                avg("count").alias("average"),
                col("count").min().alias("min"),
                col("count").max().alias("max"),
            ])
            .collect()
            .unwrap();
        println!("{:?}", pres);
        println!("Polars took {}ms", now.elapsed().as_millis());
        println!();
    }

    exec_df(
        &dfctx,
        r#"
WITH session_loads AS (
  SELECT session_id, count(*) as count
    FROM events
   WHERE event_type = 'page_load'
   GROUP BY session_id
)
SELECT AVG(count), MIN(count), MAX(count) FROM session_loads
"#,
    )
    .await
    .unwrap();

    println!();
    println!("=============================================");
    println!("Average feedback score");
    println!("=============================================");
    println!();

    exec_sqlite(
        &sqlite_conn,
        r#"
SELECT AVG(payload->>'$.fields[0].value') AS average
  FROM events
 WHERE
     event_type = 'form_submit'
     AND payload->>'$.form_type' = 'feedback'
"#,
    )
    .unwrap();
    exec_duck(
        &duck_conn,
        r#"
WITH form_submissions AS (
    SELECT payload->'$.fields' AS fields, payload->>'$.form_type' as form_type
      FROM events
     WHERE event_type = 'form_submit'
)
SELECT AVG(TRY_CAST(fields->0->>'value' AS INTEGER)) AS average
  FROM form_submissions
 WHERE form_type = 'feedback'
"#,
        vec!["average score"],
    )
    .unwrap();
    exec_duck_typed(
        &duck_typed_conn,
        r#"
WITH form_submissions AS (
    SELECT payload.fields AS fields, payload.form_type as form_type
      FROM events
     WHERE event_type = 'form_submit'
)
SELECT AVG(TRY_CAST(fields[1].value AS INTEGER)) AS average
  FROM form_submissions
 WHERE form_type = 'feedback'
"#,
        vec!["average score"],
    )
    .unwrap();

    {
        let pdf2 = pdf.clone();
        let now = Instant::now();
        let pres = pdf2
            .filter(
                col("event_type").eq(lit("form_submit")).and(
                    col("payload")
                        .struct_()
                        .field_by_name("form_type")
                        .eq(lit("feedback")),
                ),
            )
            .select([
                // '$.fields[0].value
                col("payload")
                    .struct_()
                    .field_by_name("fields")
                    .arr()
                    .first()
                    .struct_()
                    .field_by_name("value")
                    .cast(DataType::Int32)
                    .alias("score"),
            ])
            .select([avg("score")])
            .collect()
            .unwrap();
        println!("{:?}", pres);
        println!("Polars took {}ms", now.elapsed().as_millis());
        println!();
    }

    println!();
    println!("=============================================");
    println!("Top pages");
    println!("=============================================");
    println!();

    exec_sqlite(
        &sqlite_conn,
        r#"
SELECT payload->>'$.path' AS path, COUNT(*) AS count
  FROM events
 WHERE
     event_type = 'page_load'
 GROUP BY path
 ORDER BY count DESC
 LIMIT 5
"#,
    )
    .unwrap();
    exec_duck(
        &duck_conn,
        r#"
SELECT payload->>'$.path' AS path, COUNT(*) AS count
  FROM events
 WHERE
     event_type = 'page_load'
 GROUP BY path
 ORDER BY count DESC
 LIMIT 5
"#,
        vec!["path", "count"],
    )
    .unwrap();
    exec_duck_typed(
        &duck_typed_conn,
        r#"
SELECT payload.path AS path, COUNT(*) AS count
  FROM events
 WHERE
     event_type = 'page_load'
 GROUP BY path
 ORDER BY count DESC
 LIMIT 5
"#,
        vec!["path", "count"],
    )
    .unwrap();

    {
        let pdf2 = pdf.clone();
        let now = Instant::now();
        let pres = pdf2
            .filter(col("event_type").eq(lit("page_load")))
            .select([col("payload").struct_().field_by_name("path").alias("path")])
            .groupby([col("path")])
            .agg([count().alias("count")])
            .sort(
                "count",
                SortOptions {
                    descending: true,
                    ..Default::default()
                },
            )
            .limit(5)
            .collect()
            .unwrap();
        println!("{:?}", pres);
        println!("Polars took {}ms", now.elapsed().as_millis());
        println!();
    }

    exec_df(
        &dfctx,
        r#"
SELECT payload['path'] AS path, COUNT(*) AS count
  FROM events
 WHERE
     event_type = 'page_load'
 GROUP BY path
 ORDER BY count DESC
 LIMIT 5
"#,
    )
    .await
    .unwrap();

    println!();
    println!("=============================================");
    println!("Page loads per day");
    println!("=============================================");
    println!();

    exec_sqlite(
        &sqlite_conn,
        r#"
SELECT date(timestamp) AS date, COUNT(*) AS count
  FROM events
 WHERE
     event_type = 'page_load'
 GROUP BY date
 ORDER BY date
 LIMIT 10
"#,
    )
    .unwrap();
    exec_duck(
        &duck_conn,
        r#"
WITH page_loads AS (
  SELECT strftime(timestamp, '%Y-%m-%d') AS date
    FROM events
   WHERE event_type = 'page_load'
)
SELECT date, COUNT(*) AS count
  FROM page_loads
 GROUP BY date
 ORDER BY date
 LIMIT 10
"#,
        vec!["date", "count"],
    )
    .unwrap();
    exec_duck_typed(
        &duck_typed_conn,
        r#"
WITH page_loads AS (
  SELECT strftime(timestamp, '%Y-%m-%d') AS date
    FROM events
   WHERE event_type = 'page_load'
)
SELECT date, COUNT(*) AS count
  FROM page_loads
 GROUP BY date
 ORDER BY date
 LIMIT 10
"#,
        vec!["date", "count"],
    )
    .unwrap();

    {
        let pdf2 = pdf.clone();
        let now = Instant::now();
        let pres = pdf2
            .filter(col("event_type").eq(lit("page_load")))
            .select([col("timestamp").dt().date().alias("date")])
            .groupby([col("date")])
            .agg([count().alias("count")])
            .sort("date", Default::default())
            .limit(10)
            .collect()
            .unwrap();
        println!("{:?}", pres);
        println!("Polars took {}ms", now.elapsed().as_millis());
        println!();
    }

    exec_df(
        &dfctx,
        r#"
SELECT date_trunc('day', timestamp) AS date, COUNT(*) AS count
  FROM events
 WHERE
     event_type = 'page_load'
 GROUP BY date
 ORDER BY date
 LIMIT 10
"#,
    )
    .await
    .unwrap();

    println!();
    println!("=============================================");
    println!("Form submissions");
    println!("Unique: count submission once per session id");
    println!("Total: count all submission");
    println!("=============================================");
    println!();

    exec_sqlite(
        &sqlite_conn,
        r#"
WITH submissions AS (
  SELECT payload->>'$.form_type' as form_type, session_id, count(*) as count
   FROM events
   WHERE event_type = 'form_submit'
   GROUP BY form_type, session_id
)
SELECT form_type, COUNT(count) as unique_count, SUM(count) as total
  FROM submissions
 GROUP BY form_type
 ORDER BY form_type
"#,
    )
    .unwrap();

    exec_duck(
        &duck_conn,
        r#"
WITH submissions AS (
  SELECT payload->>'$.form_type' as form_type, session_id, count(*) as count
   FROM events
   WHERE event_type = 'form_submit'
   GROUP BY form_type, session_id
)
SELECT form_type, COUNT(count) as unique, SUM(count) as total
  FROM submissions
 GROUP BY form_type
 ORDER BY form_type
"#,
        vec!["form_type", "unique", "total"],
    )
    .unwrap();
    exec_duck_typed(
        &duck_typed_conn,
        r#"
WITH submissions AS (
  SELECT payload.form_type as form_type, session_id, count(*) as count
   FROM events
   WHERE event_type = 'form_submit'
   GROUP BY form_type, session_id
)
SELECT form_type, COUNT(count) as unique, SUM(count) as total
  FROM submissions
 GROUP BY form_type
 ORDER BY form_type
"#,
        vec!["form_type", "unique", "total"],
    )
    .unwrap();

    {
        let pdf2 = pdf.clone();
        let now = Instant::now();
        let pres = pdf2
            // First part
            .filter(col("event_type").eq(lit("form_submit")))
            .select([
                col("payload")
                    .struct_()
                    .field_by_name("form_type")
                    .alias("form_type"),
                col("session_id"),
            ])
            .groupby([col("form_type"), col("session_id")])
            .agg([count().alias("count")])
            // Second part
            .groupby([col("form_type")])
            .agg([count().alias("unique"), col("count").sum().alias("total")])
            .sort("form_type", Default::default())
            .collect()
            .unwrap();
        println!("{:?}", pres);
        println!("Polars took {}ms", now.elapsed().as_millis());
        println!();
    }

    exec_df(
        &dfctx,
        r#"
WITH submissions AS (
  SELECT payload['form_type'] as form_type, session_id, count(*) as count
   FROM events
   WHERE event_type = 'form_submit'
   GROUP BY form_type, session_id
)
SELECT form_type, COUNT(count) as unique, SUM(count) as total
  FROM submissions
 GROUP BY form_type
 ORDER BY form_type
"#,
    )
    .await
    .unwrap();

    println!();
    println!("=============================================");
    println!("Form submissions by page");
    println!("=============================================");
    println!();

    exec_sqlite(
        &sqlite_conn,
        r#"
SELECT e1.payload->>'$.form_type' as form_type, e2.payload->>'$.path' as path, count(*) as count
 FROM events e1
 LEFT JOIN events as e2 ON e1.page_id = e2.page_id
 WHERE e1.event_type = 'form_submit'
       AND e2.event_type = 'page_load'
       AND path = '/after'
 GROUP BY form_type, e2.payload->>'$.path'
 ORDER BY path
"#,
    )
    .unwrap();

    exec_duck(
        &duck_conn,
        r#"
SELECT e1.payload->>'$.form_type' as form_type, e2.payload->>'$.path' as path, count(*) as count
 FROM events e1
 LEFT JOIN events as e2 ON e1.page_id = e2.page_id
 WHERE e1.event_type = 'form_submit'
       AND e2.event_type = 'page_load'
       AND path = '/after'
 GROUP BY form_type, path
 ORDER BY form_type
"#,
        vec!["form_type", "path", "count"],
    )
    .unwrap();

    exec_duck_typed(
        &duck_typed_conn,
        r#"
SELECT e1.payload.form_type as form_type, e2.payload.path as path, count(*) as count
 FROM events e1
 LEFT JOIN events as e2 ON e1.page_id = e2.page_id
 WHERE e1.event_type = 'form_submit'
       AND e2.event_type = 'page_load'
       AND path = '/after'
 GROUP BY form_type, path
 ORDER BY form_type
"#,
        vec!["form_type", "path", "count"],
    )
    .unwrap();

    {
        let pdf2 = pdf.clone();
        let pdf3 = pdf.clone();
        let now = Instant::now();

        let forms_pdf = pdf2
            .filter(col("event_type").eq(lit("form_submit")))
            .select([
                col("payload")
                    .struct_()
                    .field_by_name("form_type")
                    .alias("form_type"),
                col("page_id"),
            ]);

        let paths_pdf = pdf3 //
            .filter(col("event_type").eq(lit("page_load"))) //
            .select([
                col("payload").struct_().field_by_name("path").alias("path"),
                col("page_id"),
            ]);

        let pres = forms_pdf
            .join(
                paths_pdf,
                [col("page_id")],
                [col("page_id")],
                JoinType::Left,
            )
            .filter(col("path").eq(lit("/after")))
            .groupby([col("form_type"), col("path")])
            .agg([count()])
            .sort("form_type", Default::default())
            .collect()
            .unwrap();
        println!("{:?}", pres);
        println!("Polars took {}ms", now.elapsed().as_millis());
        println!();
    }

    exec_df(
        &dfctx,
        r#"
SELECT e1.payload['form_type'] as form_type, e2.payload['path'] as path, count(*) as count
 FROM events e1
 LEFT JOIN events as e2 ON e1.page_id = e2.page_id
 WHERE e1.event_type = 'form_submit'
       AND e2.event_type = 'page_load'
       AND e2.payload['path'] = '/after'
 GROUP BY form_type, path
 ORDER BY form_type
"#,
    )
    .await
    .unwrap();

    tracing::info!("Starting to execute queries");
    tracing::info!("Done.");
}
