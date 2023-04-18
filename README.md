# Compare different embedded OLAPs

> **NOTE!** This is a not a benchmark. This is me testing different analysis solitions for a project I am developing.


* [SQLite](https://sqlite.org/)
* [DuckDB](https://duckdb.org/)
* [Polars](https://www.pola.rs/)
* [Apache DataFusion](https://arrow.apache.org/datafusion/)


## Preparation steps

### 1. Generate events data

Insert random events into SQLite, DuckDB and DuckDB with typed schema. Takes a while.

```sh
nix-shell -p openssl pkg-config libiconv darwin.apple_sdk.frameworks.IOKit
cargo run --release --bin gen_data
```

### 2. Export DuckDB tables to Parquet, CSV and JSON

```
duckdb eventsduck.db
COPY events TO 'events.parquet' (FORMAT PARQUET);
COPY events TO 'events.csv' (HEADER, DELIMITER ',');
COPY events TO 'events.json' (HEADER, DELIMITER ',');
```

```
duckdb eventsduck-typed.db
COPY events TO 'events-typed.parquet' (FORMAT PARQUET);
```

Resulting file sizes with events count of 2'274'231:

```
593M eventsqlite.db      <-- SQLite
190M eventsduck.db       <-- DuckDB with JSON payload
177M eventsduck-typed.db <-- DuckDB with typed payload

555M events.csv
701M events.json
177M events.parquet
171M events-typed.parquet
```


## Queries

```
cargo run --release --bin queries
```


### Manual queries

Prepare SQLite to show column names and query timings:

```
sqlite> .timer on
sqlite> .mode table
```

Prepare DuckDB to show query timings:

```
D .timer on
```

Now you can execute queries in the repl.


## Results

Queries were run on Macbook Air M2.

### 2M events

My data generator inserted *2'274'231* event rows. Let's see file sizes:

| File                      | Size |
|:--------------------------|:-----|
| SQLite                    | 593M |
| DuckDB *(JSON payload)*   | 190M |
| DuckDB *(Typed payload)*  | 177M |
| Parquet *(Typed payload)* | 171M |
| CSV                       | 555M |
| JSON                      | 701M |

Now to the queries:

| Query                          | SQLite | DuckDB[^duckdb] | Polars | DataFusion             |
|:-------------------------------|:-------|:----------------|:-------|:-----------------------|
| Count by event_type            | 700ms  | 10ms            | 25ms   | 80ms                   |
| Average page loads per session | 255ms  | 10ms            | 45ms   | 115ms                  |
| Average feedback score         | 255ms  | 35ms            | 160ms  | — [^datafusion-nested] |
| Top pages                      | 370ms  | 30ms            | 190ms  | 460ms                  |
| Page loads per day             | 235ms  | 5ms             | 20ms   | 55ms                   |
| Form submissions               | 485ms  | 40ms            | 225ms  | 550ms                  |
| Form submissions by page       | 535ms  | 60ms            | 380ms  | 535ms                  |

### 22M events

My data generator inserted *22'754'423* event rows. Let's see file sizes:

| File                      | Size |
|:--------------------------|:-----|
| SQLite                    | 5.8G |
| DuckDB *(JSON payload)*   | 1.7G |
| DuckDB *(Typed payload)*  | 1.7G |
| Parquet *(Typed payload)* | 1.7G |

Now to the queries:

| Query                          | SQLite  | DuckDB[^duckdb] *(Typed)*[^duckdb-typed] | Polars | DataFusion             |
|:-------------------------------|:--------|:-----------------------------------------|:-------|:-----------------------|
| Count by event_type            | 10350ms | 70ms                                     | 200ms  | 775ms                  |
| Average page loads per session | 5180ms  | 95ms                                     | 470ms  | 1170ms                 |
| Average feedback score         | 4845ms  | 500ms                                    | 2185ms | — [^datafusion-nested] |
| Top pages                      | 7820ms  | 455ms *(350ms)*                          | 2070ms | 4655ms                 |
| Page loads per day             | 4890ms  | 70ms                                     | 165ms  | 520ms                  |
| Form submissions               | 8020ms  | 345ms *(355ms)*                          | 2972ms | 5830ms                 |
| Form submissions by page       | 12340ms | 620ms *(650ms)*                          | 6980ms | 9380ms                 |

[^duckdb]: DuckDB results with payload as JSON string.

[^duckdb-typed]: In parenthesis you can see DuckDB results with typed payload. 

[^datafusion-nested]: DataFusion doesn't fully support nested structs: <https://github.com/apache/arrow-datafusion/issues/2179>


## Notes

* DuckDB uses 1-based indexes in lists :/
* Polars has extra performance features/tweaks that I haven't used. 
* Polars supports SQL queries as well. I just wanted to play around with DataFrame API in Rust.
