<div style="display: inline-flex; align-items: center; justify-content: center; margin: 0 auto;">
    <img src="https://webassembly.org/css/webassembly.svg" width="150">
    <img src="https://raw.githubusercontent.com/apache/arrow-datafusion/master/docs/source/_static/images/DataFusion-Logo-Background-White.svg" width="150">
</div>

DataFusion Playground
---------------------

Playground of [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) with [WebAssembly](https://webassembly.org). In the early experimental stage as my side project.

🌱 Live Demo: https://datafusion-contrib.github.io/datafusion-wasm-playground/

## Features

- Upload local CSV/JSON datasets (or point to remote files) and query them via SQL.
- Experimental FLASH FASTQ merge powered by the Rust/WebAssembly port of flash-lib.
- Full functional DataFusion query engine running in the browser.

## Screenshot

![Screenshot](./src/assets/datafusion-playground-demo.png)

## Examples

Create an external table from S3 parquet file:

```sql
CREATE EXTERNAL TABLE test STORED AS PARQUET
LOCATION 's3://path-to-your.parquet';
```

Create an external table from HTTP parquet file:

```sql
CREATE EXTERNAL TABLE lineitem
STORED AS PARQUET
LOCATION 'https://shell.duckdb.org/data/tpch/0_01/parquet/lineitem.parquet';
```

Explain a query:

```sql
EXPLAIN SELECT MIN(airport_fee), MAX(airport_fee) FROM test;
```

