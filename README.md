# FastFiRSt

FastFiRSt ports two DNA sequencing utilities to Rust so they can handle large
datasets more efficiently. FLASH merges paired-end reads into longer fragments,
while PERF identifies microsatellites in DNA sequences, both critical steps in
genome assembly workflows.

Genomics workloads routinely process billions of bases, so every percentage of
performance improvement shortens total runtimes, cuts infrastructure costs, and
reduces the carbon impact of large compute clusters. By consolidating the tools
into highly optimized Rust binaries and streamlining their data pipelines, the
project aims to deliver faster science with a lighter environmental footprint.

- FLASH (C): [https://github.com/Jerrythafast/FLASH-lowercase-overhang](https://github.com/Jerrythafast/FLASH-lowercase-overhang). It merges paired-end reads.
- PERF (Python): [https://github.com/rkmlab/perf](https://github.com/rkmlab/perf). It detects microsatellites.

Rust combines C-like performance with memory safety, fearless concurrency, and
rich tooling, making it ideal for rewriting high-throughput bioinformatics
software that previously relied on native extensions or manual memory
management. The resulting binaries are portable and efficient across CLI, data
processing backends, and WebAssembly targets.

FastFiRSt also adopts the Apache Arrow ecosystem to keep data in a vectorized,
columnar format from ingestion to analytics. Arrow's in-memory layout powers
[DataFusion](https://arrow.apache.org/datafusion/), a Rust-native SQL query
engine embedded in this workspace, and [Ballista](https://github.com/apache/arrow-ballista),
its distributed execution layer. Together they enable the project to scale from
local experiments to cluster-sized runs with minimal code changes, while reusing
the same kernels for both batch analytics and interactive workflows.

The repository also reimagines the [Hadoop-based BigFiRSt data
pipeline](https://github.com/JinxiangChenHome/BigFiRSt/tree/master) that
orchestrates both tools. The resulting improvements of the BigFiRSt data
pipeline are documented in [BigFiRSt: A Software Program Using Big Data
Technique for Mining Simple Sequence Repeats From Large-Scale Sequencing
Data](https://pmc.ncbi.nlm.nih.gov/articles/PMC8805145/pdf/fdata-04-727216.pdf).

Project goals include:

- Porting both tools to [Rust](https://rust-lang.org/) for improved performance
  (see the [related paper](https://arxiv.org/html/2410.05460v1#S3.T3) for
  details).
- Applying [Apache Arrow
  Ballista](https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf)
  optimizations to the end-to-end pipeline.
- Exposing the tools through WebAssembly so they remain easy to use without
  dedicated infrastructure.

## Workspace structure

- `flash-lib`: Core library crate exposing the merge algorithm.
- `flash-cli`: Thin CLI wrapper that matches the original FLASH command-line
  flags and writes the three FASTQ outputs.
- `flash-df`: Provides scaffolding for running the merge pipeline through
  DataFusion/Ballista (feature-gated stubs for now).
- `flash-wasm`: Minimal WebAssembly interface exposing the FLASH merge logic for
  the playground UI.
- `wasm-playground`: Vite/Mantine web playground that can execute SQL queries
  against DataFusion and run FLASH locally in the browser.

## Installing the CLI

The `flash-cli` binary is published on
[crates.io](https://crates.io/crates/flash-cli). You can install it with:

```bash
cargo install flash-cli
```

## Requirements

- Rust toolchain (cargo 1.89+)

## Build

```bash
cargo build --release --workspace
```

## Usage

```bash
cargo run --release --bin flash-cli -- READ1.fq READ2.fq \
  --output-dir output_dir [--output-prefix out]
```

Outputs are written to `<prefix>.extendedFrags.fastq`,
`<prefix>.notCombined_1.fastq`, and `<prefix>.notCombined_2.fastq` in the given
directory. Optional parameters default to the FLASH values; run
`cargo run --bin flash-cli -- --help` for the full list.

## Library usage

The `flash-lib` crate is also published on
[crates.io](https://crates.io/crates/flash-lib). You can add it to your project
with:

```bash
cargo add flash-lib
```

And use it:

```rust
use flash_lib::{merge_fastq_files, CombineParams};

let params = CombineParams::default();
merge_fastq_files("input1.fq", "input2.fq", "./out", "out", &params)?;
```

## DataFusion/Ballista prototype

The `flash-df` crate exposes a `FlashDistributedJob` wrapper that executes the
merge locally (re-using `flash-lib`) and provides feature-gated hooks for wiring
the workflow into a `datafusion::SessionContext`. Enable the relevant feature
flag when building:

```bash
cargo build -p flash-df --features datafusion
```

When the `datafusion` feature is enabled, `FlashDistributedJob` can register a
`FastqTableProvider` that exposes paired FASTQ records as a tabular relation:

```rust
use flash_df::FlashDistributedJob;
use flash_lib::{CombineParams, merge_fastq_files};

let job = FlashDistributedJob::new(config, CombineParams::default());
let ctx = job.session_context().await?;
job.register_fastq_sources(&ctx).await?; // registers `flash_pairs` table
let plan = job.build_logical_plan(&ctx).await?; // logical plan with combined/not-combined annotations

// Or execute the full pipeline via DataFusion and write the three FASTQ outputs
job.execute_datafusion().await?;
```

For a quick preview, you can run the bundled examples against the sample FASTQ
files checked into the workspace root:

```bash
# run an ad-hoc SQL query over the paired FASTQ rows
cargo run -p flash-df --example query --features datafusion -- \
  input1.fq \
  input2.fq \
  "SELECT tag1, seq1 FROM flash_pairs LIMIT 5"

# exercise the FLASH UDFs that produce the combined and not-combined outputs
cargo run -p flash-df --example flash_udf --features datafusion -- \
  input1.fq \
  input2.fq \
  5
```

## Website using WASM (Experimental)

This playground bundles the Rust implementation of FLASH compiled to
WebAssembly, so you can upload paired FASTQ files and inspect the merged results
directly in the browser:

1. Build the WebAssembly artifact from the workspace root:

   ```bash
   rustup target add wasm32-unknown-unknown # once per environment
   cargo build -p flash-wasm --release --target wasm32-unknown-unknown
   mkdir -p public
   cp ../target/wasm32-unknown-unknown/release/flash_wasm.wasm public/
   ```

2. Start the playground (`pnpm dev`/`npm run dev`) and open the **FLASH Merge**
   tab.
   - In **FLASH Merge**, select your forward (`R1`) and reverse (`R2`) FASTQ
     files, then click **Run FLASH** to view or download the merged outputs. The
     uploader also registers four DataFusion views for direct querying:

   - `flash_input_pairs` with the original paired reads.
   - `flash_combined` with successfully merged reads.
   - `flash_not_combined_left` / `flash_not_combined_right` mirroring FLASH's
     not-combined outputs.
