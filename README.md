# FastFiRSt

Workspace containing the Rust port of the FLASH "lowercase overhang" tool.

- `flash-lib`: core library crate exposing the merge algorithm.
- `flash-cli`: thin CLI wrapper that matches the original FLASH command-line
  flags and writes the three FASTQ outputs.
- `flash-df`: scaffolding for running the merge pipeline through
  DataFusion/Ballista (feature-gated stubs for now).
- `flash-wasm`: minimal WebAssembly interface exposing the FLASH merge logic for
  the playground UI.
- `wasm-playground`: Vite/Mantine web playground that can execute SQL queries
  against DataFusion and now run FLASH locally in the browser.

## Installing the CLI

The `flash-cli` binary is published on crates.io. Once a release is tagged and
the publish workflow runs successfully, you can install it with:

```bash
cargo install flash-cli
```

Updates are triggered manually via the `Publish flash-cli to crates.io` GitHub
Action. Ensure the crate version is bumped before running the workflow.

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

```rust
use flash_lib::{merge_fastq_files, CombineParams};

let params = CombineParams::default();
merge_fastq_files("input1.fq", "input2.fq", "./out", "out", &params)?;
```

## DataFusion/Ballista prototype

The `flash-df` crate currently exposes a `FlashDistributedJob` wrapper that can
execute the merge locally (re-using `flash-lib`) and provides feature-gated
hooks for wiring the workflow into a `datafusion::SessionContext`. Enable the
relevant feature flag when building:

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

For a quick preview, you can run the bundled example against the sample FASTQ
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

1. Build the WebAssembly artefact from the workspace root:

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
     uploader also registers four DataFusion views so you can query the results
     directly:

   - `flash_input_pairs` with the original paired reads.
   - `flash_combined` with successfully merged reads.
   - `flash_not_combined_left` / `flash_not_combined_right` mirroring FLASH's
     not-combined outputs.
