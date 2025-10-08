# flash_rs

Workspace containing the Rust port of the FLASH "lowercase overhang" tool.

- `flash-lib`: core library crate exposing the merge algorithm.
- `flash-cli`: thin CLI wrapper that matches the original FLASH command-line
  flags and writes the three FASTQ outputs.

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
