# flash_rs

Rust rewrite of the FLASH "lowercase overhang" tool. The binary exposes the
same core flags (`--min-overlap`, `--max-overlap`, `--max-mismatch-density`,
`--allow-outies`, `--cap-mismatch-quals`, `--lowercase-overhang`,
`--phred-offset`) and produces the same three FASTQ outputs as the original C
implementation.

## Requirements

- Rust toolchain (cargo 1.89+)

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run --release -- READ1.fq READ2.fq \
  --output-dir output_dir [--output-prefix out]
```

Outputs are written to `<prefix>.extendedFrags.fastq`,
`<prefix>.notCombined_1.fastq`, and `<prefix>.notCombined_2.fastq` in the given
directory. Optional parameters default to the FLASH values; run
`cargo run -- --help` for the full list.

