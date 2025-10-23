# Agent Log

## Context

- Goal: migrate FastFiRSt to Rust using Apache DataFusion/Ballista while
  maintaining compatibility with the original FLASH outputs.
- Current focus: clean separation between reusable library code and CLI entry
  point to prepare for future integrations.

## Snapshot (2024-09-26)

- Restructured the Rust work into a cargo workspace: `flash-lib` (library),
  `flash-cli` (binary), and the new `flash-df` crate for distributed runtime
  scaffolding.
- `flash-lib` now exposes `FastqRecord`/`FastqPairReader` alongside
  `merge_fastq_files` and `CombineParams`; golden test uses the upstream FLASH
  outputs for regression coverage.
- `flash-df` wraps `FlashJobConfig`/`FlashDistributedJob`, offering a local
  fallback plus a basic `FastqTableProvider`, registration helpers for
  DataFusion logical plans, and example CLIs for interactive exploration. The
  crate now targets DataFusion 41, implements the FLASH UDFs using
  `ScalarUDFImpl`, builds annotated logical plans that align with the
  combined/not-combined FLASH stages, and can now materialise the DataFusion
  outputs back into the three FLASH FASTQ files.
- `flash-wasm` exposes the FLASH pipeline to WebAssembly and the playground now
  supports uploading FASTQs, streaming the outputs, and auto-registering
  `flash_*` tables for interactive SQL queries. The Upload tab focuses on the
  FLASH merge workflow after removing the experimental dataset uploader and
  cloud credentials panel.
- Added a GitHub Action to publish `flash-cli` to crates.io together with the
  required crate metadata (license, description, README). Local development uses
  a `[patch.crates-io]` override so publishing relies on the registry version of
  `flash-lib`.
- CLI depends on the library and keeps parity with the C tool's arguments.
- README updated with workspace instructions, library example, and feature-gated
  DataFusion notes.

## Pending

- Implement the Ballista submission path and add integration tests once cluster
  wiring is available.
- Consider richer unit coverage inside `flash-lib` beyond the golden test.

## 2025-10-23

- Added `.devcontainer/Dockerfile` for GitHub Codespaces using the Rust devcontainer base image.
- Ensured Python 3 and C++ build tooling (build-essential, cmake, pkg-config) are installed to compile the workspace.
- Prefetched Cargo dependencies during the image build by copying the full workspace manifests.
- Added `.devcontainer/.dockerignore` to keep the build context small when building Codespaces images.
- Reused the base `vscode` user by renaming it to `app` so UID/GID stay 1000 and workspace permissions line up with host mounts.
- Added `scripts/build_flash_lowercase_overhang.sh` to clone and build the upstream FLASH lowercase overhang fork, publishing the executable to `bin/flash-lowercase-overhang` for container use.
- Created the `fastq-gen-cli` workspace member to emit synthetic paired-end FASTQ files with configurable record counts, read length, and output paths.
- Documented the generator usage in the README and added unit coverage that checks record formatting plus end-to-end file emission.
- Added `scripts/compare_flash_implementations.sh` to generate multi-scale datasets, benchmark both FLASH binaries, verify outputs, and summarize timings.
- Comparison script now hashes FASTQ records to surface whether mismatches are due to order or content, still failing if order differs; added `FLASH_BENCH_COUNTS` env override for custom dataset sizes.
- Order validation now identifies which implementation breaks tag ordering by referencing the original R1 inputs and emits warnings without aborting the run.
- Added flash-df `flash_cli` example to mirror CLI outputs and wired benchmark coverage including order diagnostics for DataFusion runs.
- Streamlined flash-df by streaming FASTQ batches, updated benchmark coverage, and added flash_cli example for DataFusion parity.
- Parallelised `flash-df` scan via chunked worker pool, documented the design in `architecture.md`, and ensured the benchmark script exercises the DataFusion example alongside the C and Rust CLIs.
