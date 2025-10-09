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
- CLI depends on the library and keeps parity with the C tool's arguments.
- README updated with workspace instructions, library example, and feature-gated
  DataFusion notes.

## Pending

- Implement the Ballista submission path and add integration tests once cluster
  wiring is available.
- Consider richer unit coverage inside `flash-lib` beyond the golden test.
