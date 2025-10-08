# Agent Log

## Context
- Goal: migrate FastFiRSt to Rust using Apache DataFusion/Ballista while
  maintaining compatibility with the original FLASH outputs.
- Current focus: clean separation between reusable library code and CLI entry
  point to prepare for future integrations.

## Snapshot (2024-09-26)
- Restructured the Rust work into a cargo workspace: `flash-lib` (library) and
  `flash-cli` (binary).
- `flash-lib` exports `merge_fastq_files` and `CombineParams`; golden test uses
  the upstream FLASH outputs for regression coverage.
- CLI now depends on the library and keeps parity with the C tool's arguments.
- README updated with workspace instructions and library usage example.

## Pending
- Consider richer unit coverage inside `flash-lib` beyond the golden test.
- Start sketching DataFusion/Ballista adapters that consume `flash-lib`.
