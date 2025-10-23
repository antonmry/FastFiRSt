# flash-df Architecture Notes

## Goals

- Maintain byte-for-byte parity with the Rust CLI while moving execution into
  DataFusion/Ballista.
- Support streaming workloads so memory remains bounded (only a few chunks are
  in flight).
- Scale vertically (per-node parallelism) and horizontally (future Ballista
  partitions).
- Preserve deterministic ordering; outputs must match the input sequence order.

## Current Pipeline (2025-10-23)

```text
FastqPairReader --> FastqScanExec (1 partition)
                  |-- chunk reader (orders chunks by id)
                  |-- worker pool (CPU parallel merge)
                  '-- ordered writer (priority-queue commit)
                --> DataFusion LogicalPlan + UDFs --> streaming writer --> disk
```

### Key Changes

1. **Streaming TableProvider**
   - `FastqScanExec` implements a custom `ExecutionPlan` that feeds DataFusion
     via a single partition but internally uses a worker pool.
   - Chunks (default 2048 pairs) are read sequentially, tagged with a chunk id,
     and dispatched to workers.

2. **Worker Pool with Ordered Merge**
   - Each worker receives a `ChunkTask` with FASTQ records and builds an Arrow
     `RecordBatch` after running the UDF logic.
   - Results flow back through a `crossbeam-channel`; a priority queue keyed by
     chunk id ensures we emit batches strictly in order.
   - `FastqScanStream::poll_next` keeps at most `2 * worker_threads` inflight
     chunks (configurable via `FlashJobConfig::with_worker_threads`, defaulting
     to the logical CPU count).

3. **Streaming Writer**
   - `write_plan_stream` now consumes DataFusion’s stream directly and writes
     FASTQ records as soon as batches are ready—no `collect()` buffering.

4. **Benchmark Integration**
   - `scripts/compare_flash_implementations.sh` now benchmarks three binaries:
     C FLASH, Rust CLI, and the `flash-df` example, reporting per-input runtime
     and input sizes.

### Trade-offs

- **Memory**: Bounded by `batch_size * inflight`; default is conservative (2048
  pairs × ~2 inflight chunks).
- **Latency vs Throughput**: Larger `batch_size` improves CPU utilisation but
  increases latency before the first batch emerges. Configurable via
  `FlashJobConfig::with_batch_size`.
- **CPU Utilisation**: Worker pool uses `num_cpus::get()` threads; we may add a
  config knob if oversubscription becomes an issue.
- **Error Handling**: Worker errors fail fast and drain the queue; DataFusion
  receives the failure via `RecordBatchStream` error.

## Roadmap

1. **Expose configuration**
   - Allow CLI/clients to tune chunk size and worker count. ✅
   - Surface queue depth metrics for observability.

2. **Ballista Integration**
   - Partition input across executors by chunk id ranges.
   - Coordinator concatenates per-partition outputs preserving global order.
   - Investigate writing directly to distributed object stores.

3. **I/O Improvements**
   - Optionally use async prefetch or mmap for FASTQ reading when beneficial.
   - Support gzipped FASTQs (requires blocking thread pool due to libflate).

4. **Profiling / Optimisation**
   - Benchmark with real datasets (massive, billions of pairs).
   - Identify hotspots in `combine_pair_from_strs`; consider batch UDF.

5. **Resilience**
   - For distributed execution add retry/rewind logic per chunk.
   - Persist chunk metadata to allow resume-after-failure.
