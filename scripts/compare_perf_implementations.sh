#!/usr/bin/env bash
# Compare Python PERF (CPython/PyPy) against Rust perf-cli.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${INPUT_DIR:-${ROOT_DIR}/benchmarks/perf_inputs}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/benchmarks/perf_outputs}"

SIZES_MB=(1 10 100 500)
if [[ -n "${PERF_BENCH_SIZES:-}" ]]; then
  read -r -a SIZES_MB <<<"${PERF_BENCH_SIZES}"
fi

PERF_RUST_BIN="${PERF_RUST_BIN:-${ROOT_DIR}/target/release/perf-cli}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PERF_BIN="${PERF_BIN:-PERF}"
PYPY_BIN="${PYPY_BIN:-pypy3}"
PYPY_PERF_BIN="${PYPY_PERF_BIN:-}"

PROGRAMS=("perf-rust" "perf-cpython")
if command -v "${PYPY_BIN}" >/dev/null 2>&1; then
  PROGRAMS+=("perf-pypy")
fi

now_ns() {
  date +%s%N
}

elapsed_seconds() {
  local start_ns=$1
  local end_ns=$2
  awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.3f", (end-start)/1000000000 }'
}

run_timed() {
  local stdout_log=$1
  local stderr_log=$2
  shift 2
  local time_file
  time_file=$(mktemp "${OUTPUT_DIR}/time_XXXXXX")
  local start end wall_s cpu_s
  start=$(now_ns)
  /usr/bin/time -o "$time_file" -f '%U %S' "$@" >"$stdout_log" 2>"$stderr_log"
  end=$(now_ns)
  wall_s=$(elapsed_seconds "$start" "$end")
  cpu_s=$(awk '{ printf "%.3f", $1 + $2 }' "$time_file")
  rm -f "$time_file"
  echo "${wall_s} ${cpu_s}"
}

ensure_binaries() {
  mkdir -p "$INPUT_DIR" "$OUTPUT_DIR"

  echo "Building perf-cli..."
  cargo build --release -p perf-cli

  if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    echo "Missing ${PYTHON_BIN}; install Python 3 first." >&2
    exit 1
  fi

  if ! command -v "${PERF_BIN}" >/dev/null 2>&1; then
    echo "PERF command not found. Install via: pip install perf_ssr" >&2
    exit 1
  fi

  if command -v "${PYPY_BIN}" >/dev/null 2>&1; then
    # Auto-detect PERF console script in same directory as PYPY_BIN if not set.
    if [[ -z "${PYPY_PERF_BIN}" ]]; then
      local pypy_dir
      pypy_dir="$(dirname "$(command -v "${PYPY_BIN}")")"
      if [[ -x "${pypy_dir}/PERF" ]]; then
        PYPY_PERF_BIN="${pypy_dir}/PERF"
      fi
    fi
    if [[ -z "${PYPY_PERF_BIN}" ]] || [[ ! -x "${PYPY_PERF_BIN}" ]]; then
      echo "PyPy found but PERF command not found; skipping PyPy benchmark. Install via: pypy3 -m pip install perf_ssr" >&2
      PROGRAMS=("perf-rust" "perf-cpython")
    fi
  fi
}

generate_fasta() {
  local size_mb=$1
  local output="$INPUT_DIR/${size_mb}mb.fa"
  if [[ -f "$output" ]]; then
    echo "Reusing ${output}"
    return
  fi

  echo "Generating ${size_mb}MB FASTA..."
  "${PYTHON_BIN}" - "$output" "$size_mb" <<'PY'
import random
import sys

out_path = sys.argv[1]
size_mb = int(sys.argv[2])
target = size_mb * 1024 * 1024
line_len = 80
bases = "ACGT"

random.seed(42 + size_mb)
current = 0
seq_no = 1

with open(out_path, "w", encoding="utf-8") as out:
    while current < target:
        out.write(f">seq{seq_no}\n")
        current += len(f">seq{seq_no}\n")
        seq_no += 1

        # Keep moderately sized records and inject deterministic SSR-like segments.
        seq_len = min(50000, target - current)
        if seq_len <= 0:
            break

        seq = []
        i = 0
        while i < seq_len:
            if i % 997 == 0 and i + 24 < seq_len:
                motif = random.choice(["A", "AC", "ATG", "CAGT", "AACGTT"])
                repeat = (motif * 32)[:24]
                seq.append(repeat)
                i += len(repeat)
            else:
                seq.append(random.choice(bases))
                i += 1

        chunk = "".join(seq)
        for start in range(0, len(chunk), line_len):
            line = chunk[start:start + line_len]
            out.write(line)
            out.write("\n")
            current += len(line) + 1
PY
}

run_cpython() {
  local input=$1
  local output=$2
  run_timed "${output}.stdout.log" "${output}.stderr.log" \
    "${PERF_BIN}" -i "$input" -o "$output"
}

run_pypy() {
  local input=$1
  local output=$2
  run_timed "${output}.stdout.log" "${output}.stderr.log" \
    "${PYPY_PERF_BIN}" -i "$input" -o "$output"
}

run_rust() {
  local input=$1
  local output=$2
  run_timed "${output}.stdout.log" "${output}.stderr.log" \
    "${PERF_RUST_BIN}" -i "$input" -o "$output" --format fasta
}

normalize_output() {
  local input=$1
  local output=$2
  grep -v '^#' "$input" | LC_ALL=C sort > "$output"
}

validate_outputs() {
  local baseline=$1
  local candidate=$2
  local label=$3

  local base_sorted candidate_sorted
  base_sorted=$(mktemp "${OUTPUT_DIR}/base_XXXXXX")
  candidate_sorted=$(mktemp "${OUTPUT_DIR}/cand_XXXXXX")

  normalize_output "$baseline" "$base_sorted"
  normalize_output "$candidate" "$candidate_sorted"

  if ! cmp -s "$base_sorted" "$candidate_sorted"; then
    echo "Output mismatch for ${label}" >&2
    diff -u <(head -n 50 "$base_sorted") <(head -n 50 "$candidate_sorted") >&2 || true
    rm -f "$base_sorted" "$candidate_sorted"
    exit 1
  fi

  rm -f "$base_sorted" "$candidate_sorted"
}

main() {
  ensure_binaries

  declare -A wall_results
  declare -A cpu_results

  for size_mb in "${SIZES_MB[@]}"; do
    generate_fasta "$size_mb"
    local input="$INPUT_DIR/${size_mb}mb.fa"

    local rust_out="${OUTPUT_DIR}/rust_${size_mb}mb.tsv"
    echo "Running Rust PERF for ${size_mb}MB..."
    local rust_t
    rust_t=$(run_rust "$input" "$rust_out")
    wall_results["${size_mb},perf-rust"]=${rust_t% *}
    cpu_results["${size_mb},perf-rust"]=${rust_t#* }

    local py_out="${OUTPUT_DIR}/cpython_${size_mb}mb.tsv"
    echo "Running CPython PERF for ${size_mb}MB..."
    local py_t
    py_t=$(run_cpython "$input" "$py_out")
    wall_results["${size_mb},perf-cpython"]=${py_t% *}
    cpu_results["${size_mb},perf-cpython"]=${py_t#* }

    validate_outputs "$py_out" "$rust_out" "Rust vs CPython (${size_mb}MB)"

    if [[ " ${PROGRAMS[*]} " == *" perf-pypy "* ]]; then
      local pypy_out="${OUTPUT_DIR}/pypy_${size_mb}mb.tsv"
      echo "Running PyPy PERF for ${size_mb}MB..."
      local pypy_t
      pypy_t=$(run_pypy "$input" "$pypy_out")
      wall_results["${size_mb},perf-pypy"]=${pypy_t% *}
      cpu_results["${size_mb},perf-pypy"]=${pypy_t#* }

      validate_outputs "$py_out" "$pypy_out" "PyPy vs CPython (${size_mb}MB)"
    fi
  done

  echo
  echo "PERF benchmark results (seconds):"
  printf "%-10s %-15s %10s %10s %12s\n" "Size(MB)" "Program" "Wall(s)" "CPU(s)" "CPU/Wall"
  printf "%-10s %-15s %10s %10s %12s\n" "--------" "-------" "-------" "------" "--------"

  for size_mb in "${SIZES_MB[@]}"; do
    for program in "${PROGRAMS[@]}"; do
      local key="${size_mb},${program}"
      if [[ -n "${wall_results[$key]:-}" ]]; then
        local wall="${wall_results[$key]}"
        local cpu="${cpu_results[$key]}"
        local ratio
        ratio=$(awk -v w="$wall" -v c="$cpu" 'BEGIN { if (w > 0) printf "%.1fx", c/w; else print "N/A" }')
        printf "%-10s %-15s %10s %10s %12s\n" "$size_mb" "$program" "$wall" "$cpu" "$ratio"
      fi
    done
  done

  # Write JSON results
  mkdir -p "${ROOT_DIR}/site/benchmarks"
  local json_file="${ROOT_DIR}/site/benchmarks/perf_results.json"
  {
    echo '{'
    echo '  "metadata": {'
    echo "    \"date\": \"$(date -Iseconds)\","
    echo "    \"hostname\": \"$(hostname)\","
    echo "    \"uname\": \"$(uname -srm)\""
    echo '  },'
    echo '  "programs": ['
    local first_prog=1
    for program in "${PROGRAMS[@]}"; do
      if [[ $first_prog -eq 0 ]]; then echo ','; fi
      echo -n "    \"${program}\""
      first_prog=0
    done
    echo
    echo '  ],'
    echo '  "results": ['
    local first_entry=1
    for size_mb in "${SIZES_MB[@]}"; do
      for program in "${PROGRAMS[@]}"; do
        key="${size_mb},${program}"
        if [[ -n "${wall_results[$key]:-}" ]]; then
          if [[ $first_entry -eq 0 ]]; then echo ','; fi
          local wall="${wall_results[$key]}"
          local cpu="${cpu_results[$key]}"
          local ratio
          ratio=$(awk -v w="$wall" -v c="$cpu" 'BEGIN { if (w > 0) printf "%.1f", c/w; else print "0" }')
          echo -n "    {\"size_mb\": ${size_mb}, \"program\": \"${program}\", \"wall_s\": ${wall}, \"cpu_s\": ${cpu}, \"cpu_wall_ratio\": ${ratio}}"
          first_entry=0
        fi
      done
    done
    echo
    echo '  ]'
    echo '}'
  } > "$json_file"
  echo
  echo "JSON results written to ${json_file}"
}

main "$@"
