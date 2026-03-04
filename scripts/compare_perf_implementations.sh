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

ENERGY_RUN_BIN=""
ENERGY_RUN_AVAILABLE=0
ENERGY_RUN_DISABLE_GPU="${ENERGY_RUN_DISABLE_GPU:-1}"
if command -v energy-run >/dev/null 2>&1; then
  ENERGY_RUN_BIN="$(command -v energy-run)"
  ENERGY_RUN_AVAILABLE=1
fi

now_ns() {
  date +%s%N
}

elapsed_seconds() {
  local start_ns=$1
  local end_ns=$2
  awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.3f", (end-start)/1000000000 }'
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

json_bool() {
  if [[ "$1" == "1" ]]; then
    printf 'true'
  else
    printf 'false'
  fi
}

fmt_optional_number() {
  local value=$1
  local decimals=$2
  if [[ "$value" == "null" || -z "$value" ]]; then
    printf 'N/A'
  else
    awk -v v="$value" -v d="$decimals" 'BEGIN { printf "%.*f", d, v }'
  fi
}

# Emit a tab-separated row:
# wall_s cpu_s energy_used energy_duration_s energy_cpu_j energy_gpu_j
# energy_total_j energy_total_kwh energy_avg_cpu_w energy_avg_gpu_w
# energy_avg_total_w energy_exit_code
run_timed() {
  local stdout_log=$1
  local stderr_log=$2
  shift 2

  local time_file
  time_file=$(mktemp "${OUTPUT_DIR}/time_XXXXXX")
  local energy_file
  energy_file=$(mktemp "${OUTPUT_DIR}/energy_XXXXXX.json")

  local wall_s cpu_s cmd_status
  local energy_used=0
  local energy_duration="null"
  local energy_cpu="null"
  local energy_gpu="null"
  local energy_total="null"
  local energy_kwh="null"
  local energy_avg_cpu="null"
  local energy_avg_gpu="null"
  local energy_avg_total="null"
  local energy_exit="null"

  if [[ "$ENERGY_RUN_AVAILABLE" -eq 1 ]]; then
    local -a energy_args
    energy_args=(--output json)
    if [[ "${ENERGY_RUN_DISABLE_GPU}" == "1" ]]; then
      energy_args+=(--no-gpu)
    fi

    set +e
    /usr/bin/time -o "$time_file" -f '%e %U %S' \
      "$ENERGY_RUN_BIN" "${energy_args[@]}" -- \
      bash -c 'stdout_log=$1; stderr_log=$2; shift 2; exec "$@" >"$stdout_log" 2>"$stderr_log"' _ \
      "$stdout_log" "$stderr_log" "$@" >"$energy_file"
    cmd_status=$?
    set -e

    if [[ "$cmd_status" -ne 0 && ! -s "$energy_file" ]]; then
      echo "Warning: energy-run backend unavailable; retrying with --no-cpu --no-gpu." >&2
      set +e
      /usr/bin/time -o "$time_file" -f '%e %U %S' \
        "$ENERGY_RUN_BIN" --output json --no-cpu --no-gpu -- \
        bash -c 'stdout_log=$1; stderr_log=$2; shift 2; exec "$@" >"$stdout_log" 2>"$stderr_log"' _ \
        "$stdout_log" "$stderr_log" "$@" >"$energy_file"
      cmd_status=$?
      set -e
    fi

    if [[ -s "$energy_file" ]]; then
      energy_used=1
      local parsed_energy
      parsed_energy=$(python3 - "$energy_file" <<'PY'
import json
import sys

path = sys.argv[1]

def to_field(value):
    if value is None:
        return "null"
    return str(value)

with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)

keys = [
    "duration_s",
    "cpu_energy_j",
    "gpu_energy_j",
    "total_energy_j",
    "total_energy_kwh",
    "avg_cpu_power_w",
    "avg_gpu_power_w",
    "avg_total_power_w",
    "exit_code",
]
print("\t".join(to_field(data.get(key)) for key in keys))
PY
      )
      IFS=$'\t' read -r \
        energy_duration energy_cpu energy_gpu energy_total energy_kwh \
        energy_avg_cpu energy_avg_gpu energy_avg_total energy_exit <<<"$parsed_energy"
    elif [[ "$cmd_status" -eq 0 ]]; then
      set +e
      /usr/bin/time -o "$time_file" -f '%e %U %S' "$@" >"$stdout_log" 2>"$stderr_log"
      cmd_status=$?
      set -e
    fi
  else
    set +e
    /usr/bin/time -o "$time_file" -f '%e %U %S' "$@" >"$stdout_log" 2>"$stderr_log"
    cmd_status=$?
    set -e
  fi

  wall_s=$(awk '{ printf "%.3f", $1 }' "$time_file")
  cpu_s=$(awk '{ printf "%.3f", $2 + $3 }' "$time_file")
  rm -f "$time_file" "$energy_file"

  if [[ "$cmd_status" -ne 0 ]]; then
    echo "Command failed with exit code ${cmd_status}: $*" >&2
    return "$cmd_status"
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$wall_s" "$cpu_s" "$energy_used" "$energy_duration" "$energy_cpu" "$energy_gpu" \
    "$energy_total" "$energy_kwh" "$energy_avg_cpu" "$energy_avg_gpu" "$energy_avg_total" \
    "$energy_exit"
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
  declare -A energy_used_results
  declare -A energy_duration_results
  declare -A energy_cpu_results
  declare -A energy_gpu_results
  declare -A energy_total_results
  declare -A energy_kwh_results
  declare -A energy_avg_cpu_results
  declare -A energy_avg_gpu_results
  declare -A energy_avg_total_results
  declare -A energy_exit_results

  for size_mb in "${SIZES_MB[@]}"; do
    generate_fasta "$size_mb"
    local input="$INPUT_DIR/${size_mb}mb.fa"

    local rust_out="${OUTPUT_DIR}/rust_${size_mb}mb.tsv"
    echo "Running Rust PERF for ${size_mb}MB..."
    local rust_t
    local wall cpu energy_used energy_duration energy_cpu energy_gpu
    local energy_total energy_kwh energy_avg_cpu energy_avg_gpu energy_avg_total energy_exit
    rust_t=$(run_rust "$input" "$rust_out")
    IFS=$'\t' read -r \
      wall cpu energy_used energy_duration energy_cpu energy_gpu \
      energy_total energy_kwh energy_avg_cpu energy_avg_gpu energy_avg_total \
      energy_exit <<<"$rust_t"
    wall_results["${size_mb},perf-rust"]=$wall
    cpu_results["${size_mb},perf-rust"]=$cpu
    energy_used_results["${size_mb},perf-rust"]=$energy_used
    energy_duration_results["${size_mb},perf-rust"]=$energy_duration
    energy_cpu_results["${size_mb},perf-rust"]=$energy_cpu
    energy_gpu_results["${size_mb},perf-rust"]=$energy_gpu
    energy_total_results["${size_mb},perf-rust"]=$energy_total
    energy_kwh_results["${size_mb},perf-rust"]=$energy_kwh
    energy_avg_cpu_results["${size_mb},perf-rust"]=$energy_avg_cpu
    energy_avg_gpu_results["${size_mb},perf-rust"]=$energy_avg_gpu
    energy_avg_total_results["${size_mb},perf-rust"]=$energy_avg_total
    energy_exit_results["${size_mb},perf-rust"]=$energy_exit

    local py_out="${OUTPUT_DIR}/cpython_${size_mb}mb.tsv"
    echo "Running CPython PERF for ${size_mb}MB..."
    local py_t
    local py_wall py_cpu py_energy_used py_energy_duration py_energy_cpu py_energy_gpu
    local py_energy_total py_energy_kwh py_energy_avg_cpu py_energy_avg_gpu py_energy_avg_total py_energy_exit
    py_t=$(run_cpython "$input" "$py_out")
    IFS=$'\t' read -r \
      py_wall py_cpu py_energy_used py_energy_duration py_energy_cpu py_energy_gpu \
      py_energy_total py_energy_kwh py_energy_avg_cpu py_energy_avg_gpu py_energy_avg_total \
      py_energy_exit <<<"$py_t"
    wall_results["${size_mb},perf-cpython"]=$py_wall
    cpu_results["${size_mb},perf-cpython"]=$py_cpu
    energy_used_results["${size_mb},perf-cpython"]=$py_energy_used
    energy_duration_results["${size_mb},perf-cpython"]=$py_energy_duration
    energy_cpu_results["${size_mb},perf-cpython"]=$py_energy_cpu
    energy_gpu_results["${size_mb},perf-cpython"]=$py_energy_gpu
    energy_total_results["${size_mb},perf-cpython"]=$py_energy_total
    energy_kwh_results["${size_mb},perf-cpython"]=$py_energy_kwh
    energy_avg_cpu_results["${size_mb},perf-cpython"]=$py_energy_avg_cpu
    energy_avg_gpu_results["${size_mb},perf-cpython"]=$py_energy_avg_gpu
    energy_avg_total_results["${size_mb},perf-cpython"]=$py_energy_avg_total
    energy_exit_results["${size_mb},perf-cpython"]=$py_energy_exit

    validate_outputs "$py_out" "$rust_out" "Rust vs CPython (${size_mb}MB)"

    if [[ " ${PROGRAMS[*]} " == *" perf-pypy "* ]]; then
      local pypy_out="${OUTPUT_DIR}/pypy_${size_mb}mb.tsv"
      echo "Running PyPy PERF for ${size_mb}MB..."
      local pypy_t
      local pypy_wall pypy_cpu pypy_energy_used pypy_energy_duration pypy_energy_cpu pypy_energy_gpu
      local pypy_energy_total pypy_energy_kwh pypy_energy_avg_cpu pypy_energy_avg_gpu pypy_energy_avg_total pypy_energy_exit
      pypy_t=$(run_pypy "$input" "$pypy_out")
      IFS=$'\t' read -r \
        pypy_wall pypy_cpu pypy_energy_used pypy_energy_duration pypy_energy_cpu pypy_energy_gpu \
        pypy_energy_total pypy_energy_kwh pypy_energy_avg_cpu pypy_energy_avg_gpu pypy_energy_avg_total \
        pypy_energy_exit <<<"$pypy_t"
      wall_results["${size_mb},perf-pypy"]=$pypy_wall
      cpu_results["${size_mb},perf-pypy"]=$pypy_cpu
      energy_used_results["${size_mb},perf-pypy"]=$pypy_energy_used
      energy_duration_results["${size_mb},perf-pypy"]=$pypy_energy_duration
      energy_cpu_results["${size_mb},perf-pypy"]=$pypy_energy_cpu
      energy_gpu_results["${size_mb},perf-pypy"]=$pypy_energy_gpu
      energy_total_results["${size_mb},perf-pypy"]=$pypy_energy_total
      energy_kwh_results["${size_mb},perf-pypy"]=$pypy_energy_kwh
      energy_avg_cpu_results["${size_mb},perf-pypy"]=$pypy_energy_avg_cpu
      energy_avg_gpu_results["${size_mb},perf-pypy"]=$pypy_energy_avg_gpu
      energy_avg_total_results["${size_mb},perf-pypy"]=$pypy_energy_avg_total
      energy_exit_results["${size_mb},perf-pypy"]=$pypy_energy_exit

      validate_outputs "$py_out" "$pypy_out" "PyPy vs CPython (${size_mb}MB)"
    fi
  done

  echo
  echo "PERF benchmark results (seconds):"
  printf "%-10s %-15s %10s %10s %12s %8s %10s %12s %12s\n" \
    "Size(MB)" "Program" "Wall(s)" "CPU(s)" "CPU/Wall" \
    "E-Run" "E-Dur(s)" "E-Total(J)" "E-AvgW"
  printf "%-10s %-15s %10s %10s %12s %8s %10s %12s %12s\n" \
    "--------" "-------" "-------" "------" "--------" \
    "-----" "--------" "----------" "------"

  for size_mb in "${SIZES_MB[@]}"; do
    for program in "${PROGRAMS[@]}"; do
      local key="${size_mb},${program}"
      if [[ -n "${wall_results[$key]:-}" ]]; then
        local wall="${wall_results[$key]}"
        local cpu="${cpu_results[$key]}"
        local energy_duration="${energy_duration_results[$key]:-null}"
        local energy_total="${energy_total_results[$key]:-null}"
        local energy_avg_total="${energy_avg_total_results[$key]:-null}"
        local energy_used="${energy_used_results[$key]:-0}"
        local energy_used_fmt
        local energy_duration_fmt energy_total_fmt energy_avg_total_fmt
        local ratio
        ratio=$(awk -v w="$wall" -v c="$cpu" 'BEGIN { if (w > 0) printf "%.1fx", c/w; else print "N/A" }')
        energy_used_fmt=$(json_bool "$energy_used")
        energy_duration_fmt=$(fmt_optional_number "$energy_duration" 3)
        energy_total_fmt=$(fmt_optional_number "$energy_total" 3)
        energy_avg_total_fmt=$(fmt_optional_number "$energy_avg_total" 3)
        printf "%-10s %-15s %10s %10s %12s %8s %10s %12s %12s\n" \
          "$size_mb" "$program" "$wall" "$cpu" "$ratio" \
          "$energy_used_fmt" "$energy_duration_fmt" "$energy_total_fmt" "$energy_avg_total_fmt"
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
    echo "    \"uname\": \"$(uname -srm)\","
    echo "    \"energy_run_available\": $(json_bool "$ENERGY_RUN_AVAILABLE"),"
    if [[ "$ENERGY_RUN_AVAILABLE" -eq 1 ]]; then
      echo "    \"energy_run_path\": \"$(json_escape "$ENERGY_RUN_BIN")\","
    else
      echo '    "energy_run_path": null,'
    fi
    echo "    \"energy_run_disable_gpu\": $(json_bool "$ENERGY_RUN_DISABLE_GPU")"
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
          local energy_used="${energy_used_results[$key]:-0}"
          local energy_duration="${energy_duration_results[$key]:-null}"
          local energy_cpu="${energy_cpu_results[$key]:-null}"
          local energy_gpu="${energy_gpu_results[$key]:-null}"
          local energy_total="${energy_total_results[$key]:-null}"
          local energy_kwh="${energy_kwh_results[$key]:-null}"
          local energy_avg_cpu="${energy_avg_cpu_results[$key]:-null}"
          local energy_avg_gpu="${energy_avg_gpu_results[$key]:-null}"
          local energy_avg_total="${energy_avg_total_results[$key]:-null}"
          local energy_exit="${energy_exit_results[$key]:-null}"
          local ratio
          ratio=$(awk -v w="$wall" -v c="$cpu" 'BEGIN { if (w > 0) printf "%.1f", c/w; else print "0" }')
          echo -n "    {\"size_mb\": ${size_mb}, \"program\": \"${program}\", \"wall_s\": ${wall}, \"cpu_s\": ${cpu}, \"cpu_wall_ratio\": ${ratio}, \"execution\": {\"energy_run_used\": $(json_bool "$energy_used"), \"energy\": {\"duration_s\": ${energy_duration}, \"cpu_energy_j\": ${energy_cpu}, \"gpu_energy_j\": ${energy_gpu}, \"total_energy_j\": ${energy_total}, \"total_energy_kwh\": ${energy_kwh}, \"avg_cpu_power_w\": ${energy_avg_cpu}, \"avg_gpu_power_w\": ${energy_avg_gpu}, \"avg_total_power_w\": ${energy_avg_total}, \"exit_code\": ${energy_exit}}}}"
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
