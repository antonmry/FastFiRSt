#!/usr/bin/env bash
# Generate FASTQ pairs at several scales, run the FLASH implementations,
# validate the outputs, and report execution times.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${INPUT_DIR:-${ROOT_DIR}/benchmarks/flash_inputs}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/benchmarks/flash_outputs}"
READ_LENGTH="${READ_LENGTH:-150}"

COUNTS=(1000 10000 100000 1000000 10000000)
if [[ -n "${FLASH_BENCH_COUNTS:-}" ]]; then
  read -r -a COUNTS <<<"${FLASH_BENCH_COUNTS}"
fi

FASTQ_GEN_BIN="${FASTQ_GEN_BIN:-${ROOT_DIR}/target/release/fastq-gen-cli}"
FLASH_CLI_BIN="${FLASH_CLI_BIN:-${ROOT_DIR}/target/release/flash-cli}"
FLASH_C_BIN="${FLASH_C_BIN:-${ROOT_DIR}/bin/flash-lowercase-overhang}"
FLASH_BENCH_PARALLEL_THREADS="${FLASH_BENCH_PARALLEL_THREADS:-}"
FLASH_BENCH_PARALLEL_BATCH_SIZE="${FLASH_BENCH_PARALLEL_BATCH_SIZE:-16384}"

if [[ -z "$FLASH_BENCH_PARALLEL_THREADS" ]]; then
  detected_threads=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 2)
  if [[ "$detected_threads" -gt 16 ]]; then
    FLASH_BENCH_PARALLEL_THREADS=16
  elif [[ "$detected_threads" -lt 1 ]]; then
    FLASH_BENCH_PARALLEL_THREADS=1
  else
    FLASH_BENCH_PARALLEL_THREADS="$detected_threads"
  fi
fi

PROGRAMS=("flash-cli" "flash-cli-parallel" "flash-lowercase-overhang")
ENERGY_RUN_BIN=""
ENERGY_RUN_AVAILABLE=0
ENERGY_RUN_DISABLE_GPU="${ENERGY_RUN_DISABLE_GPU:-1}"
if command -v energy-run >/dev/null 2>&1; then
  ENERGY_RUN_BIN="$(command -v energy-run)"
  ENERGY_RUN_AVAILABLE=1
fi

ensure_binaries() {
  local need_rust=0
  if [[ ! -x "$FASTQ_GEN_BIN" ]]; then
    need_rust=1
  fi

  if [[ "$need_rust" -eq 1 ]]; then
    echo "Building Rust binary (fastq-gen-cli)..."
    cargo build --release -p fastq-gen-cli
  fi

  echo "Building flash-cli with parallel feature..."
  cargo build --release -p flash-cli --features parallel

  if [[ ! -x "$FLASH_C_BIN" ]]; then
    echo "Building FLASH lowercase overhang binary..."
    "${ROOT_DIR}/scripts/build_flash_lowercase_overhang.sh"
  fi
}

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

# Run a command and emit a tab-separated row:
# wall_s cpu_s energy_used energy_duration_s energy_cpu_j energy_gpu_j
# energy_total_j energy_total_kwh energy_avg_cpu_w energy_avg_gpu_w
# energy_avg_total_w energy_exit_code
# Usage: run_timed stdout_log stderr_log cmd [args...]
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
      # Fallback path in case energy-run exits without writing JSON.
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

generate_inputs() {
  local count=$1
  local r1="$INPUT_DIR/${count}_R1.fastq"
  local r2="$INPUT_DIR/${count}_R2.fastq"

  if [[ -f "$r1" && -f "$r2" ]]; then
    echo "Reusing existing FASTQ inputs for ${count} records."
    return
  fi

  echo "Generating ${count} synthetic sequences (read length ${READ_LENGTH})..."
  mkdir -p "$INPUT_DIR"
  "$FASTQ_GEN_BIN" \
    --num-sequences "$count" \
    --read-length "$READ_LENGTH" \
    --output-r1 "$r1" \
    --output-r2 "$r2"
}

run_flash_cli() {
  local count=$1
  local r1=$2
  local r2=$3
  local output_dir="${OUTPUT_DIR}/flash-cli/${count}"

  rm -rf "$output_dir"
  mkdir -p "$output_dir"

  run_timed "$output_dir/stdout.log" "$output_dir/stderr.log" \
    "$FLASH_CLI_BIN" "$r1" "$r2" \
    --output-dir "$output_dir" \
    --output-prefix flash
}

run_flash_cli_parallel() {
  local count=$1
  local r1=$2
  local r2=$3
  local output_dir="${OUTPUT_DIR}/flash-cli-parallel/${count}"

  rm -rf "$output_dir"
  mkdir -p "$output_dir"

  run_timed "$output_dir/stdout.log" "$output_dir/stderr.log" \
    "$FLASH_CLI_BIN" "$r1" "$r2" \
    --output-dir "$output_dir" \
    --output-prefix flash \
    --parallel \
    --threads "$FLASH_BENCH_PARALLEL_THREADS" \
    --batch-size "$FLASH_BENCH_PARALLEL_BATCH_SIZE"
}

run_flash_c() {
  local count=$1
  local r1=$2
  local r2=$3
  local output_dir="${OUTPUT_DIR}/flash-lowercase-overhang/${count}"

  rm -rf "$output_dir"
  mkdir -p "$output_dir"

  run_timed "$output_dir/stdout.log" "$output_dir/stderr.log" \
    "$FLASH_C_BIN" "$r1" "$r2" \
    -d "$output_dir" \
    -o flash
}

validate_against_baseline() {
  local count=$1
  local r1_path=$2
  local baseline_dir=$3
  local baseline_label=$4
  local candidate_dir=$5
  local candidate_label=$6

  local files=(
    "flash.extendedFrags.fastq"
    "flash.notCombined_1.fastq"
    "flash.notCombined_2.fastq"
  )

  for filename in "${files[@]}"; do
    local baseline_file="${baseline_dir}/${filename}"
    local candidate_file="${candidate_dir}/${filename}"

    if [[ ! -f "$baseline_file" || ! -f "$candidate_file" ]]; then
      echo "Missing output file ${filename} for record count ${count}" >&2
      exit 1
    fi

    if cmp -s "$baseline_file" "$candidate_file"; then
      continue
    fi

    if compare_fastq_sets "$baseline_file" "$candidate_file" "${count}_${filename}"; then
      detect_order_mismatch \
        "$count" \
        "$r1_path" \
        "$baseline_file" \
        "$baseline_label" \
        "$candidate_file" \
        "$candidate_label" \
        "$filename"
      continue
    fi

    echo "Content mismatch detected in ${filename} between ${baseline_label} and ${candidate_label} for record count ${count}" >&2
    exit 1
  done
}

compare_fastq_sets() {
  local file_a=$1
  local file_b=$2
  local label=$3

  local tmp_a tmp_b
  tmp_a=$(mktemp "${OUTPUT_DIR}/hash_${label}_aXXXXXX")
  tmp_b=$(mktemp "${OUTPUT_DIR}/hash_${label}_bXXXXXX")

  hash_fastq_records "$file_a" "$tmp_a"
  hash_fastq_records "$file_b" "$tmp_b"

  LC_ALL=C sort -o "$tmp_a" "$tmp_a"
  LC_ALL=C sort -o "$tmp_b" "$tmp_b"

  if ! cmp -s "$tmp_a" "$tmp_b"; then
    echo "First few differing record hashes for ${label}:" >&2
    diff -u <(head -n 20 "$tmp_a") <(head -n 20 "$tmp_b") >&2 || true
    rm -f "$tmp_a" "$tmp_b"
    return 1
  fi

  rm -f "$tmp_a" "$tmp_b"
  return 0
}

hash_fastq_records() {
  local input=$1
  local output=$2

  python3 - "$input" "$output" <<'PY'
import hashlib
import sys

input_path, output_path = sys.argv[1], sys.argv[2]

def read_record(handle):
    tag = handle.readline()
    if not tag:
        return None
    seq = handle.readline()
    plus = handle.readline()
    qual = handle.readline()
    if not seq or not plus or not qual:
        raise SystemExit(f"Incomplete FASTQ record encountered in {input_path}")
    return tag.rstrip(), seq.rstrip(), qual.rstrip()

with open(input_path, "r", encoding="utf-8") as src, open(output_path, "w", encoding="utf-8") as dst:
    while True:
        record = read_record(src)
        if record is None:
            break
        tag, seq, qual = record
        digest = hashlib.sha256((seq + "\n" + qual).encode("utf-8")).hexdigest()
        dst.write(f"{tag}\t{digest}\n")
PY
}

ORDER_WARNINGS=()

detect_order_mismatch() {
  local count=$1
  local r1_path=$2
  local baseline_file=$3
  local baseline_label=$4
  local candidate_file=$5
  local candidate_label=$6
  local filename=$7

  local info_baseline info_candidate
  info_baseline=$(mktemp "${OUTPUT_DIR}/order_${baseline_label}_${count}_XXXXXX")
  info_candidate=$(mktemp "${OUTPUT_DIR}/order_${candidate_label}_${count}_XXXXXX")

  local baseline_ok=0
  local candidate_ok=0

  if check_order_against_input "$r1_path" "$baseline_file" "$info_baseline"; then
    baseline_ok=1
  else
    ORDER_WARNINGS+=("Record order mismatch for ${count} (${filename}, ${baseline_label}): $(<"$info_baseline")")
  fi

  if check_order_against_input "$r1_path" "$candidate_file" "$info_candidate"; then
    candidate_ok=1
  else
    ORDER_WARNINGS+=("Record order mismatch for ${count} (${filename}, ${candidate_label}): $(<"$info_candidate")")
  fi

  if [[ "$baseline_ok" -eq 1 && "$candidate_ok" -eq 0 ]]; then
    echo "Warning: ${candidate_label} output ordering diverged for ${count} (${filename})." >&2
  elif [[ "$baseline_ok" -eq 0 && "$candidate_ok" -eq 1 ]]; then
    echo "Warning: ${baseline_label} output ordering diverged for ${count} (${filename})." >&2
  else
    echo "Warning: Both ${baseline_label} and ${candidate_label} diverged in ordering for ${count} (${filename})." >&2
  fi

  rm -f "$info_baseline" "$info_candidate"
}

check_order_against_input() {
  local input_r1=$1
  local output_file=$2
  local info_file=$3

  python3 - "$input_r1" "$output_file" "$info_file" <<'PY'
import sys
from pathlib import Path

input_r1_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
info_path = Path(sys.argv[3])

def load_input_indices(path: Path):
    mapping = {}
    idx = 0
    with path.open("r", encoding="utf-8") as handle:
        while True:
            tag = handle.readline()
            if not tag:
                break
            seq = handle.readline()
            plus = handle.readline()
            qual = handle.readline()
            if not seq or not plus or not qual:
                raise SystemExit(f"Incomplete FASTQ record in {path}")
            mapping[tag.rstrip()] = idx
            idx += 1
    return mapping

def write_info(message: str):
    with info_path.open("w", encoding="utf-8") as info_handle:
        info_handle.write(message)

indices = load_input_indices(input_r1_path)
previous_idx = -1
previous_tag = None
record_number = 0

with output_path.open("r", encoding="utf-8") as out_handle:
    while True:
        tag = out_handle.readline()
        if not tag:
            break
        seq = out_handle.readline()
        plus = out_handle.readline()
        qual = out_handle.readline()
        if not seq or not plus or not qual:
            raise SystemExit(f"Incomplete FASTQ record in output {output_path}")
        tag = tag.rstrip()
        record_number += 1
        if tag not in indices:
            write_info(f"tag {tag} missing from inputs")
            sys.exit(1)
        idx = indices[tag]
        if idx <= previous_idx:
            write_info(
                f"out-of-order tag {tag} (input index {idx}) appears after {previous_tag} (index {previous_idx}) at output record {record_number}"
            )
            sys.exit(1)
        previous_idx = idx
        previous_tag = tag

sys.exit(0)
PY
}

main() {
  ensure_binaries
  mkdir -p \
    "$OUTPUT_DIR/flash-cli" \
    "$OUTPUT_DIR/flash-cli-parallel" \
    "$OUTPUT_DIR/flash-lowercase-overhang"

  declare -A wall_results
  declare -A cpu_results
  declare -A input_sizes
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

  for count in "${COUNTS[@]}"; do
    generate_inputs "$count"
    local r1="$INPUT_DIR/${count}_R1.fastq"
    local r2="$INPUT_DIR/${count}_R2.fastq"

    local size_mb
    size_mb=$(python3 - "$r1" "$r2" <<'PY'
import os
import sys
r1, r2 = sys.argv[1], sys.argv[2]
total = os.path.getsize(r1) + os.path.getsize(r2)
print(f"{total / (1024 * 1024):.2f}")
PY
    )
    input_sizes["$count"]=$size_mb

    echo "Running flash-cli for ${count} records..."
    local cli_timing
    local wall cpu energy_used energy_duration energy_cpu energy_gpu
    local energy_total energy_kwh energy_avg_cpu energy_avg_gpu energy_avg_total energy_exit
    cli_timing=$(run_flash_cli "$count" "$r1" "$r2")
    IFS=$'\t' read -r \
      wall cpu energy_used energy_duration energy_cpu energy_gpu \
      energy_total energy_kwh energy_avg_cpu energy_avg_gpu energy_avg_total \
      energy_exit <<<"$cli_timing"
    wall_results["${count},flash-cli"]=$wall
    cpu_results["${count},flash-cli"]=$cpu
    energy_used_results["${count},flash-cli"]=$energy_used
    energy_duration_results["${count},flash-cli"]=$energy_duration
    energy_cpu_results["${count},flash-cli"]=$energy_cpu
    energy_gpu_results["${count},flash-cli"]=$energy_gpu
    energy_total_results["${count},flash-cli"]=$energy_total
    energy_kwh_results["${count},flash-cli"]=$energy_kwh
    energy_avg_cpu_results["${count},flash-cli"]=$energy_avg_cpu
    energy_avg_gpu_results["${count},flash-cli"]=$energy_avg_gpu
    energy_avg_total_results["${count},flash-cli"]=$energy_avg_total
    energy_exit_results["${count},flash-cli"]=$energy_exit

    echo "Running flash-lowercase-overhang for ${count} records..."
    local c_timing
    local c_wall c_cpu c_energy_used c_energy_duration c_energy_cpu c_energy_gpu
    local c_energy_total c_energy_kwh c_energy_avg_cpu c_energy_avg_gpu c_energy_avg_total c_energy_exit
    c_timing=$(run_flash_c "$count" "$r1" "$r2")
    IFS=$'\t' read -r \
      c_wall c_cpu c_energy_used c_energy_duration c_energy_cpu c_energy_gpu \
      c_energy_total c_energy_kwh c_energy_avg_cpu c_energy_avg_gpu c_energy_avg_total \
      c_energy_exit <<<"$c_timing"
    wall_results["${count},flash-lowercase-overhang"]=$c_wall
    cpu_results["${count},flash-lowercase-overhang"]=$c_cpu
    energy_used_results["${count},flash-lowercase-overhang"]=$c_energy_used
    energy_duration_results["${count},flash-lowercase-overhang"]=$c_energy_duration
    energy_cpu_results["${count},flash-lowercase-overhang"]=$c_energy_cpu
    energy_gpu_results["${count},flash-lowercase-overhang"]=$c_energy_gpu
    energy_total_results["${count},flash-lowercase-overhang"]=$c_energy_total
    energy_kwh_results["${count},flash-lowercase-overhang"]=$c_energy_kwh
    energy_avg_cpu_results["${count},flash-lowercase-overhang"]=$c_energy_avg_cpu
    energy_avg_gpu_results["${count},flash-lowercase-overhang"]=$c_energy_avg_gpu
    energy_avg_total_results["${count},flash-lowercase-overhang"]=$c_energy_avg_total
    energy_exit_results["${count},flash-lowercase-overhang"]=$c_energy_exit

    echo "Running flash-cli (parallel, threads=${FLASH_BENCH_PARALLEL_THREADS}, batch=${FLASH_BENCH_PARALLEL_BATCH_SIZE}) for ${count} records..."
    local cli_parallel_timing
    local p_wall p_cpu p_energy_used p_energy_duration p_energy_cpu p_energy_gpu
    local p_energy_total p_energy_kwh p_energy_avg_cpu p_energy_avg_gpu p_energy_avg_total p_energy_exit
    cli_parallel_timing=$(run_flash_cli_parallel "$count" "$r1" "$r2")
    IFS=$'\t' read -r \
      p_wall p_cpu p_energy_used p_energy_duration p_energy_cpu p_energy_gpu \
      p_energy_total p_energy_kwh p_energy_avg_cpu p_energy_avg_gpu p_energy_avg_total \
      p_energy_exit <<<"$cli_parallel_timing"
    wall_results["${count},flash-cli-parallel"]=$p_wall
    cpu_results["${count},flash-cli-parallel"]=$p_cpu
    energy_used_results["${count},flash-cli-parallel"]=$p_energy_used
    energy_duration_results["${count},flash-cli-parallel"]=$p_energy_duration
    energy_cpu_results["${count},flash-cli-parallel"]=$p_energy_cpu
    energy_gpu_results["${count},flash-cli-parallel"]=$p_energy_gpu
    energy_total_results["${count},flash-cli-parallel"]=$p_energy_total
    energy_kwh_results["${count},flash-cli-parallel"]=$p_energy_kwh
    energy_avg_cpu_results["${count},flash-cli-parallel"]=$p_energy_avg_cpu
    energy_avg_gpu_results["${count},flash-cli-parallel"]=$p_energy_avg_gpu
    energy_avg_total_results["${count},flash-cli-parallel"]=$p_energy_avg_total
    energy_exit_results["${count},flash-cli-parallel"]=$p_energy_exit

    echo "Validating outputs for ${count} records..."
    local baseline_dir="${OUTPUT_DIR}/flash-cli/${count}"
    validate_against_baseline \
      "$count" "$r1" "$baseline_dir" "flash-cli" \
      "${OUTPUT_DIR}/flash-lowercase-overhang/${count}" "flash-lowercase-overhang"
    validate_against_baseline \
      "$count" "$r1" "$baseline_dir" "flash-cli" \
      "${OUTPUT_DIR}/flash-cli-parallel/${count}" "flash-cli-parallel"
  done

  echo
  echo "Benchmark results (seconds):"
  printf "%-12s %-12s %-28s %10s %10s %12s %8s %10s %12s %12s\n" \
    "Records" "Input(MB)" "Program" "Wall(s)" "CPU(s)" "CPU/Wall" \
    "E-Run" "E-Dur(s)" "E-Total(J)" "E-AvgW"
  printf "%-12s %-12s %-28s %10s %10s %12s %8s %10s %12s %12s\n" \
    "-------" "----------" "-------" "-------" "------" "--------" \
    "-----" "--------" "----------" "------"
  for count in "${COUNTS[@]}"; do
    for program in "${PROGRAMS[@]}"; do
      key="${count},${program}"
      if [[ -n "${wall_results[$key]:-}" ]]; then
        local wall="${wall_results[$key]}"
        local cpu="${cpu_results[$key]}"
        local energy_duration="${energy_duration_results[$key]:-null}"
        local energy_total="${energy_total_results[$key]:-null}"
        local energy_avg_total="${energy_avg_total_results[$key]:-null}"
        local energy_used="${energy_used_results[$key]:-0}"
        local ratio
        local energy_used_fmt
        local energy_duration_fmt energy_total_fmt energy_avg_total_fmt
        ratio=$(awk -v w="$wall" -v c="$cpu" 'BEGIN { if (w > 0) printf "%.1fx", c/w; else print "N/A" }')
        energy_used_fmt=$(json_bool "$energy_used")
        energy_duration_fmt=$(fmt_optional_number "$energy_duration" 3)
        energy_total_fmt=$(fmt_optional_number "$energy_total" 3)
        energy_avg_total_fmt=$(fmt_optional_number "$energy_avg_total" 3)
        printf "%-12s %-12s %-28s %10s %10s %12s %8s %10s %12s %12s\n" \
          "$count" "${input_sizes[$count]}" "$program" "$wall" "$cpu" "$ratio" \
          "$energy_used_fmt" "$energy_duration_fmt" "$energy_total_fmt" "$energy_avg_total_fmt"
      fi
    done
  done

  if ((${#ORDER_WARNINGS[@]})); then
    echo
    echo "Order warnings:"
    for warning in "${ORDER_WARNINGS[@]}"; do
      echo " - $warning"
    done
  fi

  # Write JSON results
  mkdir -p "${ROOT_DIR}/site/benchmarks"
  local json_file="${ROOT_DIR}/site/benchmarks/flash_results.json"
  {
    echo '{'
    echo '  "metadata": {'
    echo "    \"date\": \"$(date -Iseconds)\","
    echo "    \"read_length\": ${READ_LENGTH},"
    echo "    \"parallel_threads\": ${FLASH_BENCH_PARALLEL_THREADS},"
    echo "    \"parallel_batch_size\": ${FLASH_BENCH_PARALLEL_BATCH_SIZE},"
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
    for count in "${COUNTS[@]}"; do
      for program in "${PROGRAMS[@]}"; do
        key="${count},${program}"
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
          echo -n "    {\"records\": ${count}, \"input_mb\": ${input_sizes[$count]}, \"program\": \"${program}\", \"wall_s\": ${wall}, \"cpu_s\": ${cpu}, \"cpu_wall_ratio\": ${ratio}, \"execution\": {\"energy_run_used\": $(json_bool "$energy_used"), \"energy\": {\"duration_s\": ${energy_duration}, \"cpu_energy_j\": ${energy_cpu}, \"gpu_energy_j\": ${energy_gpu}, \"total_energy_j\": ${energy_total}, \"total_energy_kwh\": ${energy_kwh}, \"avg_cpu_power_w\": ${energy_avg_cpu}, \"avg_gpu_power_w\": ${energy_avg_gpu}, \"avg_total_power_w\": ${energy_avg_total}, \"exit_code\": ${energy_exit}}}}"
          first_entry=0
        fi
      done
    done
    echo
    echo '  ],'
    echo '  "warnings": ['
    if ((${#ORDER_WARNINGS[@]})); then
      local first_warn=1
      for warning in "${ORDER_WARNINGS[@]}"; do
        if [[ $first_warn -eq 0 ]]; then echo ','; fi
        # Escape quotes in warning text
        local escaped
        escaped=$(json_escape "$warning")
        echo -n "    \"${escaped}\""
        first_warn=0
      done
      echo
    fi
    echo '  ]'
    echo '}'
  } > "$json_file"
  echo
  echo "JSON results written to ${json_file}"
}

main "$@"
