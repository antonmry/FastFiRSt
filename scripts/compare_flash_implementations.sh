#!/usr/bin/env bash
# Generate FASTQ pairs at several scales, run the FLASH implementations,
# validate the outputs, and report execution times.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${INPUT_DIR:-${ROOT_DIR}/benchmarks/inputs}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/benchmarks/outputs}"
READ_LENGTH="${READ_LENGTH:-150}"

COUNTS=(100 1000 10000 100000 1000000)
if [[ -n "${FLASH_BENCH_COUNTS:-}" ]]; then
  read -r -a COUNTS <<<"${FLASH_BENCH_COUNTS}"
fi

FASTQ_GEN_BIN="${FASTQ_GEN_BIN:-${ROOT_DIR}/target/release/fastq-gen-cli}"
FLASH_CLI_BIN="${FLASH_CLI_BIN:-${ROOT_DIR}/target/release/flash-cli}"
FLASH_C_BIN="${FLASH_C_BIN:-${ROOT_DIR}/bin/flash-lowercase-overhang}"

PROGRAMS=("flash-cli" "flash-cli-parallel" "flash-lowercase-overhang")

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

# Run a command and return "wall_seconds cpu_seconds" (wall via date, CPU via /usr/bin/time)
# Usage: run_timed stdout_log stderr_log cmd [args...]
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
    --parallel
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
    cli_timing=$(run_flash_cli "$count" "$r1" "$r2")
    wall_results["${count},flash-cli"]=${cli_timing% *}
    cpu_results["${count},flash-cli"]=${cli_timing#* }

    echo "Running flash-lowercase-overhang for ${count} records..."
    local c_timing
    c_timing=$(run_flash_c "$count" "$r1" "$r2")
    wall_results["${count},flash-lowercase-overhang"]=${c_timing% *}
    cpu_results["${count},flash-lowercase-overhang"]=${c_timing#* }

    echo "Running flash-cli (parallel) for ${count} records..."
    local cli_parallel_timing
    cli_parallel_timing=$(run_flash_cli_parallel "$count" "$r1" "$r2")
    wall_results["${count},flash-cli-parallel"]=${cli_parallel_timing% *}
    cpu_results["${count},flash-cli-parallel"]=${cli_parallel_timing#* }

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
  printf "%-12s %-12s %-28s %10s %10s %12s\n" "Records" "Input(MB)" "Program" "Wall(s)" "CPU(s)" "CPU/Wall"
  printf "%-12s %-12s %-28s %10s %10s %12s\n" "-------" "----------" "-------" "-------" "------" "--------"
  for count in "${COUNTS[@]}"; do
    for program in "${PROGRAMS[@]}"; do
      key="${count},${program}"
      if [[ -n "${wall_results[$key]:-}" ]]; then
        local wall="${wall_results[$key]}"
        local cpu="${cpu_results[$key]}"
        local ratio
        ratio=$(awk -v w="$wall" -v c="$cpu" 'BEGIN { if (w > 0) printf "%.1fx", c/w; else print "N/A" }')
        printf "%-12s %-12s %-28s %10s %10s %12s\n" \
          "$count" "${input_sizes[$count]}" "$program" "$wall" "$cpu" "$ratio"
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
  local json_file="${ROOT_DIR}/benchmarks/results.json"
  {
    echo '{'
    echo '  "metadata": {'
    echo "    \"date\": \"$(date -Iseconds)\","
    echo "    \"read_length\": ${READ_LENGTH},"
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
    for count in "${COUNTS[@]}"; do
      for program in "${PROGRAMS[@]}"; do
        key="${count},${program}"
        if [[ -n "${wall_results[$key]:-}" ]]; then
          if [[ $first_entry -eq 0 ]]; then echo ','; fi
          local wall="${wall_results[$key]}"
          local cpu="${cpu_results[$key]}"
          local ratio
          ratio=$(awk -v w="$wall" -v c="$cpu" 'BEGIN { if (w > 0) printf "%.1f", c/w; else print "0" }')
          echo -n "    {\"records\": ${count}, \"input_mb\": ${input_sizes[$count]}, \"program\": \"${program}\", \"wall_s\": ${wall}, \"cpu_s\": ${cpu}, \"cpu_wall_ratio\": ${ratio}}"
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
        escaped=$(echo "$warning" | sed 's/"/\\"/g')
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
