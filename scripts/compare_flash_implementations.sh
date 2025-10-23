#!/usr/bin/env bash
# Generate synthetic FASTQ pairs at multiple scales, run both FLASH implementations,
# compare their outputs, and report execution times.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${INPUT_DIR:-${ROOT_DIR}/benchmarks/inputs}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/benchmarks/outputs}"
READ_LENGTH="${READ_LENGTH:-150}"
COUNTS=(100 10000 1000000 100000000)

FASTQ_GEN_BIN="${FASTQ_GEN_BIN:-${ROOT_DIR}/target/release/fastq-gen-cli}"
FLASH_CLI_BIN="${FLASH_CLI_BIN:-${ROOT_DIR}/target/release/flash-cli}"
FLASH_C_BIN="${FLASH_C_BIN:-${ROOT_DIR}/bin/flash-lowercase-overhang}"

ensure_binaries() {
  local need_build=0

  if [[ ! -x "$FASTQ_GEN_BIN" || ! -x "$FLASH_CLI_BIN" ]]; then
    need_build=1
  fi

  if [[ "$need_build" -eq 1 ]]; then
    echo "Building Rust binaries (fastq-gen-cli, flash-cli)..."
    cargo build --release -p fastq-gen-cli -p flash-cli
  fi

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

  local start
  start=$(now_ns)
  "$FLASH_CLI_BIN" "$r1" "$r2" \
    --output-dir "$output_dir" \
    --output-prefix flash \
    >"$output_dir/stdout.log" 2>"$output_dir/stderr.log"
  local end
  end=$(now_ns)

  elapsed_seconds "$start" "$end"
}

run_flash_c() {
  local count=$1
  local r1=$2
  local r2=$3
  local output_dir="${OUTPUT_DIR}/flash-lowercase-overhang/${count}"

  rm -rf "$output_dir"
  mkdir -p "$output_dir"

  local start
  start=$(now_ns)
  "$FLASH_C_BIN" "$r1" "$r2" \
    -d "$output_dir" \
    -o flash \
    >"$output_dir/stdout.log" 2>"$output_dir/stderr.log"
  local end
  end=$(now_ns)

  elapsed_seconds "$start" "$end"
}

validate_outputs() {
  local count=$1
  local cli_dir="${OUTPUT_DIR}/flash-cli/${count}"
  local c_dir="${OUTPUT_DIR}/flash-lowercase-overhang/${count}"
  local files=(
    "flash.extendedFrags.fastq"
    "flash.notCombined_1.fastq"
    "flash.notCombined_2.fastq"
  )

  for filename in "${files[@]}"; do
    local cli_file="${cli_dir}/${filename}"
    local c_file="${c_dir}/${filename}"

    if [[ ! -f "$cli_file" || ! -f "$c_file" ]]; then
      echo "Missing output file ${filename} for record count ${count}" >&2
      exit 1
    fi

    if ! cmp -s "$cli_file" "$c_file"; then
      echo "Mismatch detected in ${filename} for record count ${count}" >&2
      exit 1
    fi
  done
}

main() {
  ensure_binaries
  mkdir -p "$OUTPUT_DIR/flash-cli" "$OUTPUT_DIR/flash-lowercase-overhang"

  declare -A results

  for count in "${COUNTS[@]}"; do
    generate_inputs "$count"
    local r1="$INPUT_DIR/${count}_R1.fastq"
    local r2="$INPUT_DIR/${count}_R2.fastq"

    echo "Running flash-cli for ${count} records..."
    local cli_time
    cli_time=$(run_flash_cli "$count" "$r1" "$r2")
    results["${count},flash-cli"]=$cli_time

    echo "Running flash-lowercase-overhang for ${count} records..."
    local c_time
    c_time=$(run_flash_c "$count" "$r1" "$r2")
    results["${count},flash-lowercase-overhang"]=$c_time

    echo "Validating outputs for ${count} records..."
    validate_outputs "$count"
  done

  echo
  echo "Benchmark results (seconds):"
  printf "%-12s %-28s %10s\n" "Records" "Program" "Time"
  printf "%-12s %-28s %10s\n" "-------" "-------" "----"
  for count in "${COUNTS[@]}"; do
    for program in "flash-cli" "flash-lowercase-overhang"; do
      key="${count},${program}"
      if [[ -n "${results[$key]:-}" ]]; then
        printf "%-12s %-28s %10s\n" "$count" "$program" "${results[$key]}"
      fi
    done
  done
}

main "$@"
