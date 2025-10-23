#!/usr/bin/env bash
# Generate synthetic FASTQ pairs at multiple scales, run both FLASH implementations,
# compare their outputs, and report execution times.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_DIR="${INPUT_DIR:-${ROOT_DIR}/benchmarks/inputs}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/benchmarks/outputs}"
READ_LENGTH="${READ_LENGTH:-150}"
DEFAULT_COUNTS=(100 10000 1000000 100000000)
if [[ -n "${FLASH_BENCH_COUNTS:-}" ]]; then
  read -r -a COUNTS <<<"${FLASH_BENCH_COUNTS}"
else
  COUNTS=("${DEFAULT_COUNTS[@]}")
fi

ORDER_WARNINGS=()

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
  local r1_path=$2
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

    if cmp -s "$cli_file" "$c_file"; then
      continue
    fi

    if compare_fastq_sets "$cli_file" "$c_file" "${count}_${filename}"; then
      detect_order_mismatch "$count" "$r1_path" "$cli_file" "$c_file" "$filename"
      continue
    fi

    echo "Content mismatch detected in ${filename} for record count ${count}" >&2
    exit 1
  done
}

compare_fastq_sets() {
  local file_a=$1
  local file_b=$2
  local label=$3

  local tmp_a tmp_b
  tmp_a=$(mktemp "${OUTPUT_DIR}/hash_${label}_cliXXXXXX")
  tmp_b=$(mktemp "${OUTPUT_DIR}/hash_${label}_flashXXXXXX")

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
    record_count = 0
    while True:
        record = read_record(src)
        if record is None:
            break
        tag, seq, qual = record
        if not tag.startswith("@"):
            raise SystemExit(f"Invalid FASTQ tag line: {tag}")
        digest = hashlib.sha256((seq + "\n" + qual).encode("utf-8")).hexdigest()
        dst.write(f"{tag}\t{digest}\n")
        record_count += 1
PY
}

detect_order_mismatch() {
  local count=$1
  local r1_path=$2
  local cli_file=$3
  local c_file=$4
  local filename=$5

  local info_cli info_c
  info_cli=$(mktemp "${OUTPUT_DIR}/order_cli_${count}_XXXXXX")
  info_c=$(mktemp "${OUTPUT_DIR}/order_c_${count}_XXXXXX")

  local cli_ok=0
  local c_ok=0

  if check_order_against_input "$r1_path" "$cli_file" "$info_cli"; then
    cli_ok=1
  else
    cli_ok=0
    ORDER_WARNINGS+=("Record order mismatch for ${count} (${filename}, flash-cli): $(<"$info_cli")")
  fi

  if check_order_against_input "$r1_path" "$c_file" "$info_c"; then
    c_ok=1
  else
    c_ok=0
    ORDER_WARNINGS+=("Record order mismatch for ${count} (${filename}, flash-lowercase-overhang): $(<"$info_c")")
  fi

  if [[ "$cli_ok" -eq 1 && "$c_ok" -eq 0 ]]; then
    echo "Warning: flash-lowercase-overhang output ordering diverged for ${count} (${filename})." >&2
  elif [[ "$cli_ok" -eq 0 && "$c_ok" -eq 1 ]]; then
    echo "Warning: flash-cli output ordering diverged for ${count} (${filename})." >&2
  else
    echo "Warning: Both implementations diverged in ordering for ${count} (${filename})." >&2
  fi

  rm -f "$info_cli" "$info_c"
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
            tag = tag.rstrip()
            mapping[tag] = idx
            idx += 1
    return mapping

def write_info(message: str):
    with info_path.open("w", encoding="utf-8") as info_handle:
        info_handle.write(message)

tag_index = load_input_indices(input_r1_path)

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
        if tag not in tag_index:
            write_info(f"tag {tag} missing from inputs")
            sys.exit(1)
        idx = tag_index[tag]
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
    validate_outputs "$count" "$r1"
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

  if ((${#ORDER_WARNINGS[@]})); then
    echo
    echo "Order warnings:"
    for warning in "${ORDER_WARNINGS[@]}"; do
      echo " - $warning"
    done
  fi
}

main "$@"
