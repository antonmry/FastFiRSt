#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<USAGE
Usage: $(basename "$0") <forward.fq> <reverse.fq> [output_dir] [output_prefix]

Runs the flash-df example in sequential mode.
Defaults: output_dir=./out, output_prefix=flash
USAGE
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 2 || $# -gt 4 ]]; then
  usage
  exit 1
fi

FORWARD=$1
REVERSE=$2
OUTPUT_DIR=${3:-./out}
OUTPUT_PREFIX=${4:-flash}

if [[ ! -f "$FORWARD" ]]; then
  echo "Forward FASTQ '$FORWARD' not found." >&2
  exit 1
fi

if [[ ! -f "$REVERSE" ]]; then
  echo "Reverse FASTQ '$REVERSE' not found." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

cargo run --quiet -p flash-df --example flash_cli --features datafusion -- \
  "$FORWARD" \
  "$REVERSE" \
  "$OUTPUT_DIR" \
  "$OUTPUT_PREFIX"

echo "flash-df outputs written to '$OUTPUT_DIR' with prefix '$OUTPUT_PREFIX'."
