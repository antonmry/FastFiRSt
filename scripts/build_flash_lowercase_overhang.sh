#!/usr/bin/env bash
# Downloads and builds the FLASH lowercase overhang variant locally.

set -euo pipefail

REPO_URL="https://github.com/Jerrythafast/FLASH-lowercase-overhang"
DEST_DIR="${FLASH_LOWERCASE_OVERHANG_DIR:-third_party/FLASH-lowercase-overhang}"
BIN_DIR="${FLASH_LOWERCASE_OVERHANG_BIN_DIR:-bin}"
MAKE_CMD="${MAKE:-make}"

mkdir -p "$(dirname "$DEST_DIR")"

if [[ ! -d "$DEST_DIR/.git" ]]; then
  git clone "$REPO_URL" "$DEST_DIR"
else
  git -C "$DEST_DIR" fetch --tags
  git -C "$DEST_DIR" pull --ff-only
fi

$MAKE_CMD -C "$DEST_DIR"

mkdir -p "$BIN_DIR"
cp "$DEST_DIR/flash" "$BIN_DIR/flash-lowercase-overhang"
chmod +x "$BIN_DIR/flash-lowercase-overhang"

echo "FLASH binary available at $BIN_DIR/flash-lowercase-overhang"
