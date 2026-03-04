#!/usr/bin/env bash
# Set up all dependencies needed to run the benchmark comparison scripts:
#   - compare_flash_implementations.sh
#   - compare_perf_implementations.sh
#
# Intended to run on the host machine (not inside a VM).
# Supports Fedora/RHEL and Ubuntu/Debian.

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

info()  { echo "[setup] $*"; }
error() { echo "[setup] ERROR: $*" >&2; }

# ── System packages ──────────────────────────────────────────────────
info "Installing system packages..."
if command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y \
    gcc gcc-c++ make \
    git \
    python3 python3-pip python3-devel \
    pypy3 pypy3-devel \
    zlib-devel \
    curl
elif command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq \
    build-essential \
    git \
    python3 python3-pip python3-dev python3-venv \
    pypy3 pypy3-dev pypy3-venv \
    zlib1g-dev \
    curl
else
  error "Unsupported package manager. Install manually: gcc, make, git, python3, python3-pip, python3-devel, pypy3, pypy3-devel, zlib-devel, curl"
  exit 1
fi

# ── Rust toolchain ───────────────────────────────────────────────────
if command -v cargo >/dev/null 2>&1; then
  info "Rust already installed: $(rustc --version)"
else
  info "Installing Rust via rustup..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  # shellcheck source=/dev/null
  source "$HOME/.cargo/env"
fi

# Make sure cargo is in PATH for the rest of the script
if ! command -v cargo >/dev/null 2>&1; then
  # shellcheck source=/dev/null
  source "$HOME/.cargo/env"
fi

info "Rust: $(rustc --version)"

# ── Build Rust binaries ──────────────────────────────────────────────
info "Building flash-cli (with parallel feature)..."
cargo build --release -p flash-cli --features parallel --manifest-path "$ROOT_DIR/Cargo.toml"

info "Building fastq-gen-cli..."
cargo build --release -p fastq-gen-cli --manifest-path "$ROOT_DIR/Cargo.toml"

info "Building perf-cli..."
cargo build --release -p perf-cli --manifest-path "$ROOT_DIR/Cargo.toml"

# ── Build FLASH C reference binary ───────────────────────────────────
if [[ ! -x "$ROOT_DIR/bin/flash-lowercase-overhang" ]]; then
  info "Building FLASH lowercase-overhang (C reference)..."
  "$ROOT_DIR/scripts/build_flash_lowercase_overhang.sh"
else
  info "FLASH C binary already exists."
fi

# ── Python: perf_ssr (PERF) ──────────────────────────────────────────
VENV_DIR="$ROOT_DIR/.venv"
if [[ ! -d "$VENV_DIR" ]]; then
  info "Creating Python virtual environment..."
  python3 -m venv "$VENV_DIR"
fi

info "Installing perf_ssr into venv..."
"$VENV_DIR/bin/pip" install --upgrade pip -q
"$VENV_DIR/bin/pip" install biopython tqdm -q
"$VENV_DIR/bin/pip" install --no-deps perf_ssr -q

# Verify PERF is available
if [[ -x "$VENV_DIR/bin/PERF" ]]; then
  info "PERF installed at $VENV_DIR/bin/PERF"
else
  error "PERF not found in venv after install."
  exit 1
fi

# ── PyPy: perf_ssr under PyPy ───────────────────────────────────────
PYPY_PERF_BIN=""
if command -v pypy3 >/dev/null 2>&1; then
  info "PyPy found: $(pypy3 --version 2>&1 | head -1)"
else
  error "PyPy3 is required but not available after system package installation."
  exit 1
fi

PYPY_VENV_DIR="$ROOT_DIR/.venv-pypy"
if [[ ! -d "$PYPY_VENV_DIR" ]]; then
  info "Creating PyPy virtual environment..."
  pypy3 -m venv "$PYPY_VENV_DIR"
fi

info "Installing perf_ssr into PyPy venv..."
"$PYPY_VENV_DIR/bin/pip" install --upgrade pip -q
"$PYPY_VENV_DIR/bin/pip" install biopython tqdm -q
"$PYPY_VENV_DIR/bin/pip" install --no-deps perf_ssr -q

if [[ -x "$PYPY_VENV_DIR/bin/PERF" ]]; then
  PYPY_PERF_BIN="$PYPY_VENV_DIR/bin/PERF"
  info "PyPy PERF installed at $PYPY_PERF_BIN"
else
  error "PyPy PERF not found in venv after install."
  exit 1
fi

# ── Summary ──────────────────────────────────────────────────────────
echo
info "Setup complete. Run the benchmarks with:"
echo
echo "  # Flash benchmark"
echo "  PERF_BIN=$VENV_DIR/bin/PERF bash $ROOT_DIR/scripts/compare_flash_implementations.sh"
echo
echo "  # PERF benchmark"
if [[ -n "$PYPY_PERF_BIN" ]]; then
  echo "  PERF_BIN=$VENV_DIR/bin/PERF PYPY_PERF_BIN=$PYPY_PERF_BIN bash $ROOT_DIR/scripts/compare_perf_implementations.sh"
else
  echo "  PERF_BIN=$VENV_DIR/bin/PERF bash $ROOT_DIR/scripts/compare_perf_implementations.sh"
fi
