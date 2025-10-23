# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/devcontainers/rust:1-1-bookworm

# Install Python, C++ toolchain, and Node.js for Codex CLI
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        build-essential \
        cmake \
        pkg-config \
        libssl-dev \
        ca-certificates \
        curl \
        gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install OpenAI Codex CLI from npm registry
RUN npm install -g @openai/codex

# Rename default devcontainer user to `app` (keeps UID/GID 1000)
RUN groupmod -n app vscode \
    && usermod -l app -d /home/app -m vscode \
    && mkdir -p /workspace /workspaces/flash_rs \
    && chown -R app:app /workspace /workspaces/flash_rs
ENV USER=app
ENV HOME=/home/app

# Pre-fetch workspace dependencies to speed up container builds
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY flash-lib flash-lib
COPY flash-cli flash-cli
COPY flash-df flash-df
COPY flash-wasm flash-wasm
RUN chown -R app:app /workspace

USER app
RUN cargo fetch --locked

# Default workdir for Codespaces
WORKDIR /workspaces/flash_rs
