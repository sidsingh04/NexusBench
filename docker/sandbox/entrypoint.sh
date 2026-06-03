#!/usr/bin/env bash
# entrypoint.sh — extraction, build, and privilege-drop for all sandbox images.
#
# Execution context:
#   - Runs as ROOT initially (needed to extract archive and chown files)
#   - Drops to uid=1000 (runner) before exec-ing the contestant binary
#
# Injected env vars (set by control plane):
#   NEXUS_SUBMISSION_ID, NEXUS_TEAM, NEXUS_LANGUAGE, NEXUS_PROTOCOL, NEXUS_LISTEN_PORT

set -euo pipefail

log() { echo "[nexus] $*" >&2; }

log "Sandbox starting | submission=${NEXUS_SUBMISSION_ID} team=${NEXUS_TEAM} lang=${NEXUS_LANGUAGE} port=${NEXUS_LISTEN_PORT:-7878}"

# ── 1. Find archive ───────────────────────────────────────────────────────────
ARCHIVE=$(find /submission -maxdepth 1 -type f | head -1)
if [ -z "$ARCHIVE" ]; then
    log "ERROR: /submission is empty — no archive mounted"
    exit 1
fi
log "Found archive: $ARCHIVE"

# ── 2. Extract as root into /app ──────────────────────────────────────────────
# --no-same-owner: ignore uid/gid stored in the archive.
# Archives created on Windows carry Windows user IDs (e.g. 197609) that don't
# exist inside the Linux container. Without this flag tar tries to chown every
# extracted file to that uid and fails with "Operation not permitted".
log "Extracting..."
case "$ARCHIVE" in
    *.tar.gz|*.tgz) tar --no-same-owner -xzf "$ARCHIVE" -C /app ;;
    *.tar.bz2)       tar --no-same-owner -xjf "$ARCHIVE" -C /app ;;
    *.tar.xz)        tar --no-same-owner -xJf "$ARCHIVE" -C /app ;;
    *.zip)           unzip -q "$ARCHIVE" -d /app ;;
    *)
        cp "$ARCHIVE" /app/submission_binary
        chmod +x /app/submission_binary
        ;;
esac

# Hand ownership of everything in /app to runner so the build step can write.
chown -R runner:runner /app
log "Extraction complete, /app handed to runner"

# ── 3. Language-specific build (runs as runner) ───────────────────────────────
cd /app

case "$NEXUS_LANGUAGE" in
go)
    log "Building Go submission..."
    su -s /bin/bash runner -c "
        export PATH=/usr/local/go/bin:\$PATH
        export GOPATH=/home/runner/go
        export GOCACHE=/home/runner/.cache/go-build
        cd /app
        go build -o /app/submission_binary ./...
    "
    ;;
rust)
    log "Building Rust submission..."
    su -s /bin/bash runner -c "cd /app && cargo build --release 2>&1"
    RUST_BIN=$(find /app/target/release -maxdepth 1 -type f -executable | head -1)
    if [ -z "$RUST_BIN" ]; then
        log "ERROR: no binary found in target/release after cargo build"
        exit 1
    fi
    cp "$RUST_BIN" /app/submission_binary
    ;;
cpp)
    log "Building C++ submission..."
    if [ -f /app/CMakeLists.txt ]; then
        su -s /bin/bash runner -c "
            cd /app
            cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=OFF
            cmake --build build -j\$(nproc)
        "
        CPP_BIN=$(find /app/build -maxdepth 2 -type f -executable | head -1)
        [ -n "$CPP_BIN" ] && cp "$CPP_BIN" /app/submission_binary
    else
        su -s /bin/bash runner -c "g++ -O2 -std=c++17 -o /app/submission_binary /app/*.cpp"
    fi
    ;;
python)
    log "Setting up Python submission..."
    [ -f /app/requirements.txt ] && pip3 install -q -r /app/requirements.txt
    cat > /app/submission_binary << 'EOF'
#!/usr/bin/env bash
exec python3 /app/main.py "$@"
EOF
    chmod +x /app/submission_binary
    chown runner:runner /app/submission_binary
    ;;
binary)
    log "Using pre-compiled binary..."
    if [ ! -f /app/submission_binary ]; then
        find /app -type f -exec chmod +x {} +
        BIN=$(find /app -maxdepth 2 -type f -executable | head -1)
        if [ -z "$BIN" ]; then
            log "ERROR: no executable found in archive"
            exit 1
        fi
        cp "$BIN" /app/submission_binary
    fi
    chmod +x /app/submission_binary
    chown runner:runner /app/submission_binary
    ;;
*)
    log "ERROR: unknown language '$NEXUS_LANGUAGE'"
    exit 1
    ;;
esac

# ── 4. Sanity check ───────────────────────────────────────────────────────────
if [ ! -x /app/submission_binary ]; then
    log "ERROR: /app/submission_binary missing or not executable"
    exit 1
fi
log "Build complete"

# ── 5. Drop to runner and exec ────────────────────────────────────────────────
log "Starting contestant binary as runner..."
exec su -s /bin/bash runner -c "
    export NEXUS_SUBMISSION_ID='${NEXUS_SUBMISSION_ID}'
    export NEXUS_TEAM='${NEXUS_TEAM}'
    export NEXUS_LANGUAGE='${NEXUS_LANGUAGE}'
    export NEXUS_PROTOCOL='${NEXUS_PROTOCOL}'
    export NEXUS_LISTEN_PORT='${NEXUS_LISTEN_PORT:-7878}'
    export PATH=/usr/local/go/bin:\$PATH
    export GOPATH=/home/runner/go
    export GOCACHE=/home/runner/.cache/go-build
    exec /app/submission_binary
"
