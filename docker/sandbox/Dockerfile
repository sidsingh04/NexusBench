# NexusBench Sandbox Base Image
#
# This is the hardened runtime environment that wraps contestant code.
# Build process:
#   1. Copies the submission archive mounted at /submission
#   2. Extracts it into /app
#   3. Runs the language-appropriate build step (controlled by NEXUS_LANGUAGE env)
#   4. Drops to a non-root user and executes via the entrypoint wrapper
#
# The image supports: go, rust, cpp, python, binary
# All contestant processes are run as uid=1000 (runner)

FROM ubuntu:24.04

# ---- System packages ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Go toolchain
    golang-go \
    # Rust toolchain
    rustc cargo \
    # C++ toolchain
    g++ cmake make \
    # Python runtime
    python3 python3-pip python3-venv \
    # Utilities
    tar gzip zip unzip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ---- Non-root runner user ----
# In Ubuntu 24.04, the user 'ubuntu' (1000:1000) exists by default. We remove it first.
RUN (userdel -r ubuntu || true) && \
    groupadd -g 1000 runner && \
    useradd -u 1000 -g runner -m -s /bin/bash runner

# ---- Working directories ----
RUN mkdir -p /app /submission /nexus && chown runner:runner /app /nexus

# ---- Entrypoint script ----
# This script is run as root, extracts the archive, then drops to runner.
COPY entrypoint.sh /nexus/entrypoint.sh
RUN chmod +x /nexus/entrypoint.sh

# Expose the standard sandbox port
EXPOSE 7878

WORKDIR /app

ENTRYPOINT ["/nexus/entrypoint.sh"]
