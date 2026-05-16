FROM golang:1.22-alpine AS toolchain

FROM ubuntu:24.04 AS runtime

COPY --from=toolchain /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/home/runner/go"
ENV GOCACHE="/home/runner/.cache/go-build"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl tar gzip git \
    && rm -rf /var/lib/apt/lists/*

# Create runner user.
# /app is owned by ROOT so the entrypoint (running as root) can extract
# into it freely. We chown to runner AFTER extraction inside the entrypoint.
# /nexus holds our scripts and is also root-owned (read-only at runtime).
RUN (userdel -r ubuntu 2>/dev/null || true) && \
    groupadd -g 1000 runner && \
    useradd -u 1000 -g runner -m -s /bin/bash runner && \
    mkdir -p /app /submission /nexus

COPY entrypoint.sh /nexus/entrypoint.sh
RUN chmod +x /nexus/entrypoint.sh

EXPOSE 7878
WORKDIR /app
ENTRYPOINT ["/nexus/entrypoint.sh"]
