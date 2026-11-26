# syntax=docker/dockerfile:1
# superdial-agent: the agent side of superdial calls.
#
#   docker build -t superdial-agent .                         # production: SuperExchange issuer only
#   docker build --build-arg FEATURES=dev-lk-jwt -t superdial-agent:dev .   # development issuer too
#
# Never build this on workbench. The `livekit` crate's webrtc-sys downloads a prebuilt
# libwebrtc and compiles C++ glue with clang 21; the result links only libc, libm and libgcc_s.

FROM rust:1.98-bookworm AS build
RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends wget gnupg pkg-config libssl-dev libglib2.0-dev \
      libva-dev libdrm-dev libx11-dev libxext-dev libxfixes-dev libxdamage-dev libxrandr-dev \
      libxcomposite-dev libgbm-dev libpulse-dev libasound2-dev >/dev/null \
 && wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key > /etc/apt/trusted.gpg.d/apt.llvm.org.asc \
 && echo "deb http://apt.llvm.org/bookworm/ llvm-toolchain-bookworm-21 main" > /etc/apt/sources.list.d/llvm.list \
 && apt-get update -qq && apt-get install -y -qq --no-install-recommends clang-21 libclang-21-dev lld-21 >/dev/null \
 && rm -rf /var/lib/apt/lists/*
ENV CC=clang-21 CXX=clang++-21 LIBCLANG_PATH=/usr/lib/llvm-21/lib
ARG FEATURES=""
ARG JOBS=6
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -j "$JOBS" -p superdial-agent --features "$FEATURES" \
 && install -m 0755 target/release/superdial-agent /usr/local/bin/superdial-agent

FROM debian:bookworm-slim
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends ca-certificates >/dev/null \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/superdial-agent --create-home superdial
COPY --from=build /usr/local/bin/superdial-agent /usr/local/bin/superdial-agent
USER superdial
# Inside the container the API listens on every interface; publish it on loopback or a docker
# network only (`-p 127.0.0.1:8850:8850`).
ENV SUPERDIAL_AGENT_BIND=0.0.0.0:8850 \
    SUPERDIAL_STATE_DIR=/var/lib/superdial-agent
VOLUME /var/lib/superdial-agent
EXPOSE 8850
ENTRYPOINT ["/usr/local/bin/superdial-agent"]
