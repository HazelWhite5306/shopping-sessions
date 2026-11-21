# Build a static-ish release binary, then ship it in a slim runtime image.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Scorers are Intel Xeon, Cascade Lake or newer: AVX2 (x86-64-v3) is always there.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/sessions /usr/local/bin/sessions
USER 1000
ENTRYPOINT ["/usr/local/bin/sessions"]
