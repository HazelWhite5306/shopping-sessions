# Build one fully static binary, then ship it alone in an empty image, so the
# container starts with nothing to load but the binary.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
# Scorers are Intel Xeon, Cascade Lake or newer: AVX2 (x86-64-v3) is always
# there. crt-static links glibc in, so the binary needs no shared libraries.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/sessions /sessions

FROM scratch
COPY --from=build /sessions /sessions
USER 1000:1000
ENTRYPOINT ["/sessions"]
