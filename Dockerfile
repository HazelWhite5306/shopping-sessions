# Build a static-ish release binary, then ship it in a slim runtime image.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/sessions /usr/local/bin/sessions
USER 1000
ENTRYPOINT ["/usr/local/bin/sessions"]
