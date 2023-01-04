FROM rust:1.66 as builder

WORKDIR /usr/src/netrunner
COPY . .

RUN cargo clean && cargo install -vv --path .

FROM debian:bullseye-slim
RUN apt-get update \
    && apt install -y libssl1.1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/netrunner /usr/local/bin/netrunner
ENTRYPOINT [ "netrunner" ]