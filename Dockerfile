FROM rust:1.65 as builder

WORKDIR /usr/src/netrunner
COPY . .

RUN cargo install --path .

FROM debian:buster-slim
RUN apt-get update && \
    apt install libssl1.1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/netrunner /usr/local/bin/netrunner
ENTRYPOINT [ "netrunner" ]