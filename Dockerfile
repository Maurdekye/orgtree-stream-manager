# stream-manager container (plan §2, §17). Build: docker compose build
FROM rust:1-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p sm-bin

FROM debian:stable-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin sm \
 && mkdir -p /data /out \
 && chown 10001:10001 /data /out
# A new named volume copies this ownership, so /data is writable by uid 10001.
COPY --from=build /src/target/release/stream-manager /usr/local/bin/stream-manager
USER 10001:10001
EXPOSE 8787 8788
HEALTHCHECK --interval=30s --timeout=5s CMD ["stream-manager", "health"]
ENTRYPOINT ["stream-manager"]
CMD ["run"]
