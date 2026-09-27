FROM debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/canopy /var/lib/canopy \
    && chown 10001:10001 /var/lib/canopy
COPY --chmod=0755 canopy /usr/local/bin/canopy
USER 10001:10001
WORKDIR /var/lib/canopy
ENV TMPDIR=/var/lib/canopy
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/canopy"]
CMD ["/etc/canopy/config.json"]
