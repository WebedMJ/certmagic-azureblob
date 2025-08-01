FROM caddy:2.10.0-builder AS builder

RUN xcaddy build \
    --with github.com/webedmj/certmagic-azureblob@1.0.0

FROM caddy:2.10.0

COPY --from=builder /usr/bin/caddy /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile