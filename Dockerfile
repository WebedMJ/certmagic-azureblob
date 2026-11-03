FROM caddy:2.11.7-builder AS builder

RUN xcaddy build \
    --with github.com/webedmj/certmagic-azureblob@1.2.0

FROM caddy:2.11.7

COPY --from=builder /usr/bin/caddy /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile