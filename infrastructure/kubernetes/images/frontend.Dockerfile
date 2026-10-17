ARG NODE_IMAGE=node:24.21.0-bookworm-slim
ARG NGINX_IMAGE=nginxinc/nginx-unprivileged:1.30.4-alpine3.24
FROM ${NODE_IMAGE} AS build
WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
# Public browser configuration only. Never pass credentials as build arguments.
ARG VITE_KEYCLOAK_URL=http://127.0.0.1:18080
ARG VITE_KEYCLOAK_REALM=warexis
ARG VITE_KEYCLOAK_CLIENT_ID=warexis-operations
RUN VITE_KEYCLOAK_URL="$VITE_KEYCLOAK_URL" \
    VITE_KEYCLOAK_REALM="$VITE_KEYCLOAK_REALM" \
    VITE_KEYCLOAK_CLIENT_ID="$VITE_KEYCLOAK_CLIENT_ID" npm run build
FROM ${NGINX_IMAGE}
ARG WAREXIS_BUILD=p11-14fa97e6
LABEL org.opencontainers.image.title="Warexis operations frontend" \
      org.opencontainers.image.version="${WAREXIS_BUILD}"
COPY --from=build --chown=101:101 /build/dist/ /usr/share/nginx/html/
COPY frontend-nginx.conf /etc/nginx/conf.d/default.conf
USER 101:101
EXPOSE 8080
# Skip the upstream configuration-writing entrypoint for a read-only filesystem.
ENTRYPOINT ["nginx"]
CMD ["-g", "daemon off;"]
