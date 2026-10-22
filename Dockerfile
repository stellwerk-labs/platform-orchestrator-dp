FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine@sha256:5cb67f19c5e7994dcb80c074f7cfca8aeeada45bbdca035f73df269cefdcce61 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG SOURCE_DATE_EPOCH=0
WORKDIR /app
COPY scripts/release-tools.sh scripts/install-go-licenses.sh ./scripts/
RUN sh scripts/install-go-licenses.sh /usr/local/bin
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    sh scripts/build-server-artifact.sh

FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS final
LABEL org.opencontainers.image.source="https://github.com/stellwerk-labs/platform-orchestrator-dp"
WORKDIR /opt/server
COPY --chown=nonroot:nonroot --from=builder /opt/server .
COPY --chown=nonroot:nonroot --from=builder /opt/licenses /licenses
ENTRYPOINT ["/opt/server/server"]
