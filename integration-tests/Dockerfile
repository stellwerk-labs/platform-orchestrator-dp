FROM golang:1.26.2-alpine AS builder

WORKDIR /app

# https://stackoverflow.com/questions/36279253/go-compiled-binary-wont-run-in-an-alpine-docker-container-on-ubuntu-host
ENV CGO_ENABLED=0 GOOS=linux GOWORK=off

# See https://github.com/gotestyourself/gotestsum/blob/v1.11.0/.project/docs/running-without-go.md
RUN go build -o /opt/test2json -ldflags="-s -w" cmd/test2json

RUN GOBIN=/opt go install gotest.tools/gotestsum@v1.12.1

# Copy just the files needed for download modules to take advantage of caching in Docker for local development
# go.sum is used for cache invalidation
COPY go.mod go.sum integration-tests ./

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go test -c -o /opt/integration-tests ./integration-tests

ENTRYPOINT ["/opt/gotestsum", "--raw-command", "--", "/opt/test2json", "-t", "/opt/integration-tests", "-test.v=test2json"]
