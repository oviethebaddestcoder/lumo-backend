# syntax=docker/dockerfile:1.7
# Build from the repo root: docker build -f worker-transcoder/Dockerfile -t worker-transcoder .
FROM golang:1.26 AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY lumo-contracts/go.mod lumo-contracts/go.sum* lumo-contracts/
COPY lumo-kit/go.mod lumo-kit/go.sum* lumo-kit/
COPY worker-transcoder/go.mod worker-transcoder/go.sum* worker-transcoder/
RUN --mount=type=cache,target=/go/pkg/mod cd worker-transcoder && go mod download
COPY lumo-contracts lumo-contracts
COPY lumo-kit lumo-kit
COPY worker-transcoder worker-transcoder
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd worker-transcoder && go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/transcoder

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/* && useradd -u 65532 -r app
COPY --from=build /out/app /app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app"]
CMD ["worker"]
